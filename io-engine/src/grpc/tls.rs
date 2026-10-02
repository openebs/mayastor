//! TLS support for the io-engine gRPC server.
//!
//! The transport is fixed by configuration, not negotiated per connection:
//! when TLS is enabled the gRPC server serves TLS connections exclusively and
//! rejects plaintext clients; when TLS is disabled it serves plaintext only.
//!
//! Two certificate sources are supported:
//! - [`GrpcServerTls::Auto`]: an ephemeral, in-memory self-signed certificate,
//!   regenerated on each start. The control-plane connects to io-engine
//!   instances with certificate verification bypassed (auto-TLS), so this is
//!   sufficient for transport encryption without any certificate distribution.
//! - [`GrpcServerTls::Files`]: file-backed certificate/key (and optional client
//!   CA for mutual TLS), reloaded automatically when the files rotate on disk.

use futures::StreamExt;
use std::{
    convert::TryFrom,
    io,
    net::SocketAddr,
    path::PathBuf,
    pin::Pin,
    sync::{Arc, PoisonError, RwLock},
    task::{Context as TaskContext, Poll},
};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    net::TcpStream,
};
use tokio_rustls::{server::TlsStream, TlsAcceptor, TlsConnector};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Endpoint};

/// A rustls server certificate verifier which accepts any certificate.
///
/// This is used by the auto-TLS client (see [`auto_tls_connect`]), which
/// connects to a gRPC server serving an ephemeral self-signed certificate that
/// cannot be verified against a known CA. The connection is still encrypted;
/// only the server identity is not authenticated.
#[derive(Debug)]
struct NoCertificateVerification;

impl rustls::client::danger::ServerCertVerifier for NoCertificateVerification {
    fn verify_server_cert(
        &self,
        _end_entity: &rustls::pki_types::CertificateDer,
        _intermediates: &[rustls::pki_types::CertificateDer],
        _server_name: &rustls::pki_types::ServerName,
        _ocsp_response: &[u8],
        _now: rustls::pki_types::UnixTime,
    ) -> Result<rustls::client::danger::ServerCertVerified, rustls::Error> {
        // This is expected in auto-tls mode, so don't spam the logs.
        Ok(rustls::client::danger::ServerCertVerified::assertion())
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        certificate: &rustls::pki_types::CertificateDer,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        // The server identity is not authenticated (see verify_server_cert),
        // but still verify the handshake signature against the presented
        // certificate's key, using the installed provider's algorithms so only
        // FIPS-approved ones are used in FIPS mode.
        rustls::crypto::verify_tls13_signature(
            message,
            certificate,
            signature,
            &fips::signature_verification_algorithms(),
        )
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        certificate: &rustls::pki_types::CertificateDer,
        signature: &rustls::DigitallySignedStruct,
    ) -> Result<rustls::client::danger::HandshakeSignatureValid, rustls::Error> {
        // The server identity is not authenticated (see verify_server_cert),
        // but still verify the handshake signature against the presented
        // certificate's key, using the installed provider's algorithms so only
        // FIPS-approved ones are used in FIPS mode.
        rustls::crypto::verify_tls12_signature(
            message,
            certificate,
            signature,
            &fips::signature_verification_algorithms(),
        )
    }

    fn supported_verify_schemes(&self) -> Vec<rustls::SignatureScheme> {
        // Advertise the signature schemes of the installed default crypto
        // provider rather than hardcoding them, so in FIPS mode we only offer
        // the FIPS-approved schemes (e.g. no SHA-1 or EdDSA). This mirrors what
        // rustls' own WebPkiServerVerifier does.
        fips::supported_signature_schemes()
    }
}

/// Build a client TLS configuration that bypasses server certificate
/// verification (auto-TLS).
fn auto_client_config() -> rustls::ClientConfig {
    let mut config = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();
    config
        .dangerous()
        .set_certificate_verifier(Arc::new(NoCertificateVerification));
    // gRPC uses HTTP/2 as its transport, so advertise h2 via ALPN.
    config.alpn_protocols = vec![b"h2".to_vec()];
    config
}

/// Build a client TLS configuration from the current certificate files.
///
/// The CA bundle, if provided, replaces the default trust roots used to verify
/// the server certificate. A certificate/key pair, if provided, is presented
/// to the server for mutual TLS.
fn file_client_config(tls: &TlsConfig) -> Result<rustls::ClientConfig, String> {
    use rustls::RootCertStore;
    use std::{fs::File, io::BufReader};

    let open = |path: &PathBuf| -> Result<BufReader<File>, String> {
        File::open(path)
            .map(BufReader::new)
            .map_err(|error| format!("failed to open {}: {error}", path.display()))
    };

    let mut roots = RootCertStore::empty();
    if let Some(ca_certificate) = &tls.ca_certificate {
        let ca_certificates = rustls_pemfile::certs(&mut open(ca_certificate)?)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("failed to read TLS CA file: {error}"))?;
        let (valid, _) = roots.add_parsable_certificates(ca_certificates);
        if valid == 0 {
            return Err("no valid certificates found in the TLS CA file".to_string());
        }
    } else {
        let (valid, _) = roots.add_parsable_certificates(native_root_certs());
        if valid == 0 {
            return Err("no system trust roots could be loaded for TLS".to_string());
        }
    }

    let builder = rustls::ClientConfig::builder().with_root_certificates(roots);
    let mut config =
        if let (Some(certificate), Some(private_key)) = (&tls.certificate, &tls.private_key) {
            let certificates = rustls_pemfile::certs(&mut open(certificate)?)
                .collect::<Result<Vec<_>, _>>()
                .map_err(|error| format!("failed to read TLS certificate: {error}"))?;
            let private_key = rustls_pemfile::private_key(&mut open(private_key)?)
                .map_err(|error| format!("failed to read TLS private key: {error}"))?
                .ok_or_else(|| "no private key found in the TLS key file".to_string())?;
            builder
                .with_client_auth_cert(certificates, private_key)
                .map_err(|error| format!("invalid TLS client certificate/key: {error}"))?
        } else {
            builder.with_no_client_auth()
        };

    // gRPC uses HTTP/2 as its transport, so advertise h2 via ALPN.
    config.alpn_protocols = vec![b"h2".to_vec()];
    Ok(config)
}

/// The system trust roots, used when no CA bundle is configured.
fn native_root_certs() -> Vec<rustls::pki_types::CertificateDer<'static>> {
    // The io-engine registration client connects to the control-plane over a
    // known in-cluster address; when a CA file is supplied it is used, and
    // otherwise an empty root store would reject every server. Load the OS
    // trust store so a publicly-issued (or cluster-CA-issued) certificate can
    // be verified without an explicit CA file.
    rustls_native_certs::load_native_certs().certs
}

/// Establish a single auto-TLS connection to the endpoint described by `uri`,
/// bypassing certificate verification. The returned stream is wrapped for use
/// with hyper/tonic.
async fn auto_tls_connect_io(
    tls: TlsConnector,
    uri: http::Uri,
) -> io::Result<hyper_util::rt::TokioIo<tokio_rustls::client::TlsStream<TcpStream>>> {
    let host = uri
        .host()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "gRPC endpoint has no host"))?;
    let port = uri.port_u16().unwrap_or(443);
    let stream = TcpStream::connect((host, port)).await?;
    let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
    tls.connect(server_name, stream)
        .await
        .map(hyper_util::rt::TokioIo::new)
        .map_err(io::Error::other)
}

/// Eagerly establish an auto-TLS channel to `endpoint`, bypassing certificate
/// verification.
///
/// Used to connect to a control-plane serving an ephemeral self-signed
/// certificate (auto-TLS). The endpoint must use an `http` scheme so tonic does
/// not apply (and reject) its own TLS logic; the TLS handshake is performed by
/// the custom connector.
pub async fn auto_tls_connect(endpoint: &Endpoint) -> Result<Channel, tonic::transport::Error> {
    let tls = TlsConnector::from(Arc::new(auto_client_config()));
    let connector = tower::service_fn(move |uri: http::Uri| auto_tls_connect_io(tls.clone(), uri));
    endpoint.connect_with_connector(connector).await
}

/// Lazily establish an auto-TLS channel to `endpoint`, bypassing certificate
/// verification. The connection is deferred until the first request.
pub fn auto_tls_connect_lazy(endpoint: &Endpoint) -> Channel {
    let tls = TlsConnector::from(Arc::new(auto_client_config()));
    let connector = tower::service_fn(move |uri: http::Uri| auto_tls_connect_io(tls.clone(), uri));
    endpoint.connect_with_connector_lazy(connector)
}

/// Lazily establish a file-backed TLS channel to `endpoint`.
///
/// The server certificate is verified against the configured CA bundle (or the
/// system trust roots if none is set), and a client certificate is presented
/// for mutual TLS when configured. The endpoint must use an `http` scheme so
/// tonic does not apply its own TLS logic; the handshake is performed by the
/// custom connector.
pub fn file_tls_connect_lazy(endpoint: &Endpoint, tls: &TlsConfig) -> Result<Channel, String> {
    let config = Arc::new(file_client_config(tls)?);
    let connector = TlsConnector::from(config);
    let connector = tower::service_fn(move |uri: http::Uri| {
        let connector = connector.clone();
        async move {
            let host = uri.host().ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidInput, "gRPC endpoint has no host")
            })?;
            let port = uri.port_u16().unwrap_or(443);
            let stream = TcpStream::connect((host, port)).await?;
            let server_name = rustls::pki_types::ServerName::try_from(host.to_string())
                .map_err(|error| io::Error::new(io::ErrorKind::InvalidInput, error))?;
            connector
                .connect(server_name, stream)
                .await
                .map(hyper_util::rt::TokioIo::new)
                .map_err(io::Error::other)
        }
    });
    Ok(endpoint.connect_with_connector_lazy(connector))
}

/// Build a (lazy) gRPC client channel for the registration endpoint, honouring
/// the gRPC TLS configuration.
///
/// - No TLS: a plaintext channel.
/// - [`GrpcServerTls::Auto`]: an auto-TLS channel (certificate verification
///   bypassed), matching the ephemeral self-signed certificate the io-engine
///   serves.
/// - [`GrpcServerTls::Files`]: a file-backed TLS channel, verifying the server
///   against the configured CA (or the system roots) and presenting the client
///   certificate for mutual TLS when configured.
///
/// The `endpoint` must carry an `http` scheme: when TLS is used the handshake
/// is driven by a custom connector, so tonic's own TLS logic is bypassed.
pub fn registration_channel(
    endpoint: &Endpoint,
    tls: Option<&GrpcServerTls>,
) -> Result<Channel, String> {
    match tls {
        None => Ok(endpoint.connect_lazy()),
        Some(GrpcServerTls::Auto) => Ok(auto_tls_connect_lazy(endpoint)),
        Some(GrpcServerTls::Files(tls)) => file_tls_connect_lazy(endpoint, tls),
    }
}

/// The TLS configuration source for the gRPC server.
#[derive(Clone, Debug)]
pub enum GrpcServerTls {
    /// Serve an ephemeral, in-memory self-signed certificate.
    Auto,
    /// Serve a file-backed certificate/key, reloaded on rotation.
    Files(TlsConfig),
}

/// TLS certificate files used by the gRPC server.
///
/// The files are intentionally retained as paths rather than loaded into
/// memory so the configuration can be rebuilt after the certificates are
/// rotated on disk.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct TlsConfig {
    ca_certificate: Option<PathBuf>,
    certificate: Option<PathBuf>,
    private_key: Option<PathBuf>,
}

impl TlsConfig {
    /// Create a TLS configuration from optional CA and server identity files.
    ///
    /// The certificate and private key must be provided together.
    pub fn new(
        ca_certificate: Option<PathBuf>,
        certificate: Option<PathBuf>,
        private_key: Option<PathBuf>,
    ) -> Result<Self, String> {
        if certificate.is_some() != private_key.is_some() {
            return Err(
                "both the TLS certificate and private key files must be specified".to_string(),
            );
        }
        Ok(Self {
            ca_certificate,
            certificate,
            private_key,
        })
    }

    /// Whether TLS has been enabled for this endpoint.
    pub fn enabled(&self) -> bool {
        self.ca_certificate.is_some() || self.certificate.is_some()
    }

    /// Certificate files that must be watched for automatic reload.
    fn paths(&self) -> Vec<PathBuf> {
        IntoIterator::into_iter([
            self.ca_certificate.clone(),
            self.certificate.clone(),
            self.private_key.clone(),
        ])
        .flatten()
        .collect()
    }

    /// Filesystem targets to watch for certificate rotation.
    ///
    /// Kubernetes projected secrets replace directory entries atomically, so
    /// watch their shared parent directory when all TLS files live below it.
    fn watch_targets(&self) -> Vec<PathBuf> {
        watch_targets(self.paths())
    }

    /// Last-modified fingerprint used to skip duplicate reload events.
    fn fingerprint(&self) -> io::Result<Vec<std::time::SystemTime>> {
        fingerprint(&self.paths())
    }
}

/// Build a rustls server configuration from the current certificate files.
fn build_rustls_server_config(tls: &TlsConfig) -> Result<rustls::ServerConfig, String> {
    use rustls::{server::WebPkiClientVerifier, RootCertStore};
    use std::{fs::File, io::BufReader};

    let (Some(certificate), Some(private_key)) = (&tls.certificate, &tls.private_key) else {
        return Err("a TLS server requires both certificate and private key files".to_string());
    };

    let open = |path: &PathBuf| -> Result<BufReader<File>, String> {
        File::open(path)
            .map(BufReader::new)
            .map_err(|error| format!("failed to open {}: {error}", path.display()))
    };

    let certificates = rustls_pemfile::certs(&mut open(certificate)?)
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| format!("failed to read TLS certificate: {error}"))?;
    let private_key = rustls_pemfile::private_key(&mut open(private_key)?)
        .map_err(|error| format!("failed to read TLS private key: {error}"))?
        .ok_or_else(|| "no private key found in the TLS key file".to_string())?;

    let builder = rustls::ServerConfig::builder();
    let mut config = if let Some(ca_certificate) = &tls.ca_certificate {
        let client_ca_certificates = rustls_pemfile::certs(&mut open(ca_certificate)?)
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| format!("failed to read TLS client CA file: {error}"))?;
        let mut roots = RootCertStore::empty();
        let (valid, _) = roots.add_parsable_certificates(client_ca_certificates);
        if valid == 0 {
            return Err("no valid certificates found in the TLS client CA file".to_string());
        }
        builder
            .with_client_cert_verifier(
                WebPkiClientVerifier::builder(Arc::new(roots))
                    .build()
                    .map_err(|error| format!("failed to build TLS client verifier: {error}"))?,
            )
            .with_single_cert(certificates, private_key)
            .map_err(|error| format!("invalid TLS certificate/key: {error}"))?
    } else {
        builder
            .with_no_client_auth()
            .with_single_cert(certificates, private_key)
            .map_err(|error| format!("invalid TLS certificate/key: {error}"))?
    };

    // gRPC uses HTTP/2 as its transport, so advertise h2 via ALPN.
    config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(config)
}

/// Generate an ephemeral self-signed server configuration.
///
/// Because its certificate only exists in memory, this configuration cannot be
/// reloaded and does not support client authentication.
pub fn auto_server_config(
    subject_alt_names: Vec<String>,
) -> Result<Arc<rustls::ServerConfig>, String> {
    use rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    let certificate_material = rcgen::generate_simple_self_signed(subject_alt_names)
        .map_err(|error| format!("failed to generate self-signed certificate: {error}"))?;
    let certificate = certificate_material.cert.der().clone();
    let private_key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
        certificate_material.key_pair.serialize_der(),
    ));

    let mut config = rustls::ServerConfig::builder()
        .with_no_client_auth()
        .with_single_cert(vec![certificate], private_key)
        .map_err(|error| format!("invalid self-signed certificate: {error}"))?;

    // gRPC uses HTTP/2 as its transport, so advertise h2 via ALPN.
    config.alpn_protocols = vec![b"h2".to_vec()];

    Ok(Arc::new(config))
}

/// A TLS gRPC server connection.
///
/// This is a thin newtype over a [`TlsStream`] so tonic's [`Connected`] trait
/// can be implemented for it: the orphan rule forbids implementing that foreign
/// trait directly on the foreign stream type.
///
/// [`Connected`]: tonic::transport::server::Connected
pub struct TlsConnection(Pin<Box<TlsStream<TcpStream>>>);

impl AsyncRead for TlsConnection {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        self.get_mut().0.as_mut().poll_read(cx, buf)
    }
}

impl AsyncWrite for TlsConnection {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut TaskContext<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        self.get_mut().0.as_mut().poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        self.get_mut().0.as_mut().poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut TaskContext<'_>) -> Poll<io::Result<()>> {
        self.get_mut().0.as_mut().poll_shutdown(cx)
    }
}

impl tonic::transport::server::Connected for TlsConnection {
    type ConnectInfo = ();

    fn connect_info(&self) -> Self::ConnectInfo {}
}

/// Complete the TLS handshake on an accepted connection.
///
/// Any failure is mapped to a recoverable error kind so a single bad connection
/// never tears down the listener.
async fn accept_tls(connection: TcpStream, acceptor: TlsAcceptor) -> io::Result<TlsConnection> {
    acceptor
        .accept(connection)
        .await
        .map(|stream| TlsConnection(Box::pin(stream)))
        .map_err(|error| io::Error::new(io::ErrorKind::InvalidData, error))
}

/// Maximum number of connections whose TLS handshake may be in progress at
/// once.
///
/// This bounds the work performed off the accept path and provides
/// backpressure: once this many handshakes are in flight, no further
/// connections are accepted until one completes.
const MAX_CONCURRENT_HANDSHAKES: usize = 256;

/// Accept connections and perform the TLS handshake concurrently.
///
/// [`StreamExt::buffer_unordered`] keeps up to [`MAX_CONCURRENT_HANDSHAKES`]
/// handshakes in flight at once and yields whichever completes first, so no
/// connection is held up by a slow or stalled peer.
fn accepted_incoming(
    listener: tokio::net::TcpListener,
    acceptor: impl Fn() -> TlsAcceptor + Send + 'static,
) -> impl futures::Stream<Item = Result<TlsConnection, io::Error>> {
    TcpListenerStream::new(listener)
        .map(move |connection| {
            let acceptor = acceptor();
            async move {
                match connection {
                    Ok(connection) => accept_tls(connection, acceptor).await,
                    Err(error) => Err(error),
                }
            }
        })
        .buffer_unordered(MAX_CONCURRENT_HANDSHAKES)
}

/// Bind a TCP listener that serves gRPC over TLS exclusively, using reloadable
/// file-backed TLS material.
pub async fn incoming(
    socket: SocketAddr,
    tls: TlsConfig,
) -> Result<impl futures::Stream<Item = Result<TlsConnection, io::Error>>, String> {
    let listener = tokio::net::TcpListener::bind(socket)
        .await
        .map_err(|error| format!("failed to bind gRPC socket to {socket}: {error}"))?;
    let tls = ReloadableServerTls::new(tls)?;
    Ok(accepted_incoming(listener, move || tls.acceptor()))
}

/// Bind a TCP listener that serves gRPC over TLS exclusively, using an in-memory
/// TLS server configuration.
pub async fn incoming_with_server_config(
    socket: SocketAddr,
    config: Arc<rustls::ServerConfig>,
) -> Result<impl futures::Stream<Item = Result<TlsConnection, io::Error>>, String> {
    let listener = tokio::net::TcpListener::bind(socket)
        .await
        .map_err(|error| format!("failed to bind gRPC socket to {socket}: {error}"))?;
    let acceptor = TlsAcceptor::from(config);
    Ok(accepted_incoming(listener, move || acceptor.clone()))
}

/// A rustls server configuration that is rebuilt when its file-backed TLS
/// material changes on disk.
#[derive(Clone)]
struct ReloadableServerTls {
    current: Arc<RwLock<Arc<rustls::ServerConfig>>>,
}

struct ServerState {
    tls: TlsConfig,
    fingerprint: Vec<std::time::SystemTime>,
    current: Arc<RwLock<Arc<rustls::ServerConfig>>>,
}

impl ReloadableServerTls {
    fn new(tls: TlsConfig) -> Result<Self, String> {
        let current = Arc::new(RwLock::new(Arc::new(build_rustls_server_config(&tls)?)));
        let targets = tls.watch_targets();
        let paths = tls.paths();
        let state = Arc::new(RwLock::new(ServerState {
            fingerprint: tls.fingerprint().unwrap_or_default(),
            tls,
            current: current.clone(),
        }));
        spawn_watcher("grpc-tls-cert-watcher", targets, move || {
            Self::reload_logged(&state, &paths)
        });
        Ok(Self { current })
    }

    fn acceptor(&self) -> TlsAcceptor {
        TlsAcceptor::from(
            self.current
                .read()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
        )
    }

    fn reload(state: &Arc<RwLock<ServerState>>) -> Result<bool, String> {
        let state_guard = state.read().unwrap_or_else(PoisonError::into_inner);
        let fingerprint = state_guard
            .tls
            .fingerprint()
            .map_err(|error| error.to_string())?;
        if fingerprint == state_guard.fingerprint {
            return Ok(false);
        }
        let config = Arc::new(build_rustls_server_config(&state_guard.tls)?);
        *state_guard
            .current
            .write()
            .unwrap_or_else(PoisonError::into_inner) = config;
        drop(state_guard);
        state
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .fingerprint = fingerprint;
        Ok(true)
    }

    fn reload_logged(state: &Arc<RwLock<ServerState>>, targets: &[PathBuf]) {
        match Self::reload(state) {
            Ok(true) => tracing::info!(?targets, "Reloaded gRPC TLS certificates"),
            Ok(false) => {}
            Err(error) => {
                tracing::warn!(?targets, %error, "Failed to reload gRPC TLS certificates")
            }
        }
    }
}

/// Filesystem paths to watch for certificate changes.
///
/// If all paths share a parent directory and the process runs inside
/// Kubernetes, watch that directory instead. This catches atomic symlink-swap
/// rotations (as used by Kubernetes secret mounts) where individual file
/// symlinks may not emit events.
fn watch_targets(paths: Vec<PathBuf>) -> Vec<PathBuf> {
    if paths.is_empty() || std::env::var("KUBERNETES_SERVICE_HOST").is_err() {
        return paths;
    }
    let mut parents = paths.iter().map(|path| path.parent().map(PathBuf::from));
    if let Some(Some(first)) = parents.next() {
        if parents.all(|parent| parent.as_ref() == Some(&first)) {
            return vec![first];
        }
    }
    paths
}

/// Last-modified fingerprint of the given files, used to skip redundant
/// reloads.
fn fingerprint(paths: &[PathBuf]) -> io::Result<Vec<std::time::SystemTime>> {
    paths
        .iter()
        .map(|path| std::fs::metadata(path)?.modified())
        .collect()
}

/// Spawn a background OS thread that watches `watch_paths` and invokes `reload`
/// on changes.
///
/// A plain OS thread is used rather than an async task because the watch blocks
/// in `inotify` reads and the reload path is fully synchronous/blocking. On
/// non-Linux platforms this logs a warning and does nothing.
#[cfg(target_os = "linux")]
fn spawn_watcher(
    thread_name: &str,
    watch_paths: Vec<PathBuf>,
    reload: impl FnMut() + Send + 'static,
) {
    if watch_paths.is_empty() {
        return;
    }
    std::thread::Builder::new()
        .name(thread_name.into())
        .spawn(move || watch(watch_paths, reload))
        .ok();
}

#[cfg(not(target_os = "linux"))]
fn spawn_watcher(
    _thread_name: &str,
    watch_paths: Vec<PathBuf>,
    _reload: impl FnMut() + Send + 'static,
) {
    if watch_paths.is_empty() {
        return;
    }
    tracing::warn!(
        "TLS certificate watch is only supported on Linux; automatic reload is disabled"
    );
}

/// Watch certificate paths and invoke the reload callback after changes.
///
/// A single inotify instance reacts to `MODIFY` (in-place writes) and
/// `MOVED_TO` (an atomic-rename rotation). When inotify cannot be armed this
/// falls back to invoking the reload callback on a timer until it recovers.
#[cfg(target_os = "linux")]
fn watch(watch_paths: Vec<PathBuf>, mut reload: impl FnMut()) {
    use inotify::{EventMask, Inotify, WatchMask};
    use std::time::Duration;

    /// The polling interval used whenever the inotify watch is unavailable.
    const POLL_INTERVAL: Duration = Duration::from_secs(60);

    tracing::debug!(?watch_paths, "TLS watch requested");

    // Keep one inotify instance for the watcher's lifetime: the kernel only
    // drops the watch descriptors when their inodes are deleted, the instance
    // stays usable.
    let mut inotify = loop {
        match Inotify::init() {
            Ok(inotify) => break inotify,
            Err(error) => {
                tracing::warn!(
                    %error,
                    "Failed to initialise inotify for TLS watch; polling for changes"
                );
                std::thread::sleep(POLL_INTERVAL);
                reload();
            }
        }
    };

    let mask = WatchMask::MODIFY | WatchMask::MOVED_TO;
    let mut buffer = [0u8; 4096];
    'outer: loop {
        for path in &watch_paths {
            if let Err(error) = inotify.watches().add(path, mask) {
                tracing::warn!(
                    %error,
                    path = %path.display(),
                    "Failed to watch TLS path; will retry"
                );
                // Fall back to polling until the full set can be re-armed.
                std::thread::sleep(POLL_INTERVAL);
                reload();
                continue 'outer;
            }
        }

        // Catch up on any change that happened before the watches were
        // (re-)armed, since such changes emit no events; the fingerprint guard
        // makes this a no-op otherwise.
        reload();

        loop {
            // Blocks until an event is available, then returns the whole batch
            // of currently-queued events in one read.
            let events = match inotify.read_events_blocking(&mut buffer) {
                Ok(events) => events,
                Err(error) => {
                    tracing::warn!(%error, "TLS watch read error; re-arming");
                    break;
                }
            };

            tracing::debug!(?watch_paths, "TLS watch events received");

            // The watch descriptor is only dropped when the watched inode
            // itself is removed (delivered as `IGNORED`); in-directory
            // rotations keep the inode and the watch keeps reporting
            // `MOVED_TO`/`MODIFY`.
            let ignored = events
                .into_iter()
                .any(|event| event.mask.contains(EventMask::IGNORED));

            reload();

            if ignored {
                break;
            }
        }

        // Brief settle time before re-arming.
        std::thread::sleep(Duration::from_millis(100));
    }
}

#[cfg(test)]
mod tests {
    use super::TlsConfig;
    use std::path::PathBuf;

    #[test]
    fn rejects_incomplete_identity() {
        assert!(TlsConfig::new(None, Some(PathBuf::from("server.pem")), None).is_err());
        assert!(TlsConfig::new(None, None, Some(PathBuf::from("server.key"))).is_err());
    }

    #[test]
    fn tracks_all_tls_files() {
        let tls = TlsConfig::new(
            Some(PathBuf::from("ca.pem")),
            Some(PathBuf::from("server.pem")),
            Some(PathBuf::from("server.key")),
        )
        .unwrap();

        assert!(tls.enabled());
        assert_eq!(
            tls.paths(),
            vec![
                PathBuf::from("ca.pem"),
                PathBuf::from("server.pem"),
                PathBuf::from("server.key"),
            ]
        );
    }
}
