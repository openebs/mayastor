//! TLS support for the io-engine gRPC server and registration client, on top of
//! the shared [`grpc_tls`] primitives.
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
//!   The registration client uses the same files to verify the control-plane
//!   against the CA and present its own certificate for mutual TLS.

use std::net::SocketAddr;
use tonic::transport::{Channel, Endpoint};

pub use grpc_tls::{TlsConfig, TlsIncoming};

/// The TLS configuration source for the gRPC server.
#[derive(Clone, Debug)]
pub enum GrpcServerTls {
    /// Serve an ephemeral, in-memory self-signed certificate.
    Auto,
    /// Serve a file-backed certificate/key, reloaded on rotation.
    Files(TlsConfig),
}

/// Build a (lazy) gRPC client channel for the registration endpoint, honouring
/// the gRPC TLS configuration.
///
/// - No TLS: a plaintext channel.
/// - [`GrpcServerTls::Auto`]: an auto-TLS channel (certificate verification
///   bypassed), matching the ephemeral self-signed certificate the io-engine serves.
/// - [`GrpcServerTls::Files`]: a file-backed TLS channel, verifying the server
///   against the configured CA (or the system roots) and presenting the client
///   certificate for mutual TLS when configured. The TLS material is reloaded
///   when the files rotate on disk, so reconnections use the latest certificates.
///
/// The `endpoint` must carry an `http` scheme: when TLS is used the handshake
/// is driven by a custom connector, so tonic's own TLS logic is bypassed.
///
/// # Panics
///
/// For the TLS variants the returned channel is built with
/// [`Endpoint::connect_with_connector_lazy`], which eagerly constructs a
/// pooled hyper client that spawns a background task via [`tokio::spawn`].
/// This must therefore be called from within a Tokio runtime context.
pub fn registration_channel(
    endpoint: &Endpoint,
    tls: Option<&GrpcServerTls>,
) -> Result<Channel, String> {
    match tls {
        None => Ok(endpoint.connect_lazy()),
        Some(GrpcServerTls::Auto) => Ok(grpc_tls::auto_tls_connect_lazy(endpoint)),
        Some(GrpcServerTls::Files(tls)) => {
            grpc_tls::file_tls_connect_lazy(endpoint, tls).map_err(|error| error.to_string())
        }
    }
}

/// Bind a TCP listener that serves gRPC over TLS exclusively, using the given
/// TLS configuration source.
///
/// The io-engine never sits at a mixed-transport boundary: the control-plane
/// dials it with the transport advertised in the registration, so plaintext
/// clients are rejected rather than sniffed for.
pub async fn incoming(socket: SocketAddr, tls: &GrpcServerTls) -> Result<TlsIncoming, String> {
    let listener = match tls {
        GrpcServerTls::Auto => grpc_tls::Listener::auto(socket, vec!["localhost".to_string()])
            .await
            .map_err(|error| error.to_string())?,
        GrpcServerTls::Files(tls) => grpc_tls::Listener::file(socket, tls)
            .await
            .map_err(|error| error.to_string())?,
    };
    Ok(listener.tls_incoming())
}
