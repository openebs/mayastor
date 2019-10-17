extern crate tonic_build;

fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/csi.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("csi protobuf compilation failed: {}", e));
}
