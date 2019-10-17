extern crate tonic_build;

fn main() {
    // this file containers the message definitions
    tonic_build::configure()
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(&["proto/mayastor.proto"], &["proto"])
        .unwrap_or_else(|e| {
            panic!("mayastor protobuf compilation failed: {}", e)
        });

    // this file contains the RPC methods
    tonic_build::configure()
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(&["proto/mayastor_service.proto"], &["proto"])
        .unwrap_or_else(|e| {
            panic!("mayastor protobuf compilation failed: {}", e)
        });
}
