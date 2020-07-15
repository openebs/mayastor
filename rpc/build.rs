extern crate tonic_build;

fn main() {
    tonic_build::configure()
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(&["proto/mayastor.proto"], &["proto"])
        .unwrap_or_else(|e| {
            panic!("mayastor protobuf compilation failed: {}", e)
        });
}
