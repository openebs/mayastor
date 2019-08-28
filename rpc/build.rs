extern crate tower_grpc_build;
use prost_build::Config;

fn main() {
    let mut config = Config::new();
    config
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    tower_grpc_build::Config::from_prost(config)
        .build(&["proto/mayastor.proto"], &["proto"])
        .unwrap_or_else(|e| {
            panic!("mayastor protobuf compilation failed: {}", e)
        });

    // we share the proto generated code between different workspaces, we
    // separate out the structures and the services themselves. The reason
    // for this is that it avoids the need to include, for example, tower-grpc
    // in any file you might want to use the structs in

    let mut config = Config::new();
    config
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]");

    tower_grpc_build::Config::from_prost(config)
        .enable_server(true)
        .build(&["proto/mayastor_service.proto"], &["proto"])
        .unwrap_or_else(|e| {
            panic!("egress protobuf compilation failed: {}", e)
        });
}
