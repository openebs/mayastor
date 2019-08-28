extern crate tower_grpc_build;

fn main() {
    tower_grpc_build::Config::new()
        .enable_server(true)
        .build(&["proto/csi.proto"], &["proto"])
        .unwrap_or_else(|e| panic!("csi protobuf compilation failed: {}", e));
}
