extern crate tonic_build;

fn main() {
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/csi.proto"], &["proto"])
        .expect("csi protobuf compilation failed");
    tonic_build::configure()
        .build_server(true)
        .compile(&["proto/mayastornodeplugin.proto"], &["proto"])
        .expect("mayastor node grpc service protobuf compilation failed");
}
