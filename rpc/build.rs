use std::{
    env,
    path::{Path, PathBuf},
    process::Command,
};

extern crate tonic_build;

fn main() {
    if !Path::new("mayastor-api/.git").exists() {
        let output = Command::new("git")
            .args(&["submodule", "update", "--init"])
            .output()
            .expect("failed to execute git command");
        dbg!(&output);
        if !output.status.success() {
            panic!("submodule checkout failed");
        }
    }
    let reflection_descriptor = PathBuf::from(env::var("OUT_DIR").unwrap())
        .join("mayastor_reflection.bin");
    tonic_build::configure()
        .file_descriptor_set_path(&reflection_descriptor)
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &["mayastor-api/protobuf/mayastor.proto"],
            &["mayastor-api/protobuf"],
        )
        .unwrap_or_else(|e| {
            panic!("mayastor protobuf compilation failed: {}", e)
        });

    tonic_build::configure()
        .file_descriptor_set_path(&reflection_descriptor)
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &[
                "mayastor-api/protobuf/v1/bdev.proto",
                "mayastor-api/protobuf/v1/json.proto",
                "mayastor-api/protobuf/v1/pool.proto",
                "mayastor-api/protobuf/v1/replica.proto",
                "mayastor-api/protobuf/v1/host.proto",
                "mayastor-api/protobuf/v1/nexus.proto",
                "mayastor-api/protobuf/v1/registration.proto",
            ],
            &["mayastor-api/protobuf/v1"],
        )
        .unwrap_or_else(|e| {
            panic!("mayastor v1 protobuf compilation failed: {}", e)
        });
}
