use std::{path::Path, process::Command};

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

    tonic_build::configure()
        .build_server(true)
        .type_attribute(".", "#[derive(serde::Serialize, serde::Deserialize)]")
        .compile(
            &[
                "mayastor-api/protobuf/mayastor.proto",
                "mayastor-api/protobuf/v1/mayastor.proto",
            ],
            &["mayastor-api/protobuf"],
        )
        .unwrap_or_else(|e| {
            panic!("mayastor protobuf compilation failed: {}", e)
        });
}
