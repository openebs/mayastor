use super::*;

#[async_trait]
impl ComponentAction for Rest {
    fn configure(
        &self,
        options: &StartOptions,
        cfg: Builder,
    ) -> Result<Builder, Error> {
        Ok(if options.no_rest {
            cfg
        } else {
            if options.build {
                std::process::Command::new("cargo")
                    .args(&["build", "-p", "rest", "--bin", "rest"])
                    .status()?;
            }
            if !options.jaeger {
                cfg.add_container_spec(
                    ContainerSpec::from_binary(
                        "rest",
                        Binary::from_dbg("rest")
                            .with_nats("-n")
                            .with_arg("--dummy-certificates")
                            .with_arg("--no-auth")
                            .with_args(vec!["--https", "rest:8080"])
                            .with_args(vec!["--http", "rest:8081"]),
                    )
                    .with_portmap("8080", "8080")
                    .with_portmap("8081", "8081"),
                )
            } else {
                let jaeger_config = format!("jaeger.{}:6831", cfg.get_name());
                cfg.add_container_spec(
                    ContainerSpec::from_binary(
                        "rest",
                        Binary::from_dbg("rest")
                            .with_nats("-n")
                            .with_arg("--dummy-certificates")
                            .with_arg("--no-auth")
                            .with_args(vec!["-j", &jaeger_config])
                            .with_args(vec!["--https", "rest:8080"])
                            .with_args(vec!["--http", "rest:8081"]),
                    )
                    .with_portmap("8080", "8080")
                    .with_portmap("8081", "8081"),
                )
            }
        })
    }
    async fn start(
        &self,
        options: &StartOptions,
        cfg: &ComposeTest,
    ) -> Result<(), Error> {
        if !options.no_rest {
            cfg.start("rest").await?;
        }
        Ok(())
    }
}
