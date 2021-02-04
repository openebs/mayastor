use super::*;

#[async_trait]
impl ComponentAction for Mayastor {
    fn configure(
        &self,
        options: &StartOptions,
        cfg: Builder,
    ) -> Result<Builder, Error> {
        if options.build {
            let status = std::process::Command::new("cargo")
                .args(&["build", "-p", "mayastor", "--bin", "mayastor"])
                .status()?;
            build_error("mayastor", status.code())?;
        }

        let mut cfg = cfg;
        for i in 0 .. options.mayastors {
            let mayastor_socket = format!("{}:10124", cfg.next_container_ip()?);

            cfg = cfg.add_container_bin(
                &Self::name(i, options),
                Binary::from_dbg("mayastor")
                    .with_nats("-n")
                    .with_args(vec!["-N", &Self::name(i, options)])
                    .with_args(vec!["-g", &mayastor_socket]),
            )
        }
        Ok(cfg)
    }
    async fn start(
        &self,
        options: &StartOptions,
        cfg: &ComposeTest,
    ) -> Result<(), Error> {
        for i in 0 .. options.mayastors {
            cfg.start(&Self::name(i, options)).await?;
        }
        Ok(())
    }
}

impl Mayastor {
    fn name(i: u32, options: &StartOptions) -> String {
        if options.mayastors == 1 {
            "mayastor".into()
        } else {
            format!("mayastor-{}", i + 1)
        }
    }
}
