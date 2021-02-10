use super::*;

#[async_trait]
impl ComponentAction for Nats {
    fn configure(
        &self,
        _options: &StartOptions,
        cfg: Builder,
    ) -> Result<Builder, Error> {
        Ok(cfg.add_container_spec(
            ContainerSpec::from_binary(
                "nats",
                Binary::from_nix("nats-server").with_arg("-DV"),
            )
            .with_portmap("4222", "4222"),
        ))
    }
    async fn start(
        &self,
        _options: &StartOptions,
        cfg: &ComposeTest,
    ) -> Result<(), Error> {
        cfg.start("nats").await?;
        cfg.connect_to_bus("nats").await;
        Ok(())
    }
}
