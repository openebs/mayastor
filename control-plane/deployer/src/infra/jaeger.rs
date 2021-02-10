use super::*;

#[async_trait]
impl ComponentAction for Jaeger {
    fn configure(
        &self,
        options: &StartOptions,
        cfg: Builder,
    ) -> Result<Builder, Error> {
        Ok(if !options.jaeger {
            cfg
        } else {
            cfg.add_container_spec(
                ContainerSpec::from_image(
                    "jaeger",
                    "jaegertracing/all-in-one:latest",
                )
                .with_portmap("16686", "16686")
                .with_portmap("6831/udp", "6831/udp")
                .with_portmap("6832/udp", "6832/udp"),
            )
        })
    }
    async fn start(
        &self,
        options: &StartOptions,
        cfg: &ComposeTest,
    ) -> Result<(), Error> {
        if options.jaeger {
            cfg.start("jaeger").await?;
        }
        Ok(())
    }
}
