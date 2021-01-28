use super::*;

#[async_trait]
impl ComponentAction for Empty {
    fn configure(
        &self,
        _options: &StartOptions,
        cfg: Builder,
    ) -> Result<Builder, Error> {
        Ok(cfg)
    }
    async fn start(
        &self,
        _options: &StartOptions,
        _cfg: &ComposeTest,
    ) -> Result<(), Error> {
        Ok(())
    }
}
