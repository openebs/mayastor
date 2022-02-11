use serde_json::json;

use super::{nexus_iter, NioCtx};

use spdk_rs::{
    BdevModule,
    BdevModuleBuild,
    JsonWriteContext,
    WithModuleConfigJson,
    WithModuleFini,
    WithModuleGetCtxSize,
    WithModuleInit,
};

/// Name for Nexus Bdev module name.
pub(crate) const NEXUS_MODULE_NAME: &str = "NEXUS_CAS_MODULE";

/// TODO
#[derive(Debug)]
pub(crate) struct NexusModule {}

impl NexusModule {
    /// Returns Nexus Bdev module instance.
    /// Panics if the Nexus module was not registered.
    pub fn current() -> BdevModule {
        match BdevModule::find_by_name(NEXUS_MODULE_NAME) {
            Ok(m) => m,
            Err(err) => panic!("{}", err),
        }
    }
}

impl WithModuleInit for NexusModule {
    /// TODO
    fn module_init() -> i32 {
        info!("Initializing Nexus CAS Module");
        0
    }
}

impl WithModuleFini for NexusModule {
    /// TODO
    fn module_fini() {
        info!("Unloading Nexus CAS Module");
    }
}

impl WithModuleGetCtxSize for NexusModule {
    /// TODO
    fn ctx_size() -> i32 {
        std::mem::size_of::<NioCtx>() as i32
    }
}

impl WithModuleConfigJson for NexusModule {
    /// Creates a JSON object that can be applied to Mayastor that
    /// will construct the nexus object and its children.
    /// Note that the nexus implicitly tries to create the children as such
    /// you should not have any iSCSI create related calls that
    /// construct children in the config file.
    fn config_json(w: JsonWriteContext) -> i32 {
        nexus_iter().for_each(|nexus| {
            let uris = nexus
                .children
                .iter()
                .map(|c| c.get_name().to_string())
                .collect::<Vec<String>>();

            let json = json!({
                "method": "create_nexus",
                "params": {
                    "name" : nexus.name,
                    "uuid" : unsafe { nexus.bdev().uuid_as_string() },
                    "children" : uris,
                    "size": nexus.req_size,
                },
            });

            if let Err(e) = w.write(&json) {
                error!("Bdev module config JSON failed: {}", e);
            }
        });
        0
    }
}

impl BdevModuleBuild for NexusModule {}

pub fn register_module() {
    NexusModule::builder(NEXUS_MODULE_NAME)
        .with_module_init()
        .with_module_fini()
        .with_module_ctx_size()
        .with_module_config_json()
        .register();
}
