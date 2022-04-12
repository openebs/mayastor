use common::compose::{Builder, ComposeTest, MayastorTest};
use io_engine::{
    bdev::nexus::{nexus_create, nexus_lookup_mut},
    core::{MayastorCliArgs, Share},
    nexus_uri::bdev_destroy,
};
use once_cell::sync::OnceCell;
use rpc::mayastor::{BdevShareRequest, BdevUri};

pub mod common;

static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();
static DOCKER_COMPOSE: OnceCell<ComposeTest> = OnceCell::new();

async fn nexus_3_way_create() {
    let hdls = DOCKER_COMPOSE.get().unwrap().grpc_handles().await.unwrap();

    MAYASTOR
        .get()
        .unwrap()
        .spawn(async move {
            nexus_create(
                "nexus0",
                1024 * 1024 * 50,
                None,
                &[
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[0].endpoint.ip()
                    ),
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[1].endpoint.ip()
                    ),
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[2].endpoint.ip()
                    ),
                ],
            )
            .await
            .unwrap();

            let n = nexus_lookup_mut("nexus0").unwrap();
            n.share_nvmf(None).await.unwrap();
        })
        .await;
}

async fn nexus_destroy() {
    MAYASTOR
        .get()
        .unwrap()
        .spawn(async move {
            nexus_lookup_mut("nexus0").unwrap().destroy().await.unwrap();
        })
        .await;
}
async fn nexus_share() {
    let n = nexus_lookup_mut("nexus0").unwrap();
    n.share_nvmf(None).await.unwrap();
}

async fn nexus_create_2_way_add_one() {
    let hdls = DOCKER_COMPOSE.get().unwrap().grpc_handles().await.unwrap();
    MAYASTOR
        .get()
        .unwrap()
        .spawn(async move {
            nexus_create(
                "nexus0",
                1024 * 1024 * 50,
                None,
                &[
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[0].endpoint.ip()
                    ),
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[1].endpoint.ip()
                    ),
                ],
            )
            .await
            .unwrap();
        })
        .await;

    // MAYASTOR
    //     .get()
    //     .unwrap()
    //     .spawn(async move { nexus_share().await })
    //     .await;

    let hdls = DOCKER_COMPOSE.get().unwrap().grpc_handles().await.unwrap();
    MAYASTOR
        .get()
        .unwrap()
        .spawn(async move {
            let mut n = nexus_lookup_mut("nexus0").unwrap();

            assert_eq!(n.children.len(), 2);
            n.as_mut()
                .add_child(
                    &format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[2].endpoint.ip()
                    ),
                    true,
                )
                .await
                .unwrap();
            assert_eq!(n.children.len(), 3);
        })
        .await;

    MAYASTOR
        .get()
        .unwrap()
        .spawn(async move { nexus_share().await })
        .await;
}

async fn nexus_2_way_destroy_destroy_child() {
    let hdls = DOCKER_COMPOSE.get().unwrap().grpc_handles().await.unwrap();
    MAYASTOR
        .get()
        .unwrap()
        .spawn(async move {
            nexus_create(
                "nexus0",
                1024 * 1024 * 50,
                None,
                &[
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[0].endpoint.ip()
                    ),
                    format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[1].endpoint.ip()
                    ),
                ],
            )
            .await
            .unwrap();

            nexus_share().await;
        })
        .await;

    let hdls = DOCKER_COMPOSE.get().unwrap().grpc_handles().await.unwrap();
    MAYASTOR
        .get()
        .unwrap()
        .spawn(async move {
            let mut n = nexus_lookup_mut("nexus0").unwrap();

            assert_eq!(n.children.len(), 2);
            n.as_mut()
                .add_child(
                    &format!(
                        "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                        hdls[2].endpoint.ip()
                    ),
                    true,
                )
                .await
                .unwrap();
            assert_eq!(n.children.len(), 3);
        })
        .await;

    let hdls = DOCKER_COMPOSE.get().unwrap().grpc_handles().await.unwrap();
    MAYASTOR
        .get()
        .unwrap()
        .spawn(async move {
            bdev_destroy(&format!(
                "nvmf://{}:8420/nqn.2019-05.io.openebs:disk0",
                hdls[0].endpoint.ip()
            ))
            .await
            .unwrap();
        })
        .await;
}

async fn create_targets() {
    let mut hdls = DOCKER_COMPOSE.get().unwrap().grpc_handles().await.unwrap();

    // for each grpc client, invoke these methods.
    for h in &mut hdls {
        // create the bdev
        h.bdev
            .create(BdevUri {
                uri: "malloc:///disk0?size_mb=100".into(),
            })
            .await
            .unwrap();
        // share it over nvmf
        h.bdev
            .share(BdevShareRequest {
                name: "disk0".into(),
                proto: "nvmf".into(),
            })
            .await
            .unwrap();
    }
}

#[tokio::test]
async fn nexus_add_remove() {
    // create the docker containers
    let compose = Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .add_container("ms1")
        .add_container("ms2")
        .add_container("ms3")
        .with_clean(true)
        .with_prune(true)
        .build()
        .await
        .unwrap();

    // create the mayastor test instance
    let ms = MayastorTest::new(MayastorCliArgs {
        log_components: vec!["all".into()],
        reactor_mask: "0x3".to_string(),
        no_pci: true,
        grpc_endpoint: "0.0.0.0".to_string(),
        ..Default::default()
    });

    DOCKER_COMPOSE.set(compose).unwrap();

    let ms = MAYASTOR.get_or_init(|| ms);

    create_targets().await;
    nexus_3_way_create().await;
    nexus_destroy().await;

    nexus_create_2_way_add_one().await;
    nexus_destroy().await;

    nexus_2_way_destroy_destroy_child().await;
    ms.stop().await;

    DOCKER_COMPOSE.get().unwrap().down().await;
}
