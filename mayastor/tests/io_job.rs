use std::sync::Arc;

use once_cell::sync::OnceCell;
use tokio::time::Duration;

use mayastor::{
    bdev::{nexus_create, nexus_lookup},
    core::{io_driver, Bdev, MayastorCliArgs},
};
use rpc::mayastor::{BdevShareRequest, BdevUri};

pub mod common;
use common::compose::{self, Binary, ComposeTest, MayastorTest};
use mayastor::core::io_driver::JobQueue;

static DOCKER_COMPOSE: OnceCell<ComposeTest> = OnceCell::new();
static MAYASTOR: OnceCell<MayastorTest> = OnceCell::new();

// this functions runs with in the context of the mayastorTest instance
async fn create_work(queue: Arc<JobQueue>) {
    // get a vector of grpc clients to all containers that are part of this test
    let mut hdls = DOCKER_COMPOSE.get().unwrap().grpc_handles().await.unwrap();

    // for each grpc client, invoke these methods.
    for h in &mut hdls {
        // create the bdev
        h.bdev
            .create(BdevUri {
                uri: "malloc:///disk0?size_mb=64".into(),
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

    DOCKER_COMPOSE.get().unwrap().logs_all().await.unwrap();

    // get a reference to mayastor (used later)
    let ms = MAYASTOR.get().unwrap();

    // have ms create our nexus to the targets created above to know the IPs of
    // the mayastor instances that run in the container, the handles can be
    // used. This avoids hardcoded IPs and having magic constants.
    ms.spawn(async move {
        nexus_create(
            "nexus0",
            1024 * 1024 * 60,
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

        let bdev = Bdev::lookup_by_name("nexus0").unwrap();

        // create a job using the bdev we looked up, we are in the context here
        // of the ms instance and not the containers.
        let job = io_driver::Builder::new()
            .core(1)
            .bdev(bdev)
            .qd(32)
            .io_size(512)
            .build()
            .await;

        queue.start(job);
    })
    .await
}

async fn stats() {
    // we grab an instance to mayastor test
    let ms = MAYASTOR.get().unwrap();
    // and spawn a future on it
    ms.spawn(async move {
        let bdev = Bdev::bdev_first().unwrap().into_iter();
        for b in bdev {
            let result = b.stats().await.unwrap();
            println!("{}: {:?}", b.name(), result);
        }
    })
    .await;
}

#[tokio::test]
async fn io_driver() {
    //
    // We are creating 3 mayastor instances in total. Two of them will be
    // running in side a container. Once these two instances are running, we
    // will create a malloc bdev on each and share that over nvmf. Using
    // these targets a 3de mayastor instance will be started. The third one
    // however, is started by means of struct MayastorTest. This way, we can
    // interact with it using .spawn() and .send().
    //
    // The spawn() method returns an awaitable handle and .send()  does a fire
    // and forget. Using these methods we create a nexus in the mayastor
    // test instance (ms). As part of the test, we also create a malloc bdev
    // on that instance
    //
    // Finally, we create 2 jobs, one for the nexus and one for the malloc bdev
    // and let the test run for 5 seconds.

    // To make it easy to get access to the ComposeTest and MayastorTest
    // instances they are, after creation stored in the static globals
    //

    // the queue that holds our jobs once started. As we pass this around
    // between this thread the mayastor instance we keep a ref count. We
    // need to keep track of the Jobs to avoid them from being dropped.
    let queue = Arc::new(JobQueue::new());

    // create the docker containers
    // we are pinning them to 3rd and 4th core spectively to improve stability
    // of the test. Be aware that default docker container cpuset is 0-3!
    let compose = compose::Builder::new()
        .name("cargo-test")
        .network("10.1.0.0/16")
        .add_container_bin(
            "nvmf-target1",
            Binary::from_dbg("mayastor").with_args(vec!["-l", "2"]),
        )
        .add_container_bin(
            "nvmf-target2",
            Binary::from_dbg("mayastor").with_args(vec!["-l", "3"]),
        )
        .with_prune(true)
        .with_clean(true)
        .build()
        .await
        .unwrap();

    // create the mayastor test instance
    let mayastor_test = MayastorTest::new(MayastorCliArgs {
        log_components: vec!["all".into()],
        reactor_mask: "0x3".to_string(),
        no_pci: true,
        grpc_endpoint: "0.0.0.0".to_string(),
        ..Default::default()
    });

    // set the created instances to the globals here such that we can access
    // them whenever we want by "getting" them. Because some code is async
    // we cannot do this one step as the async runtime cannot be used during
    // init.
    DOCKER_COMPOSE.set(compose).unwrap();

    // later down the road we use the ms instance (to spawn futures) so here we
    // use get_or_init() it is a shorter way of writing:
    // ```rust
    // MAYASTOR.set(mayastor);
    // let ms = MAYASTOR.get().unwrap();
    // ```
    let ms = MAYASTOR.get_or_init(|| mayastor_test);

    // the creation of the targets -- is done by grpc handles. Subsequently, we
    // create the nexus and the malloc bdev (using futures). To keep things
    // a bit organised we do that in a single function notice we pass queue
    // here as an argument. We could also make a static queue here if we wanted
    // too to avoid passing arguments around.

    create_work(queue.clone()).await;

    // the devices have been created and they are pumping IO
    tokio::time::delay_for(Duration::from_secs(5)).await;

    // we must stop all jobs otherwise mayastor would never exit (unless we
    // signal it)
    queue.stop_all().await;
    // grab some stats of the bdevs in the ms instance
    stats().await;

    // Both ComposeTest and MayastorTest impl Drop. However, we want to control
    // the sequence of shut down here, so we destroy the nexus to avoid that
    // the system destroys the containers before it destroys mayastor.
    ms.spawn(nexus_lookup("nexus0").unwrap().destroy())
        .await
        .unwrap();
    // now we manually destroy the docker containers
    DOCKER_COMPOSE.get().unwrap().down().await;

    // ms gets dropped and will call mayastor_env_stop()
}
