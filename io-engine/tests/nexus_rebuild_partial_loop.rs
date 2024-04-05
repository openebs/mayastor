#![cfg(feature = "extended-tests")]

pub mod common;

use std::{
    sync::Arc,
    time::{Duration, Instant},
};

use common::{
    compose::{
        rpc::v1::{nexus::ChildState, GrpcConnect, SharedRpcHandle, Status},
        Binary,
        Builder,
        ComposeTest,
    },
    file_io::DataSize,
    fio::{spawn_fio_task, FioBuilder, FioJobBuilder},
    nexus::{find_nexus_by_uuid, NexusBuilder},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};
use tokio::sync::{oneshot, Mutex};
use tonic::Code;

/// Test cluster node.
struct Node {
    test: Arc<ComposeTest>,
    name: String,
    ms: SharedRpcHandle,
    data_file_path: String,
    pool: PoolBuilder,
    repl: ReplicaBuilder,
}

impl Drop for Node {
    fn drop(&mut self) {
        common::delete_file(&[self.data_file_path.clone()]);
    }
}

impl Node {
    /// Restarts this node and brings back pools and replicas.
    async fn restart(&mut self) {
        self.test.restart(&self.name).await.unwrap();

        self.ms = GrpcConnect::new(&self.test)
            .grpc_handle_shared(&self.name)
            .await
            .unwrap();

        self.pool.create().await.unwrap();
        self.repl.share().await.unwrap();
    }
}

/// Creates 3 io-engine containers, a volume (nexus) on node #2 with 3 replicas,
/// and restarts containers #0 and #1 alternately in a loop, while a long FIO
/// job is running on the volume.
/// This tests takes a long time to pass: about 1 hour or more.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn nexus_rebuild_partial_loop() {
    common::composer_init();
    println!(" ");

    const REPL_SIZE: u64 = 8192;
    const NEXUS_SIZE: u64 = REPL_SIZE;
    const POOL_SIZE: u64 = REPL_SIZE * 2;
    const DISK_SIZE: u64 = POOL_SIZE * 1024 * 1024;
    const DISK_NAME: &str = "disk";

    let test = Arc::new(
        Builder::new()
            .name("cargo-test")
            .network("10.1.0.0/16")
            .unwrap()
            .add_container_bin(
                "ms_0",
                Binary::from_dbg("io-engine")
                    .with_args(vec!["-l", "1,2", "-Fcolor,compact,nodate,host"])
                    .with_bind("/tmp", "/host/tmp"),
            )
            .add_container_bin(
                "ms_1",
                Binary::from_dbg("io-engine")
                    .with_args(vec!["-l", "3,4", "-Fcolor,compact,nodate,host"])
                    .with_bind("/tmp", "/host/tmp"),
            )
            .add_container_bin(
                "ms_2",
                Binary::from_dbg("io-engine")
                    .with_args(vec!["-l", "5,6", "-Fcolor,compact,nodate,host"])
                    .with_bind("/tmp", "/host/tmp"),
            )
            .with_clean(true)
            .build()
            .await
            .unwrap(),
    );

    let conn = GrpcConnect::new(&test);

    // Nexus node handle.
    let ms_nex = conn.grpc_handle_shared("ms_2").await.unwrap();

    // Create node objects.
    let mut nodes = Vec::new();

    for (idx, name) in ["ms_0", "ms_1", "ms_2"].iter().enumerate() {
        let ms = conn.grpc_handle_shared(name).await.unwrap();

        let file_name = format!("{DISK_NAME}_{idx}");

        // Create pool data file.
        let data_file_path = format!("/tmp/{file_name}");
        common::delete_file(&[data_file_path.clone()]);
        common::truncate_file_bytes(&data_file_path, DISK_SIZE);

        let bdev_name = format!("aio:///host/tmp/{file_name}?blk_size=512");

        let mut pool = PoolBuilder::new(ms.clone())
            .with_name(&format!("p{idx}"))
            .with_new_uuid()
            .with_bdev(&bdev_name);

        pool.create().await.unwrap();

        let mut repl = ReplicaBuilder::new(ms.clone())
            .with_pool(&pool)
            .with_name(&format!("v0r{r}", r = idx))
            .with_new_uuid()
            .with_size_mb(REPL_SIZE)
            .with_thin(false);

        repl.create().await.unwrap();
        repl.share().await.unwrap();

        nodes.push(Node {
            test: test.clone(),
            name: name.to_string(),
            ms,
            data_file_path,
            pool,
            repl,
        });
    }

    // Create the nexus.
    let mut nex = NexusBuilder::new(ms_nex.clone())
        .with_name(&format!("v0"))
        .with_new_uuid()
        .with_size_mb(NEXUS_SIZE)
        .with_replica(&nodes[0].repl)
        .with_replica(&nodes[1].repl)
        .with_replica(&nodes[2].repl);

    nex.create().await.unwrap();
    nex.publish().await.unwrap();

    // Notifier.
    let (sender, mut recv) = oneshot::channel();

    let j0 = tokio::spawn({
        let nvmf = nex.nvmf_location();
        async move {
            println!("[A]");
            let (_cg, path) = nvmf.open().unwrap();
            println!("[A] nvme path: {path:?}");

            // Create a FIO job with exactly the same parameters as we do on
            // e2e.
            let fio = FioBuilder::new()
                .with_verbose(true)
                .with_verbose_err(true)
                .with_job(
                    FioJobBuilder::new()
                        .with_filename(path)
                        .with_direct(true)
                        .with_ioengine("libaio")
                        .with_bs(DataSize::from_kb(4))
                        .with_iodepth(16)
                        .with_verify("crc32")
                        .with_verify_fatal(true)
                        .with_verify_async(2)
                        .with_numjobs(1)
                        .with_name("fio".to_string())
                        .with_random_generator("tausworthe64".to_string())
                        .with_loops(10)
                        .with_rw("randrw")
                        .with_thread(None) // We don't set this on e2e.
                        .with_norandommap(None) // We don't set this on e2e.
                        .build(),
                )
                .build();

            let r = spawn_fio_task(&fio).await;
            println!("[A] FIO FINISHED");
            sender.send(()).unwrap();
            println!("[A] NOTIFIED OTHER TASK");

            if r.is_err() {
                println!("[A] FIO ERROR");
                println!("{r:?}");
            }
            r
        }
    });
    tokio::pin!(j0);

    let j1 = tokio::spawn({
        let nex = nex.clone();
        async move {
            println!("[B]: starting node restart loop ...");
            let mut itr = 0;
            loop {
                match recv.try_recv() {
                    Ok(_) => {
                        println!("[B]: fio finished, exiting");
                        break;
                    }
                    Err(_) => {
                        println!("[B]: (fio not finished yet, continue ...)");
                    }
                }

                println!("[B]: node restart iter #{itr} ...");

                for (idx, node) in nodes[0 ..= 1].iter_mut().enumerate() {
                    println!(
                        "[B]:      sleeping before restarting node {idx} ..."
                    );
                    tokio::time::sleep(Duration::from_secs(2)).await;

                    println!("[B]:      restarting node {idx} ...");
                    node.restart().await;

                    nex.online_child_replica(&node.repl).await.unwrap();

                    monitor_nexus(&nex, Duration::from_secs(120))
                        .await
                        .unwrap();

                    println!("[B]:      restarting node {idx} done");
                }

                println!("[B]: node restart iter #{itr} done\n");

                itr += 1;
            }
            println!(
                "[B]: starting node restart loop finished after {itr} iteration(s)"
            );
        }
    });
    tokio::pin!(j1);

    println!("[C]");
    println!("[C] Joining jobs ...");
    let (io_res, _) = tokio::join!(j0, j1);
    println!("[C] Joining jobs done");
    let io_res = io_res.unwrap();
    io_res.unwrap();
}

/// Periodically polls the nexus, prints its child status, and checks if the
/// children are online.
async fn monitor_nexus(
    nex: &NexusBuilder,
    timeout: Duration,
) -> Result<(), Status> {
    let start = Instant::now();

    loop {
        let n = find_nexus_by_uuid(nex.rpc(), &nex.uuid()).await?;
        let buf = n
            .children
            .iter()
            .map(|c| {
                let s = if c.rebuild_progress >= 0 {
                    format!(
                        "{s:?} [{p}]",
                        s = c.state(),
                        p = c.rebuild_progress,
                    )
                } else {
                    format!("{s:?}", s = c.state())
                };

                format!("{s:20}")
            })
            .collect::<Vec<String>>()
            .join(" | ");

        println!("[B]:      | {buf}");

        if n.children.iter().all(|c| c.state() == ChildState::Online) {
            return Ok(());
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        if Instant::now() - start > timeout {
            return Err(Status::new(
                Code::Cancelled,
                "Waiting for children to get online timed out",
            ));
        }
    }
}
