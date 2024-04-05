pub mod common;

use std::time::{Duration, Instant};

use common::{
    compose::{
        rpc::v1::{nexus::ChildState, GrpcConnect, Status},
        Binary,
        Builder,
    },
    nexus::NexusBuilder,
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};
use tonic::Code;

struct Volume {
    replicas: Vec<ReplicaBuilder>,
    nex: NexusBuilder,
}

/// Runs multiple rebuild jobs parallel.
#[tokio::test]
async fn nexus_rebuild_parallel() {
    common::composer_init();

    const R: usize = 3; // Number of replicas / nodes.
    const N: usize = 20; // Number f volumes
    const REPL_SIZE: u64 = 50;
    const NEXUS_SIZE: u64 = REPL_SIZE;
    const POOL_SIZE: u64 = REPL_SIZE * N as u64 + REPL_SIZE;
    const DISK_SIZE: u64 = POOL_SIZE * 1024 * 1024;
    const DISK_NAME: &str = "disk";

    // Create pool data file.
    for r in 0 .. R {
        let name = format!("/tmp/{DISK_NAME}_{r}");
        common::delete_file(&[name.clone()]);
        common::truncate_file_bytes(&name, DISK_SIZE);
    }

    let test = Builder::new()
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
        .unwrap();

    let conn = GrpcConnect::new(&test);

    let ms_nex = conn.grpc_handle_shared("ms_2").await.unwrap();

    let ms = [
        conn.grpc_handle_shared("ms_0").await.unwrap(),
        conn.grpc_handle_shared("ms_1").await.unwrap(),
        conn.grpc_handle_shared("ms_2").await.unwrap(),
    ];

    let mut pools = Vec::new();

    // Create pools.
    for (r, ms) in ms.iter().enumerate() {
        let bdev = format!("aio:///host/tmp/{DISK_NAME}_{r}?blk_size=512");
        let mut pool = PoolBuilder::new(ms.clone())
            .with_name(&format!("p{r}"))
            .with_new_uuid()
            .with_bdev(&bdev);
        pool.create().await.unwrap();
        pools.push(pool);
    }

    let mut vols = Vec::new();

    for i in 0 .. N {
        // Create R replicas on the pools.
        let mut replicas = Vec::new();
        for r in 0 .. R {
            let mut repl = ReplicaBuilder::new(ms[r].clone())
                .with_pool(&pools[r])
                .with_name(&format!("v{i}r{r}"))
                .with_new_uuid()
                .with_size_mb(REPL_SIZE)
                .with_thin(false);
            repl.create().await.unwrap();
            repl.share().await.unwrap();
            replicas.push(repl);
        }

        // Create the nexus with 2 replicas @ 1,2.
        let mut nex = NexusBuilder::new(ms_nex.clone())
            .with_name(&format!("v{i}"))
            .with_new_uuid()
            .with_size_mb(NEXUS_SIZE)
            .with_replica(&replicas[1])
            .with_replica(&replicas[2]);

        nex.create().await.unwrap();

        vols.push(Volume {
            replicas,
            nex,
        });
    }

    // Adding replicas / starting rebuilds.
    for vol in &vols {
        vol.nex.add_replica(&vol.replicas[0], false).await.unwrap();
    }

    monitor_volumes(&vols, Duration::from_secs(30))
        .await
        .expect("All volumes must go online");

    // Delete test files.
    for r in 0 .. R {
        let name = format!("/tmp/{DISK_NAME}_{r}");
        common::delete_file(&[name.clone()]);
    }
}

/// Monitors and prints volume states.
async fn monitor_volumes(
    vols: &Vec<Volume>,
    timeout: Duration,
) -> Result<(), Status> {
    println!("\nMonitoring {n} volumes", n = vols.len());

    let start = Instant::now();

    loop {
        let mut vols_degraded = false;
        let mut vols_failed = false;

        for vol in vols {
            let mut s = String::new();

            let n = vol.nex.get_nexus().await.unwrap();
            for c in n.children.iter() {
                match c.state() {
                    ChildState::Faulted => {
                        s = format!("{s} FAILED     | ");
                        vols_failed = true;
                    }
                    ChildState::Online => {
                        s = format!("{s} ONLINE     | ");
                    }
                    ChildState::Unknown => {
                        s = format!("{s} UNKNOWN    | ");
                        vols_degraded = true;
                    }
                    ChildState::Degraded => {
                        s = format!(
                            "{s} REBUILD {p:02} | ",
                            p = c.rebuild_progress
                        );
                        vols_degraded = true;
                    }
                }
            }

            println!("    {n}: {s}", n = n.name)
        }
        println!("-");

        if vols_failed {
            println!("One or more volumes failed");
            return Err(Status::new(
                Code::Internal,
                "One or more volumes failed".to_string(),
            ));
        }

        if !vols_degraded {
            println!("All volumes are online");
            return Ok(());
        }

        if start.elapsed() > timeout {
            return Err(Status::new(
                Code::Cancelled,
                "Waiting for volumes to go Online state timed out".to_string(),
            ));
        }

        tokio::time::sleep(std::time::Duration::from_millis(1000)).await;
    }
}
