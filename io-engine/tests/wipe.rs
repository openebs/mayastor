use futures::StreamExt;
use io_engine_tests::{
    compose::{
        rpc::v1::{GrpcConnect, RpcHandle},
        Binary,
        Builder,
        ComposeTest,
    },
    dd_urandom_blkdev,
    nvme::{list_mayastor_nvme_devices, NmveConnectGuard},
    pool::PoolBuilder,
    replica::ReplicaBuilder,
};

use mayastor_api::{
    v1 as v1_rpc,
    v1::{
        replica::Replica,
        test::{wipe_options::WipeMethod, WipeReplicaResponse},
    },
};

const REPLICA_NAME: &str = "r1";
const REPLICA_UUID: &str = "e962fb16-acf2-4cde-bf50-3655b0381efa";
mod common;

#[tokio::test]
async fn replica_wipe() {
    common::composer_init();

    std::env::set_var("NEXUS_NVMF_ANA_ENABLE", "1");
    std::env::set_var("NEXUS_NVMF_RESV_ENABLE", "1");
    // create a new composeTest
    let test = Builder::new()
        .name("replica_wipe")
        .network("10.1.0.0/16")
        .unwrap()
        .add_container_bin(
            "ms1",
            Binary::from_dbg("io-engine").with_bind("/tmp", "/host/tmp"),
        )
        .with_clean(true)
        .with_logs(false)
        .build()
        .await
        .unwrap();
    let mut replica_builder = create_pool_replica(&test).await;

    let grpc = GrpcConnect::new(&test);
    let mut ms = grpc.grpc_handle("ms1").await.unwrap();

    let replica = replica_builder.create().await.unwrap();
    let replica_size = 1024 * 1024 * 1024;
    assert_eq!(replica_size, replica.size);
    let tests = vec![
        TestWipeReplica {
            wipe_method: WipeMethod::None,
            chunk_size: 0,
            expected_chunk_size: replica.size,
            expected_last_chunk_size: replica.size,
            expected_notifications: 2,
            expected_successes: 2,
        },
        TestWipeReplica {
            wipe_method: WipeMethod::None,
            chunk_size: replica.size,
            expected_chunk_size: replica.size,
            expected_last_chunk_size: replica.size,
            expected_notifications: 2,
            expected_successes: 2,
        },
        TestWipeReplica {
            wipe_method: WipeMethod::None,
            chunk_size: 512 * 1024 * 1024,
            expected_chunk_size: 512 * 1024 * 1024,
            expected_last_chunk_size: 512 * 1024 * 1024,
            expected_notifications: 3,
            expected_successes: 3,
        },
        TestWipeReplica {
            wipe_method: WipeMethod::None,
            chunk_size: 500 * 1024 * 1024,
            expected_chunk_size: 500 * 1024 * 1024,
            expected_last_chunk_size: 24 * 1024 * 1024,
            expected_notifications: 4,
            expected_successes: 4,
        },
        TestWipeReplica {
            wipe_method: WipeMethod::None,
            chunk_size: 500 * 1024 * 1024 + 512,
            expected_chunk_size: 500 * 1024 * 1024 + 512,
            expected_last_chunk_size: 24 * 1024 * 1024 - 2 * 512,
            expected_notifications: 4,
            expected_successes: 4,
        },
        TestWipeReplica {
            wipe_method: WipeMethod::None,
            chunk_size: 500 * 1024 * 1024 + 500, // 500 % 512 != 0
            expected_chunk_size: 0,
            expected_last_chunk_size: 0,
            expected_notifications: 1,
            expected_successes: 0,
        },
        TestWipeReplica {
            wipe_method: WipeMethod::None,
            chunk_size: 1024 * 1024, // too many chunks!
            expected_chunk_size: 0,
            expected_last_chunk_size: 0,
            expected_notifications: 1,
            expected_successes: 0,
        },
    ];

    for test in tests {
        validate_wipe_replica(&mut ms, &replica, test).await;
    }
    replica_builder.destroy().await.unwrap();
    let nvmf_location = replica_builder.nvmf_location();

    // Create a smaller replica to validate write_zero wiping!
    let replica = replica_builder
        .with_size_mb(4)
        .with_nvmf()
        .create()
        .await
        .unwrap();

    let _nvme_guard =
        NmveConnectGuard::connect_addr(&nvmf_location.addr, &nvmf_location.nqn);
    let device = nvme_device();

    let mb = 1024 * 1024;
    // already zeroed up to 8MiB after creation!
    io_engine_tests::compare_devices(&device, "/dev/zero", 4 * mb, true);

    assert_eq!(dd_urandom_blkdev(&device), 0);
    io_engine_tests::compare_devices(&device, "/dev/zero", 4 * mb, false);

    wipe_replica(&mut ms, &replica, WipeMethod::None, mb).await;
    io_engine_tests::compare_devices(&device, "/dev/zero", mb, false);

    wipe_replica(&mut ms, &replica, WipeMethod::WriteZeroes, mb).await;
    io_engine_tests::compare_devices(&device, "/dev/zero", mb, true);

    io_engine_tests::compare_devices(&device, "/dev/zero", 4 * mb, true);
}

fn nvme_device() -> String {
    let nvme_ms = list_mayastor_nvme_devices();
    assert_eq!(nvme_ms.len(), 1);
    format!("/dev/{}", nvme_ms[0].device)
}

#[derive(Debug)]
struct TestWipeReplica {
    wipe_method: WipeMethod,
    chunk_size: u64,
    expected_chunk_size: u64,
    expected_last_chunk_size: u64,
    expected_notifications: usize,
    expected_successes: usize,
}

async fn wipe_replica(
    ms: &mut RpcHandle,
    replica: &Replica,
    wipe_method: WipeMethod,
    chunk_size: u64,
) {
    let response =
        issue_wipe_replica(ms, replica, wipe_method, chunk_size).await;
    let stream = response.into_inner();
    let responses = collect_stream(stream).await;
    let last = responses.last();
    assert!(last.is_some());
    assert!(last.unwrap().is_ok());
    assert_eq!(last.unwrap().as_ref().unwrap().remaining_bytes, 0);
}

async fn validate_wipe_replica(
    ms: &mut RpcHandle,
    replica: &Replica,
    wipe: TestWipeReplica,
) {
    let response =
        issue_wipe_replica(ms, replica, wipe.wipe_method, wipe.chunk_size)
            .await;
    let stream = response.into_inner();
    let responses = collect_stream(stream).await;
    let oks = responses
        .iter()
        .filter_map(|r| r.as_ref().ok())
        .collect::<Vec<_>>();
    let errs = responses
        .iter()
        .filter_map(|r| r.as_ref().err())
        .collect::<Vec<_>>();
    assert_eq!(responses.len(), wipe.expected_notifications, "{wipe:#?}");
    assert_eq!(oks.len(), wipe.expected_successes, "{wipe:#?}");
    assert_eq!(
        errs.len(),
        wipe.expected_notifications - wipe.expected_successes,
        "{wipe:#?}"
    );

    if let Some(ok) = oks.last() {
        assert_eq!(ok.remaining_bytes, 0, "{wipe:#?}");
    }

    let mut expected_chunks = 0;
    // validate all up to the last one!
    if !oks.is_empty() {
        for ok in oks.iter().take(oks.len() - 1) {
            assert_eq!(ok.chunk_size, wipe.expected_chunk_size, "{wipe:#?}");
            assert_eq!(ok.wiped_chunks, expected_chunks, "{wipe:#?}");
            assert_eq!(
                ok.remaining_bytes,
                replica.size - expected_chunks * wipe.expected_chunk_size,
                "{wipe:#?}"
            );
            assert_eq!(
                ok.wiped_bytes,
                expected_chunks * wipe.expected_chunk_size,
                "{wipe:#?}"
            );
            assert_eq!(
                ok.last_chunk_size, wipe.expected_last_chunk_size,
                "{wipe:#?}"
            );
            expected_chunks += 1;
        }
        if wipe.expected_chunk_size != wipe.expected_last_chunk_size {
            assert_eq!(
                replica.size - (expected_chunks - 1) * wipe.expected_chunk_size,
                wipe.expected_last_chunk_size,
                "{wipe:#?}"
            );
        }
    }

    if let Some(ok) = oks.last() {
        assert_eq!(ok.chunk_size, wipe.expected_chunk_size, "{wipe:#?}");
        assert_eq!(ok.wiped_chunks, expected_chunks, "{wipe:#?}");
        assert_eq!(
            ok.last_chunk_size, wipe.expected_last_chunk_size,
            "{wipe:#?}"
        );
        assert_eq!(ok.remaining_bytes, 0, "{wipe:#?}");
        assert_eq!(
            ok.wiped_bytes,
            (expected_chunks - 1) * wipe.expected_chunk_size
                + wipe.expected_last_chunk_size,
            "{wipe:#?}"
        );
    }
}

async fn collect_stream(
    mut stream: tonic::Streaming<WipeReplicaResponse>,
) -> Vec<Result<WipeReplicaResponse, tonic::Status>> {
    let mut responses = vec![];
    while let Some(response) = stream.next().await {
        println!("{response:?}");
        responses.push(response);
    }
    responses
}

async fn issue_wipe_replica(
    ms: &mut RpcHandle,
    replica: &Replica,
    wipe_method: WipeMethod,
    chunk_size: u64,
) -> tonic::Response<tonic::Streaming<WipeReplicaResponse>> {
    let replica = replica.clone();
    ms.test
        .wipe_replica(v1_rpc::test::WipeReplicaRequest {
            uuid: replica.uuid,
            pool: Some(v1_rpc::test::wipe_replica_request::Pool::PoolUuid(
                replica.pooluuid,
            )),
            wipe_options: Some(v1_rpc::test::StreamWipeOptions {
                options: Some(v1_rpc::test::WipeOptions {
                    wipe_method: wipe_method as i32,
                    write_pattern: None,
                }),
                chunk_size,
            }),
        })
        .await
        .unwrap()
}

async fn create_pool_replica(test: &ComposeTest) -> ReplicaBuilder {
    const POOL_SIZE_MB: u64 = 1100;
    const REPL_SIZE_MB: u64 = 1024;

    let grpc = GrpcConnect::new(test);
    let ms = grpc.grpc_handle_shared("ms1").await.unwrap();

    let mut pool = PoolBuilder::new(ms.clone())
        .with_name("pool0")
        .with_new_uuid()
        .with_malloc("mem0", POOL_SIZE_MB);

    let repl_0 = ReplicaBuilder::new(ms.clone())
        .with_pool(&pool)
        .with_name(REPLICA_NAME)
        .with_uuid(REPLICA_UUID)
        .with_size_mb(REPL_SIZE_MB)
        .with_thin(false);
    pool.create().await.unwrap();
    repl_0
}
