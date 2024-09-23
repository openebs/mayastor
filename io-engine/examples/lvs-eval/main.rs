mod display;

use clap::Parser;
use version_info::{package_description, version_info_str};

use io_engine_tests::MayastorTest;

use crate::display::print_lvs;
use io_engine::{
    core::{LogicalVolume, MayastorCliArgs},
    lvs::{Lvol, Lvs, LvsError, LvsLvol},
    pool_backend::{IPoolProps, PoolArgs, PoolMetadataArgs, ReplicaArgs},
};

/// TODO
#[derive(Debug, Clone, Parser)]
#[clap(
    name = package_description!(),
    about = "LVS Evaluation",
    version = version_info_str!(),
)]
pub struct CliArgs {
    pub disk: String,
    #[clap(short = 'r', default_value_t = 1)]
    pub replicas: u32,
    #[clap(short = 'N', default_value_t = 1)]
    pub replica_clusters: u64,
    /// Cluster size in Mb.
    #[clap(short = 'C')]
    pub cluster_size: Option<u32>,
    /// Thin provisioned.
    #[clap(short = 't', default_value_t = false)]
    pub thin: bool,
    /// MD reservation ratio.
    #[clap(short = 'M')]
    pub md_resv_ratio: Option<f32>,
    /// Use extent table.
    #[clap(short = 'E')]
    pub use_extent_table: Option<bool>,
    /// Pool name.
    #[clap(default_value = "pool0")]
    pub pool_name: String,
    /// Destroy pool on exit (pool is exported by default).
    #[clap(long = "destroy")]
    pub destroy_pool: bool,
    /// Create a temporary filler replica before each replica.
    #[clap(short = 'F', default_value_t = false)]
    pub fillers: bool,
}

static mut G_USE_EXTENT_TABLE: bool = true;

/// TODO
#[tokio::main(worker_threads = 4)]
async fn main() {
    let args = CliArgs::parse();

    unsafe {
        G_USE_EXTENT_TABLE = args.use_extent_table.unwrap_or(true);
    }

    let ms_args = MayastorCliArgs {
        log_format: Some("nodate,nohost,color".parse().unwrap()),
        reactor_mask: "0x3".into(),
        ..Default::default()
    };

    // let ms = MayastorTest::new_ex(ms_args,
    // Some("debug,io_engine::lvs::lvs_store=info"));
    let ms = MayastorTest::new_ex(ms_args, Some("info"));

    ms.spawn(async move {
        println!();
        println!("-------------------------");
        println!("{args:#?}");
        println!("-------------------------");

        println!("Creating LVS ...");
        let lvs = create_lvs(&args).await;

        let mut fillers = Vec::new();

        // Create replicas.
        println!("Creating {n} replicas ...", n = args.replicas);
        for idx in 0 .. args.replicas {
            if args.fillers {
                match create_filler_replica(&lvs, idx, 1).await {
                    Ok(lvol) => fillers.push(lvol),
                    Err(_) => break,
                }
            }

            if create_replica(&lvs, idx, args.replica_clusters, args.thin)
                .await
                .is_err()
            {
                break;
            }
        }

        println!("Created pool.");
        print_lvs(&lvs).await;

        if !fillers.is_empty() {
            // Destroy fillers.
            for lvol in fillers.into_iter() {
                println!("Destroying filler '{name}'", name = lvol.name());
                lvol.destroy().await.unwrap();
            }

            println!("Destroyed fillers.");
            print_lvs(&lvs).await;
        }

        if args.destroy_pool {
            println!("Destroying pool ...");
            lvs.destroy().await.unwrap();
        } else {
            println!("Exporting pool ...");
            lvs.export().await.unwrap();
        }
    })
    .await;
}

/// TODO
async fn create_lvs(args: &CliArgs) -> Lvs {
    const POOL_UUID: &str = "b8589388-295c-4859-a88a-72a60d3902e8";

    let lvs_args = PoolArgs {
        name: args.pool_name.clone(),
        disks: vec![args.disk.to_string()],
        uuid: Some(POOL_UUID.to_string()),
        cluster_size: args.cluster_size.map(|sz| sz * 1024 * 1024),
        md_args: Some(PoolMetadataArgs {
            md_resv_ratio: args.md_resv_ratio,
        }),
        backend: Default::default(),
    };

    Lvs::create_or_import(lvs_args.clone()).await.unwrap()
}

/// TODO
async fn create_replica(
    lvs: &Lvs,
    serial: u32,
    n: u64,
    thin: bool,
) -> Result<Lvol, LvsError> {
    let name = format!("replica_{serial}");
    let uuid = format!("45c23e54-dc86-45f6-b55b-e44d05f1{serial:04}");

    create_lvol(lvs, &name, &uuid, n, thin).await
}

/// TODO
async fn create_filler_replica(
    lvs: &Lvs,
    serial: u32,
    n: u64,
) -> Result<Lvol, LvsError> {
    let name = format!("filler_{serial}");
    let uuid = format!("56723e54-dc86-45f6-b55b-e44d05f1{serial:04}");

    create_lvol(lvs, &name, &uuid, n, false).await
}

/// TODO
async fn create_lvol(
    lvs: &Lvs,
    name: &str,
    uuid: &str,
    n: u64,
    thin: bool,
) -> Result<Lvol, LvsError> {
    let et = unsafe { G_USE_EXTENT_TABLE };

    println!(
        "Creating lvol '{name}': size = {n} cluster(s), thin: {thin}, et: {et}",
    );

    let opts = ReplicaArgs {
        name: name.to_owned(),
        size: lvs.cluster_size() as u64 * n,
        uuid: uuid.to_string(),
        thin,
        entity_id: None,
        use_extent_table: Some(et),
    };

    lvs.create_lvol_with_opts(opts).await.map_err(|err| {
        println!("Failed to create lvol '{name}': {err}");
        err
    })
}
