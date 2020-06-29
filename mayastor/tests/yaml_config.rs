use std::{fs::metadata, sync::Mutex, time::Duration};

use common::ms_exec::run_test;
use mayastor::{subsys, subsys::Config};

pub mod common;

#[test]
// Test we can start without any mayastor specific configuration.
fn yaml_default() {
    let args = vec!["-s".into(), "128".into()];
    run_test(Box::from(args), |ms| {
        // knock, anyone there?
        let out = ms
            .rpc_call("rpc_get_methods", serde_json::json!(null))
            .unwrap();
        assert_ne!(out.as_array().unwrap().len(), 0);
    });
}

#[test]
// The YAML file we load does not exist. The test ensures we create a new one
// when we save it where the values are bootstrapped with our defaults defined
// in ['Config::opts']
fn yaml_not_exist() {
    let args = vec![
        "-s".to_string(),
        "128".into(),
        "-y".into(),
        "/tmp/test.yaml".into(),
    ];

    // delete any existing file
    common::delete_file(&["/tmp/test.yaml".into()]);

    run_test(Box::from(args), |ms| {
        let out = ms
            .rpc_call("mayastor_config_export", serde_json::json!(null))
            .unwrap();
        assert_eq!(out, serde_json::Value::Null);
        assert_eq!(metadata("/tmp/test.yaml").unwrap().is_file(), true);
    });
}

#[test]
// Create a new config file with some bdevs in it. Write out the config file and
// then start mayastor using it. This tests that serialisation is done properly
// and that we indeed can create bdevs defined with in the config
fn yaml_load_from_existing() {
    let mut cfg = Config::default();
    common::truncate_file("/tmp/disk1.img", 1024 * 64);

    let bdev = subsys::BaseBdev {
        uri: "aio:///tmp/disk1.img?blk_size=512".to_string(),
        uuid: Some("3dbbaeb0-ec02-4962-99c5-4e8f67c6b80c".to_string()),
    };

    cfg.source = Some("/tmp/loadme.yaml".into());
    cfg.base_bdevs = Some(vec![bdev]);

    cfg.write("/tmp/loadme.yaml").unwrap();

    assert_eq!(
        std::fs::metadata("/tmp/loadme.yaml").unwrap().is_file(),
        true
    );

    let args = vec![
        "-s".to_string(),
        "128".to_string(),
        "-y".to_string(),
        "/tmp/loadme.yaml".to_string(),
    ];

    run_test(Box::from(args), |ms| {
        // get the config we just loaded and validate it's the bdev is in that
        // config
        let out = ms
            .rpc_call(
                "framework_get_config",
                serde_json::json!({"name": "mayastor"}),
            )
            .unwrap();

        let base_bdevs = out
            .as_object()
            .unwrap()
            .get("base_bdevs")
            .unwrap()
            .as_array()
            .unwrap();

        assert_eq!(
            base_bdevs[0]["uri"].as_str().unwrap(),
            "aio:///tmp/disk1.img?blk_size=512"
        );
        assert_eq!(
            base_bdevs[0]["uuid"].as_str().unwrap(),
            "3dbbaeb0-ec02-4962-99c5-4e8f67c6b80c"
        );

        // out of scope for testing this but -- lets ensure the bdev is actually
        // here
        let bdev = ms.rpc_call(
            "bdev_get_bdevs",
            serde_json::json!({"name": "aio:///tmp/disk1.img?blk_size=512"}),
        ).unwrap();

        assert_ne!(bdev.as_array().unwrap().len(), 0);
    });

    common::delete_file(&["/tmp/disk1.img".into()]);
}

#[test]
// In this test we want to validate that we can create a pool using a config
// file. Moreover, we also want to validate that if we have a pool, we can
// export the pool topology and then restart ourselves with that pool defined in
// the config. Said differently, import and export pools.
fn yaml_pool_tests() {
    let mut cfg = Config::default();
    common::delete_file(&["/tmp/disk1.img".into()]);
    common::truncate_file("/tmp/disk1.img", 1024 * 64);

    // create a config where and define the pool we want to create. The pool
    // does not exist, so we expect that it gets created -- and not imported.
    let pool = subsys::Pool {
        name: "tpool".to_string(),
        disks: vec!["/tmp/disk1.img".into()],
        blk_size: 512,
        io_if: 1,
    };

    // we use this UUID to ensure that the created pool is indeed  -- the pool
    // we later import. We use a mutex to get unwind safety.

    let uuid: Mutex<String> = Mutex::new("".into());

    cfg.source = Some("/tmp/pool.yaml".into());
    cfg.pools = Some(vec![pool]);
    cfg.nexus_opts.nvmf_enable = false;

    cfg.write("/tmp/pool.yaml").unwrap();

    // setup the arguments we want to load mayastor with
    let args = vec![
        "-s".to_string(),
        "128".into(),
        "-y".into(),
        "/tmp/pool.yaml".to_string(),
    ];

    run_test(Box::from(args.clone()), |ms| {
        let pools = common::retry(10, Duration::from_millis(500), || {
            let p = ms.rpc_call("list_pools", serde_json::json!(null)).unwrap();
            if p.is_array() {
                Ok(p)
            } else {
                Err(())
            }
        });

        assert_eq!(
            pools.as_array().unwrap()[0]
                .as_object()
                .unwrap()
                .get("name")
                .unwrap(),
            "tpool"
        );

        // Ok we got our pool, lets try to grab the UUID.  We don't have that
        // property in our code so use the builtin ones to extract it.
        let lvols = ms
            .rpc_call("bdev_lvol_get_lvstores", serde_json::json!(null))
            .unwrap();

        let lvol_uuid = lvols.as_array().unwrap()[0]
            .as_object()
            .unwrap()
            .get("uuid")
            .unwrap()
            .to_string();

        *uuid.lock().unwrap() = lvol_uuid;

        // delete our config file to validate that pool export logic works
        // properly
        common::delete_file(&["/tmp/pool.yaml".into()]);

        let out = ms
            .rpc_call("mayastor_config_export", serde_json::json!(null))
            .unwrap();
        assert_eq!(out, serde_json::Value::Null);
        assert_eq!(metadata("/tmp/pool.yaml").unwrap().is_file(), true);
    });

    // Part two, in a galaxy far far away... the string has been set by the
    // first jedi maya instance. Now we need to determine if we can trust the
    // import, or if a certain amount of fear is justified.
    //
    // Fear is the path to the dark side. Fear leads to anger. Anger leads to
    // hate. Hate leads to suffering.
    //
    // In episode one we used (attack of the) arg.clone() -- so we can use the
    // same arguments to start his next episode. We load the same config and
    // expect the UUID to be the exact same. As we do not specify a UUID
    // explicitly, matching UUIDs confirm that the pool has not been
    // recreated.
    run_test(Box::from(args), |ms| {
        let lvols = common::retry(10, Duration::from_millis(500), || {
            let vols = ms
                .rpc_call("bdev_lvol_get_lvstores", serde_json::json!(null))
                .unwrap();
            if vols.as_array().unwrap().is_empty() {
                Err(())
            } else {
                Ok(vols)
            }
        });

        // compare the UUID we stored from the first step, with the current
        assert_eq!(
            *uuid.lock().unwrap(),
            lvols.as_array().unwrap()[0]
                .as_object()
                .unwrap()
                .get("uuid")
                .unwrap()
                .to_string()
        );
    });

    // delete the pool
    common::delete_file(&["/tmp/disk1.img".into()]);
}

#[test]
// Try to see if we can start two mayastor instances where the nvmf and iSCSI
// target is disabled for one of them. If we did not disable one of them, one
// would fail to start.
fn yaml_multi_maya() {
    common::delete_file(&[
        "/tmp/first.yaml".to_string(),
        "/tmp/second.yaml".into(),
    ]);

    let mut first = Config::default();
    let second = Config::default();

    first.nexus_opts.iscsi_enable = false;
    first.nexus_opts.nvmf_enable = false;

    first.write("/tmp/first.yaml").unwrap();
    second.write("/tmp/second.yaml").unwrap();

    let first_args = vec![
        "-s".to_string(),
        "128".into(),
        "-y".into(),
        "/tmp/first.yaml".into(),
    ];

    let second_args = vec![
        "-s".to_string(),
        "128".into(),
        "-y".into(),
        "/tmp/second.yaml".into(),
    ];

    run_test(Box::from(first_args), |ms1| {
        let out = ms1
            .rpc_call("rpc_get_methods", serde_json::json!(null))
            .unwrap();
        assert_ne!(out.as_array().unwrap().len(), 0);

        run_test(Box::from(second_args), |ms2| {
            let out = ms2
                .rpc_call("rpc_get_methods", serde_json::json!(null))
                .unwrap();
            assert_ne!(out.as_array().unwrap().len(), 0);
        });
    });

    common::delete_file(&[
        "/tmp/first.yaml".to_string(),
        "/tmp/second.yaml".into(),
    ])
}
