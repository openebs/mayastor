use nvmeadm::nvmf_discovery::{disconnect, DiscoveryBuilder};

#[test]
fn disovery_test() {
    let mut explorer = DiscoveryBuilder::default()
        .transport("tcp".to_string())
        .traddr("127.0.0.01".to_string())
        .trsvcid(4420)
        .build()
        .unwrap();

    // only root can discover
    let _ = explorer.discover();
}

#[test]
fn connect_test() {
    let mut explorer = DiscoveryBuilder::default()
        .transport("tcp".to_string())
        .traddr("127.0.0.01".to_string())
        .trsvcid(4420)
        .build()
        .unwrap();

    // only root can discover and connect
    let _ = explorer.discover();
    let _ = explorer.connect("mynqn");
}

#[test]
fn disconnect_test() {
    let _ = disconnect("mynqn");
}
