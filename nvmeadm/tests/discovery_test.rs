use nvmeadm::nvmf_discovery::DiscoveryBuilder;

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
