use super::compose::rpc::v1::{
    test::{
        AddFaultInjectionRequest,
        FaultInjection,
        ListFaultInjectionsRequest,
        RemoveFaultInjectionRequest,
    },
    SharedRpcHandle,
    Status,
};

pub async fn add_fault_injection(
    rpc: SharedRpcHandle,
    inj_uri: &str,
) -> Result<(), Status> {
    rpc.lock()
        .await
        .test
        .add_fault_injection(AddFaultInjectionRequest {
            uri: inj_uri.to_owned(),
        })
        .await
        .map(|r| r.into_inner())
}

pub async fn remove_fault_injection(
    rpc: SharedRpcHandle,
    inj_uri: &str,
) -> Result<(), Status> {
    rpc.lock()
        .await
        .test
        .remove_fault_injection(RemoveFaultInjectionRequest {
            uri: inj_uri.to_owned(),
        })
        .await
        .map(|r| r.into_inner())
}

pub async fn list_fault_injections(
    rpc: SharedRpcHandle,
) -> Result<Vec<FaultInjection>, Status> {
    rpc.lock()
        .await
        .test
        .list_fault_injections(ListFaultInjectionsRequest {})
        .await
        .map(|r| r.into_inner().injections)
}
