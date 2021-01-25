use kube::api::{Api, DeleteParams, ListParams, Meta, PostParams};
use kube_derive::CustomResource;
use rest_client::versions::v0::*;
use serde::{Deserialize, Serialize};
use std::convert::TryFrom;
use structopt::StructOpt;
use tracing::{debug, error, info, instrument};

#[derive(Debug, StructOpt)]
struct CliArgs {
    /// The Rest Server hostname to connect to
    /// Default: localhost:8080
    #[structopt(long, short, default_value = "localhost:8080")]
    rest: String,

    /// Polling period
    #[structopt(long, short, default_value = "30s")]
    period: humantime::Duration,

    /// Trace rest requests to the Jaeger endpoint agent
    #[structopt(long, short)]
    jaeger: Option<String>,
}

#[derive(CustomResource, Deserialize, Serialize, Clone, Debug)]
#[kube(
    group = "openebs.io",
    version = "v1alpha1",
    kind = "MayastorNode",
    namespaced
)]
#[kube(apiextensions = "v1beta1")]
#[kube(status = "String")]
#[serde(rename_all = "camelCase")]
pub struct MayastorNodeSpec {
    pub grpc_endpoint: String,
}

impl TryFrom<&MayastorNode> for Node {
    type Error = strum::ParseError;
    fn try_from(kube_node: &MayastorNode) -> Result<Self, Self::Error> {
        Ok(Node {
            id: NodeId::from(kube_node.name()),
            grpc_endpoint: kube_node.spec.grpc_endpoint.clone(),
            state: kube_node
                .status
                .as_ref()
                .unwrap_or(&"".to_string())
                .parse()?,
        })
    }
}

use opentelemetry::{
    global,
    sdk::{propagation::TraceContextPropagator, trace::Tracer},
};
use opentelemetry_jaeger::Uninstall;

fn init_tracing() -> Option<(Tracer, Uninstall)> {
    if let Ok(filter) = tracing_subscriber::EnvFilter::try_from_default_env() {
        tracing_subscriber::fmt().with_env_filter(filter).init();
    } else {
        tracing_subscriber::fmt().with_env_filter("info").init();
    }
    if let Some(agent) = CliArgs::from_args().jaeger {
        tracing::info!("Starting jaeger trace pipeline at {}...", agent);
        // Start a new jaeger trace pipeline
        global::set_text_map_propagator(TraceContextPropagator::new());
        let (_tracer, _uninstall) = opentelemetry_jaeger::new_pipeline()
            .with_agent_endpoint(agent)
            .with_service_name("node-operator")
            .install()
            .expect("Jaeger pipeline install error");
        Some((_tracer, _uninstall))
    } else {
        None
    }
}

#[actix_web::main]
async fn main() -> anyhow::Result<()> {
    // need to keep the jaeger pipeline tracer alive, if enabled
    let _tracer = init_tracing();

    let polling_period = CliArgs::from_args().period.into();

    let rest_url = format!("https://{}", CliArgs::from_args().rest);
    let rest_cli = rest_client::ActixRestClient::new(
        &rest_url,
        CliArgs::from_args().jaeger.is_some(),
    )?;

    let kube_client = kube::Client::try_default().await?;
    let namespace = "mayastor";

    // Validate that our "CRD" is up to date?

    // Manage the MayastorNode CR
    let nodes_api: Api<MayastorNode> =
        Api::namespaced(kube_client.clone(), namespace);

    loop {
        // Poll for kubernetes nodes and rest nodes
        // Reconcile from rest into kubernetes
        if let Err(error) = polling_work(&nodes_api, rest_cli.v0()).await {
            error!("Error while polling: {}", error);
        }

        // Sleep till the next poll
        tokio::time::delay_for(polling_period).await;
    }
}

/// This isn't quite a reconciler as no action is taken from k8s MayastorNodes
/// (msn), in fact, they should not be updated by the user.
/// We simply forward/translate control plane nodes into k8s nodes.
#[instrument(skip(nodes_api, rest_cli))]
async fn polling_work(
    nodes_api: &Api<MayastorNode>,
    rest_cli: impl RestClient,
) -> anyhow::Result<()> {
    // Fetch all nodes as seen by the control plane via REST
    let rest_nodes = rest_cli.get_nodes().await?;
    println!("Retrieved rest nodes: {:?}", rest_nodes);

    // Fetch all node CRD's from k8s
    let kube_nodes = nodes_get_all(&nodes_api).await?;
    debug!("Retrieved kube nodes: {:?}", kube_nodes);

    // control plane nodes which do not exist in k8s
    let new_nodes = rest_nodes
        .iter()
        .filter(|node| {
            !kube_nodes
                .iter()
                .any(|kube_node| kube_node.name() == node.id.to_string())
        })
        .collect::<Vec<&Node>>();

    // k8s nodes which no longer exist in the control plane
    let delete_nodes = kube_nodes
        .iter()
        .filter(|kube_node| {
            !rest_nodes
                .iter()
                .any(|node| kube_node.name() == node.id.to_string())
        })
        .collect::<Vec<&MayastorNode>>();

    // k8s nodes are out of date so need an update
    let update_nodes = rest_nodes
        .iter()
        .filter(|&node| {
            kube_nodes.iter().any(|kube_node| {
                let node_from_kube = Node::try_from(kube_node);
                if let Ok(kube_node) = node_from_kube.as_ref() {
                    kube_node != node
                } else {
                    error!(
                        "Node {:#?} is not formatted properly.",
                        node_from_kube
                    );
                    true
                }
            })
        })
        .collect::<Vec<&Node>>();

    if !new_nodes.is_empty() {
        info!("Creating nodes: {:?}", new_nodes);

        for node in new_nodes {
            if let Err(error) = node_create(&nodes_api, &node).await {
                error!(
                    "Failed to create kube_node: {}, error={}",
                    node.id, error
                );
            }
        }
    }

    if !update_nodes.is_empty() {
        info!("Updating nodes: {:?}", update_nodes);

        for node in update_nodes {
            if let Err(error) = node_update(&nodes_api, &node).await {
                error!(
                    "Failed to update kube_node: {}, error={:?}",
                    node.id, error
                );
            }
        }
    }

    if !delete_nodes.is_empty() {
        info!("Deleting nodes: {:?}", delete_nodes);

        for node in delete_nodes {
            if let Err(error) = node_delete(&nodes_api, &Meta::name(node)).await
            {
                error!(
                    "Failed to delete kube_node: {}, error={:?}",
                    Meta::name(node),
                    error
                );
            }
        }
    }

    Ok(())
}

#[instrument(skip(nodes_api))]
async fn nodes_get_all(
    nodes_api: &Api<MayastorNode>,
) -> anyhow::Result<Vec<MayastorNode>> {
    let list_params = ListParams::default();
    let kube_nodes = nodes_api.list(&list_params).await?.items;
    Ok(kube_nodes)
}

#[instrument(skip(nodes_api))]
async fn node_create(
    nodes_api: &Api<MayastorNode>,
    node: &Node,
) -> anyhow::Result<()> {
    let kube_node = MayastorNode::new(
        node.id.as_str(),
        MayastorNodeSpec {
            grpc_endpoint: node.grpc_endpoint.clone(),
        },
    );

    let post_params = PostParams::default();
    let mut kube_node = nodes_api.create(&post_params, &kube_node).await?;

    let status = Some(node.state.to_string());
    kube_node.status = status.clone();
    let kube_node = nodes_api
        .replace_status(
            &Meta::name(&kube_node),
            &post_params,
            serde_json::to_vec(&kube_node)?,
        )
        .await?;
    assert_eq!(kube_node.status, status);

    Ok(())
}

#[instrument(skip(nodes_api))]
async fn node_update(
    nodes_api: &Api<MayastorNode>,
    node: &Node,
) -> anyhow::Result<()> {
    let post_params = PostParams::default();
    let status = Some(node.state.to_string());

    let mut kube_node = nodes_api.get(node.id.as_str()).await?;
    kube_node.status = status.clone();

    let kube_node = nodes_api
        .replace_status(
            &Meta::name(&kube_node),
            &post_params,
            serde_json::to_vec(&kube_node)?,
        )
        .await?;
    assert_eq!(kube_node.status, status);

    Ok(())
}

#[instrument(skip(nodes_api))]
async fn node_delete(
    nodes_api: &Api<MayastorNode>,
    name: &str,
) -> anyhow::Result<()> {
    let delete_params = DeleteParams::default();
    let _ = nodes_api.delete(name, &delete_params).await?;
    Ok(())
}
