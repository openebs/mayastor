pub mod dns;
mod empty;
pub mod jaeger;
pub mod mayastor;
pub mod nats;
pub mod rest;

pub use ::nats::*;
pub use dns::*;
pub use empty::*;
pub use jaeger::*;
pub use mayastor::*;
pub use rest::*;

use super::StartOptions;
use async_trait::async_trait;
use composer::{Binary, Builder, BuilderConfigure, ComposeTest, ContainerSpec};
use mbus_api::{
    v0::{ChannelVs, Liveness},
    Message,
};
use paste::paste;
use std::{cmp::Ordering, str::FromStr};
use structopt::StructOpt;
use strum::VariantNames;
use strum_macros::{EnumVariantNames, ToString};
pub(crate) type Error = Box<dyn std::error::Error>;

#[macro_export]
macro_rules! impl_ctrlp_agents {
    ($($name:ident,)+) => {
        #[derive(Debug, Clone)]
        pub(crate) struct ControlPlaneAgents(Vec<ControlPlaneAgent>);

        #[derive(Debug, Clone, StructOpt, ToString, EnumVariantNames)]
        #[structopt(about = "Control Plane Agents")]
        pub(crate) enum ControlPlaneAgent {
            Empty(Empty),
            $(
                $name($name),
            )+
        }

        impl From<&ControlPlaneAgent> for Component {
            fn from(ctrlp_svc: &ControlPlaneAgent) -> Self {
                match ctrlp_svc {
                    ControlPlaneAgent::Empty(obj) => Component::Empty(obj.clone()),
                    $(ControlPlaneAgent::$name(obj) => Component::$name(obj.clone()),)+
                }
            }
        }

        paste! {
            impl FromStr for ControlPlaneAgent {
                type Err = String;

                fn from_str(source: &str) -> Result<Self, Self::Err> {
                    Ok(match source.trim().to_ascii_lowercase().as_str() {
                        "" => Self::Empty(Empty::default()),
                        $(stringify!([<$name:lower>]) => Self::$name($name::default()),)+
                        _ => return Err(format!(
                            "\"{}\" is an invalid type of agent! Available types: {:?}",
                            source,
                            Self::VARIANTS
                        )),
                    })
                }
            }
        }

        $(#[async_trait]
        impl ComponentAction for $name {
            fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
                let name = stringify!($name).to_ascii_lowercase();
                if options.build {
                    let status = std::process::Command::new("cargo")
                        .args(&["build", "-p", "agents", "--bin", &name])
                        .status()?;
                    build_error(&format!("the {} agent", name), status.code())?;
                }
                Ok(cfg.add_container_bin(
                    &name,
                    Binary::from_dbg(&name).with_nats("-n"),
                ))
            }
            async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
                let name = stringify!($name).to_ascii_lowercase();
                cfg.start(&name).await?;
                Liveness {}.request_on(ChannelVs::$name).await?;
                Ok(())
            }
        })+
    };
    ($($name:ident), +) => {
        impl_ctrlp_agents!($($name,)+);
    };
}

#[macro_export]
macro_rules! impl_ctrlp_operators {
    ($($name:ident,)+) => {
        #[derive(Debug, Clone)]
        pub(crate) struct ControlPlaneOperators(Vec<ControlPlaneOperator>);

        #[derive(Debug, Clone, StructOpt, ToString, EnumVariantNames)]
        #[structopt(about = "Control Plane Operators")]
        pub(crate) enum ControlPlaneOperator {
            Empty(Empty),
            $(
                $name(paste!{[<$name Op>]}),
            )+
        }

        paste! {
            impl From<&ControlPlaneOperator> for Component {
                fn from(ctrlp_svc: &ControlPlaneOperator) -> Self {
                    match ctrlp_svc {
                        ControlPlaneOperator::Empty(obj) => Component::Empty(obj.clone()),
                        $(ControlPlaneOperator::$name(obj) => Component::[<$name Op>](obj.clone()),)+
                    }
                }
            }
        }

        paste! {
            impl FromStr for ControlPlaneOperator {
                type Err = String;

                fn from_str(source: &str) -> Result<Self, Self::Err> {
                    Ok(match source.trim().to_ascii_lowercase().as_str() {
                        "" => Self::Empty(Default::default()),
                        $(stringify!([<$name:lower>]) => Self::$name(<paste!{[<$name Op>]}>::default()),)+
                        _ => return Err(format!(
                            "\"{}\" is an invalid type of operator! Available types: {:?}",
                            source,
                            Self::VARIANTS
                        )),
                    })
                }
            }
        }

        $(#[async_trait]
        impl ComponentAction for paste!{[<$name Op>]} {
            fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
                let name = format!("{}-op", stringify!($name).to_ascii_lowercase());
                if options.build {
                    let status = std::process::Command::new("cargo")
                        .args(&["build", "-p", "operators", "--bin", &name])
                        .status()?;
                    build_error(&format!("the {} operator", name), status.code())?;
                }
                let rest = format!("http://rest.{}:8081", cfg.get_name());
                let host_kube_config = match &options.kube_config {
                    Some(config) => config.clone(),
                    None => {
                        match std::env::var("USER") {
                            Ok(user) => format!("/home/{}/.kube/config", user),
                            Err(_) => "/root/.kube/config".to_string(),
                        }
                    }
                };
                let kube_config = match options.base_image {
                    Some(_) => "/root/.kube/config",
                    None => "/.kube/config",
                };
                Ok(if options.jaeger {
                    let jaeger_config = format!("jaeger.{}:6831", cfg.get_name());
                    cfg.add_container_spec(
                        ContainerSpec::from_binary(
                            &name,
                            Binary::from_dbg(&name)
                                .with_args(vec!["-r", &rest])
                                .with_args(vec!["-j", &jaeger_config]),
                        )
                        .with_bind(&host_kube_config, kube_config),
                    )
                } else {
                    cfg.add_container_spec(
                        ContainerSpec::from_binary(
                            &name,
                            Binary::from_dbg(&name).with_args(vec!["-r", &rest])
                        )
                        .with_bind(&host_kube_config, kube_config),
                    )
                })
            }
            async fn start(&self, _options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
                // todo: wait for the rest server to be up
                let name = format!("{}-op", stringify!($name).to_ascii_lowercase());
                cfg.start(&name).await?;
                Ok(())
            }
        })+
    };
    ($($name:ident), +) => {
        impl_ctrlp_operators!($($name,)+);
    };
}

pub(crate) fn build_error(
    name: &str,
    status: Option<i32>,
) -> Result<(), Error> {
    let make_error = |extra: &str| {
        let error = format!("Failed to build {}: {}", name, extra);
        std::io::Error::new(std::io::ErrorKind::Other, error)
    };
    match status {
        Some(0) => Ok(()),
        Some(code) => {
            let error = format!("exited with code {}", code);
            Err(make_error(&error).into())
        }
        None => Err(make_error("interrupted by signal").into()),
    }
}

#[macro_export]
macro_rules! impl_component {
    ($($name:ident,$order:literal,)+) => {
        #[derive(Debug, Clone, StructOpt, ToString, EnumVariantNames, Eq, PartialEq)]
        #[structopt(about = "Control Plane Components")]
        pub(crate) enum Component {
            $(
                $name($name),
            )+
        }

        #[derive(Debug, Clone)]
        pub(crate) struct Components(Vec<Component>, StartOptions);
        impl BuilderConfigure for Components {
            fn configure(&self, cfg: Builder) -> Result<Builder, Error> {
                let mut cfg = cfg;
                for component in &self.0 {
                    cfg = component.configure(&self.1, cfg)?;
                }
                Ok(cfg)
            }
        }

        impl Components {
            pub(crate) fn push_generic_components(&mut self, name: &str, component: Component) {
                if !ControlPlaneAgent::VARIANTS.iter().any(|&s| s == name) &&
                    !ControlPlaneOperator::VARIANTS.iter().any(|&s| &format!("{}Op", s) == name) {
                    self.0.push(component);
                }
            }
            pub(crate) fn new(options: StartOptions) -> Components {
                let agents = options.agents.clone();
                let operators = options.operators.clone().unwrap_or_default();
                let mut components = agents
                    .iter()
                    .map(Component::from)
                    .collect::<Vec<Component>>();
                components.extend(operators
                                    .iter()
                                    .map(Component::from)
                                    .collect::<Vec<Component>>());

                let mut components = Components(components, options.clone());
                $(components.push_generic_components(stringify!($name), $name::default().into());)+
                components.0.sort();
                components
            }
            pub(crate) async fn start(&self, cfg: &ComposeTest) -> Result<(), Error> {
                for component in &self.0 {
                    component.start(&self.1, cfg).await?;
                }
                Ok(())
            }
        }

        #[async_trait]
        pub(crate) trait ComponentAction {
            fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error>;
            async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error>;
        }

        #[async_trait]
        impl ComponentAction for Component {
            fn configure(&self, options: &StartOptions, cfg: Builder) -> Result<Builder, Error> {
                match self {
                    $(Self::$name(obj) => obj.configure(options, cfg),)+
                }
            }
            async fn start(&self, options: &StartOptions, cfg: &ComposeTest) -> Result<(), Error> {
                match self {
                    $(Self::$name(obj) => obj.start(options, cfg).await,)+
                }
            }
        }

        $(impl From<$name> for Component {
            fn from(from: $name) -> Component {
                Component::$name(from)
            }
        })+

        $(#[derive(Default, Debug, Clone, StructOpt, Eq, PartialEq)]
        pub(crate) struct $name {})+

        impl Component {
            fn boot_order(&self) -> u32 {
                match self {
                    $(Self::$name(_) => $order,)+
                }
            }
        }

        impl PartialOrd for Component {
            fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
                self.boot_order().partial_cmp(&other.boot_order())
            }
        }
        impl Ord for Component {
            fn cmp(&self, other: &Self) -> Ordering {
                self.boot_order().cmp(&other.boot_order())
            }
        }
    };
    ($($name:ident, $order:ident), +) => {
        impl_component!($($name,$order)+);
    };
}

// Component Name and bootstrap ordering
// from lower to high
impl_component! {
    Empty,      0,
    Dns,        0,
    Jaeger,     0,
    Nats,       0,
    Rest,       1,
    Mayastor,   1,
    Node,       2,
    Pool,       3,
    Volume,     3,
    NodeOp,     4,
}

// Message Bus Control Plane Agents
impl_ctrlp_agents!(Node, Pool, Volume);

// Kubernetes Mayastor Low-level Operators
impl_ctrlp_operators!(Node);
