//!
//! methods to interact with the nexus child

use crate::context::Context;
use ::rpc::mayastor as rpc;
use clap::{App, AppSettings, Arg, ArgMatches, SubCommand};
use tonic::Status;

pub async fn handler(
    ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    match matches.subcommand() {
        ("fault", Some(args)) => fault(ctx, &args).await,
        (cmd, _) => {
            Err(Status::not_found(format!("command {} does not exist", cmd)))
        }
    }
}

// TODO: Make subcommands use the name of the child rather than the uri
pub fn subcommands<'a, 'b>() -> App<'a, 'b> {
    let fault = SubCommand::with_name("fault")
        .about("fault a child")
        .arg(
            Arg::with_name("uuid")
                .required(true)
                .index(1)
                .help("uuid of the nexus"),
        )
        .arg(
            Arg::with_name("uri")
                .required(true)
                .index(2)
                .help("uri of the child"),
        );

    SubCommand::with_name("child")
        .settings(&[
            AppSettings::SubcommandRequiredElseHelp,
            AppSettings::ColoredHelp,
            AppSettings::ColorAlways,
        ])
        .about("Nexus child management")
        .subcommand(fault)
}

async fn fault(
    mut ctx: Context,
    matches: &ArgMatches<'_>,
) -> Result<(), Status> {
    let uuid = matches.value_of("uuid").unwrap().to_string();
    let uri = matches.value_of("uri").unwrap().to_string();

    ctx.v2(&format!("Faulting child {} on nexus {}", uri, uuid));
    ctx.client
        .fault_nexus_child(rpc::FaultNexusChildRequest {
            uuid: uuid.clone(),
            uri: uri.clone(),
        })
        .await?;
    ctx.v1(&format!("Faulted child {} on nexus {}", uri, uuid));
    Ok(())
}
