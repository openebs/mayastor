use deployer_lib::{infra::Error, *};
use structopt::StructOpt;

#[tokio::main]
async fn main() -> Result<(), Error> {
    let cli_args = CliArgs::from_args();
    println!("Using options: {:?}", &cli_args);

    cli_args.execute().await
}
