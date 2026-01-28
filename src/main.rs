mod client;
mod server;

use clap::{Parser, Subcommand};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    #[command(subcommand)]
    cmd: Command,
}

#[derive(Subcommand)]
enum Command {
    Server {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
    },
    Client {
        #[arg(long, default_value = "127.0.0.1:50051")]
        addr: String,
        #[arg(long, default_value = "1")]
        id: usize,
    },
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let args = Args::parse();
    match args.cmd {
        Command::Server { addr } => server::run(addr).await?,
        Command::Client { addr, id } => client::run(addr, id).await?,
    }
    Ok(())
}
