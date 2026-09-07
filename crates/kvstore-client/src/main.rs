//! `kvctl` — command-line client for the distributed KV store.

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use kvstore_proto::kv::kv_client::KvClient;
use kvstore_proto::kv::{DeleteRequest, GetRequest, PutRequest};

#[derive(Debug, Parser)]
#[command(name = "kvctl", version, about)]
struct Args {
    /// Address of any cluster node (writes are redirected to the leader).
    #[arg(long, env = "KV_ENDPOINT", default_value = "http://127.0.0.1:5400")]
    endpoint: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Read the value for a key.
    Get {
        /// The key to read.
        key: String,
        /// Route through the leader for a linearizable read.
        #[arg(long)]
        linearizable: bool,
    },
    /// Write a value for a key.
    Put {
        /// The key to write.
        key: String,
        /// The value to store.
        value: String,
    },
    /// Delete a key.
    Delete {
        /// The key to remove.
        key: String,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    let mut client = KvClient::connect(args.endpoint.clone())
        .await
        .with_context(|| format!("connecting to {}", args.endpoint))?;

    match args.command {
        Command::Get { key, linearizable } => {
            let resp = client
                .get(GetRequest {
                    key: key.into_bytes(),
                    linearizable,
                })
                .await?
                .into_inner();
            if resp.found {
                println!("{}", String::from_utf8_lossy(&resp.value));
            } else {
                println!("(nil)");
            }
        }
        Command::Put { key, value } => {
            client
                .put(PutRequest {
                    key: key.into_bytes(),
                    value: value.into_bytes(),
                })
                .await?;
            println!("OK");
        }
        Command::Delete { key } => {
            client
                .delete(DeleteRequest {
                    key: key.into_bytes(),
                })
                .await?;
            println!("OK");
        }
    }
    Ok(())
}
