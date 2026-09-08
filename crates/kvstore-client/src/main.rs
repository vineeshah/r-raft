//! `kvctl` — command-line client for the distributed KV store.
//!
//! Phase 0: defines the command surface. Wire-up to the gRPC `Kv` service lands
//! in Phase 3.

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "kvctl", version, about)]
struct Args {
    /// Address of any cluster node.
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

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.command {
        Command::Get { key } => println!("(scaffold) GET {key} @ {}", args.endpoint),
        Command::Put { key, value } => {
            println!("(scaffold) PUT {key}={value} @ {}", args.endpoint)
        }
        Command::Delete { key } => println!("(scaffold) DELETE {key} @ {}", args.endpoint),
    }
    Ok(())
}
