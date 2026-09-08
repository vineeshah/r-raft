//! Developer task runner. Invoke with `cargo xtask <task>`.
//!
//! Centralizes repetitive dev workflows so contributors don't memorize long
//! command lines. Tasks are filled in as the corresponding phases land.

use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "xtask", about = "Developer task runner")]
struct Args {
    #[command(subcommand)]
    task: Task,
}

#[derive(Debug, Subcommand)]
enum Task {
    /// Bring up a local multi-node cluster (Phase 3).
    ClusterUp,
    /// Tear down the local cluster (Phase 3).
    ClusterDown,
    /// Run the benchmark suite (Phase 4).
    Bench,
}

fn main() -> anyhow::Result<()> {
    let args = Args::parse();
    match args.task {
        Task::ClusterUp => println!("cluster-up: not yet implemented (Phase 3)"),
        Task::ClusterDown => println!("cluster-down: not yet implemented (Phase 3)"),
        Task::Bench => println!("bench: not yet implemented (Phase 4)"),
    }
    Ok(())
}
