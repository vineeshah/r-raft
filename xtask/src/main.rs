//! Developer task runner. Invoke with `cargo xtask <task>`.
//!
//! Centralizes repetitive dev workflows so contributors don't memorize long
//! command lines.

use std::process::Command as Proc;

use anyhow::{bail, Result};
use clap::{Parser, Subcommand};

#[derive(Debug, Parser)]
#[command(name = "xtask", about = "Developer task runner")]
struct Args {
    #[command(subcommand)]
    task: Task,
}

#[derive(Debug, Subcommand)]
enum Task {
    /// Build and start the local 3-node docker cluster.
    ClusterUp,
    /// Stop and remove the local cluster.
    ClusterDown,
    /// Run the benchmark suite (Phase 4).
    Bench,
}

fn compose(args: &[&str]) -> Result<()> {
    let status = Proc::new("docker").arg("compose").args(args).status()?;
    if !status.success() {
        bail!("`docker compose {}` failed", args.join(" "));
    }
    Ok(())
}

fn main() -> Result<()> {
    match Args::parse().task {
        Task::ClusterUp => compose(&["up", "--build", "-d"]),
        Task::ClusterDown => compose(&["down", "-v"]),
        Task::Bench => {
            let status = Proc::new(env!("CARGO"))
                .args(["bench", "--workspace"])
                .status()?;
            if !status.success() {
                bail!("cargo bench failed");
            }
            Ok(())
        }
    }
}
