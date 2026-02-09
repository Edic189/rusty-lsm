use anyhow::Result;
use clap::{Parser, Subcommand};
use rusty_lsm::config::LsmConfig;
use rusty_lsm::engine::StorageEngine;
use rusty_lsm::server::start_server;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::signal;
use tokio::time::{Duration, sleep};

#[derive(Parser)]
#[command(name = "rusty-lsm")]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    Start {
        #[arg(long, default_value = "./db_data")]
        data_dir: PathBuf,

        #[arg(long, default_value_t = 8080)]
        port: u16,

        #[arg(long, default_value_t = 1000)]
        cache_size: usize,

        #[arg(long, default_value_t = 4096)]
        block_size: usize,

        #[arg(long, default_value_t = 67108864)]
        memtable_size: usize,
    },
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let cli = Cli::parse();

    match cli.command {
        Commands::Start {
            data_dir,
            port,
            cache_size,
            block_size,
            memtable_size,
        } => {
            let config = LsmConfig {
                dir: data_dir,
                port,
                block_cache_capacity: cache_size,
                block_size,
                memtable_capacity: memtable_size,
                ..LsmConfig::default()
            };

            tracing::info!("Starting Engine...");
            let engine = Arc::new(StorageEngine::new(config).await?);

            let compact_engine = engine.clone();
            tokio::spawn(async move {
                loop {
                    sleep(Duration::from_secs(10)).await;
                    if let Err(e) = compact_engine.compact().await {
                        tracing::error!("Compaction error: {}", e);
                    }
                }
            });

            tracing::info!("Engine Ready. Press Ctrl+C to shutdown.");

            tokio::select! {
                res = start_server(engine.clone()) => {
                    if let Err(e) = res {
                        tracing::error!("Server error: {}", e);
                    }
                }
                _ = signal::ctrl_c() => {
                    tracing::info!("Shutdown signal received. Flushing data...");
                }
            }

            if let Err(e) = engine.flush().await {
                tracing::error!("Final flush failed: {}", e);
            } else {
                tracing::info!("Shutdown complete.");
            }
        }
    }

    Ok(())
}
