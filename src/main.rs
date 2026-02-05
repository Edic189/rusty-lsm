use anyhow::Result;
use rusty_lsm::engine::StorageEngine;
use std::io::{self, Write};
use std::ops::Bound;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();

    let engine = StorageEngine::new("./db_data").await?;
    println!("--- Rusty LSM-Tree Engine Loaded ---");
    println!("Commands:");
    println!("  put [k] [v]");
    println!("  get [k]");
    println!("  delete [k]");
    println!("  scan [start_k] [end_k]  (inclusive start, exclusive end)");
    println!("  flush");
    println!("  compact");
    println!("  exit");

    loop {
        print!("> ");
        io::stdout().flush()?;

        let mut input = String::new();
        io::stdin().read_line(&mut input)?;
        let parts: Vec<&str> = input.trim().split_whitespace().collect();

        match parts.as_slice() {
            ["put", k, v] => {
                engine.put(k.to_string(), v.to_string()).await?;
                println!("OK");
            }
            ["get", k] => match engine.get(k.as_bytes()).await? {
                Some(v) => println!("{}", String::from_utf8_lossy(&v)),
                None => println!("(not found)"),
            },
            ["delete", k] => {
                engine.delete(k.to_string()).await?;
                println!("OK (tombstone set)");
            }
            ["scan", start, end] => {
                let start_b = Bound::Included(start.as_bytes().to_vec());
                let end_b = Bound::Excluded(end.as_bytes().to_vec());

                // 'results' je sada iterator, ne vektor
                let results = engine.scan((start_b, end_b)).await?;

                let mut count = 0;
                println!("Scanning items...");

                // Iteriramo kroz stream
                for (k, v) in results {
                    println!(
                        "{} = {}",
                        String::from_utf8_lossy(&k),
                        String::from_utf8_lossy(&v)
                    );
                    count += 1;
                }
                println!("Total found: {}", count);
            }
            ["scan"] => {
                let results = engine.scan(..).await?;

                let mut count = 0;
                println!("Scanning all items...");

                for (k, v) in results {
                    println!(
                        "{} = {}",
                        String::from_utf8_lossy(&k),
                        String::from_utf8_lossy(&v)
                    );
                    count += 1;
                }
                println!("Total found: {}", count);
            }
            ["flush"] => {
                engine.flush().await?;
                println!("Flush complete.");
            }
            ["compact"] => {
                println!("Compacting SSTables...");
                engine.compact().await?;
                println!("Compaction complete.");
            }
            ["exit"] => break,
            _ => println!("Unknown command."),
        }
    }

    Ok(())
}
