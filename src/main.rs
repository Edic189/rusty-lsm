// src/main.rs
use anyhow::Result;
use rusty_lsm::engine::StorageEngine;
use std::io::{self, Write};

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing for observability
    tracing_subscriber::fmt::init();

    let engine = StorageEngine::new("./db_data").await?;
    println!("--- Rusty LSM-Tree Engine Loaded ---");
    println!("Commands: put [k] [v], get [k], delete [k], flush, exit");

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
            ["flush"] => {
                engine.flush().await?;
                println!("Flush complete.");
            }
            ["exit"] => break,
            _ => println!("Unknown command."),
        }
    }

    Ok(())
}
