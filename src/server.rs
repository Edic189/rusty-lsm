use crate::engine::StorageEngine;
use anyhow::Result;
use std::ops::Bound;
use std::sync::Arc;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

pub async fn start_server(engine: Arc<StorageEngine>) -> Result<()> {
    let addr = format!("0.0.0.0:{}", engine.config.port);
    let listener = TcpListener::bind(&addr).await?;

    tracing::info!("Server listening on {}", addr);

    loop {
        let (mut socket, _) = listener.accept().await?;
        let engine = engine.clone();

        tokio::spawn(async move {
            let (reader, mut writer) = socket.split();
            let mut reader = BufReader::new(reader);
            let mut line = String::new();

            loop {
                line.clear();
                let bytes_read = reader.read_line(&mut line).await.unwrap_or(0);
                if bytes_read == 0 {
                    break;
                }

                let parts: Vec<&str> = line.trim().split_whitespace().collect();

                let mut response = String::new();

                match parts.as_slice() {
                    ["PUT", k, v] => match engine.put(k.to_string(), v.to_string()).await {
                        Ok(_) => response.push_str("OK\n"),
                        Err(e) => response.push_str(&format!("ERR: {}\n", e)),
                    },
                    ["GET", k] => match engine.get(k.as_bytes()).await {
                        Ok(Some(v)) => {
                            response.push_str(&format!("{}\n", String::from_utf8_lossy(&v)))
                        }
                        Ok(None) => response.push_str("(nil)\n"),
                        Err(e) => response.push_str(&format!("ERR: {}\n", e)),
                    },
                    ["DEL", k] => match engine.delete(k.to_string()).await {
                        Ok(_) => response.push_str("OK\n"),
                        Err(e) => response.push_str(&format!("ERR: {}\n", e)),
                    },
                    ["SCAN", start, end] => {
                        let start_b = Bound::Included(start.as_bytes().to_vec());
                        let end_b = Bound::Excluded(end.as_bytes().to_vec());

                        match engine.scan((start_b, end_b)).await {
                            Ok(iter) => {
                                let mut count = 0;
                                for (k, v) in iter {
                                    response.push_str(&format!(
                                        "{}={}\n",
                                        String::from_utf8_lossy(&k),
                                        String::from_utf8_lossy(&v)
                                    ));
                                    count += 1;
                                    if count > 1000 {
                                        response.push_str("ERR: Scan limit reached\n");
                                        break;
                                    }
                                }
                                response.push_str("END\n");
                            }
                            Err(e) => response.push_str(&format!("ERR: {}\n", e)),
                        }
                    }
                    ["STATS"] => {
                        // NOVA KOMANDA
                        let stats = engine.stats().await;
                        response.push_str(&stats);
                        response.push('\n');
                    }
                    _ => response.push_str("ERR: Unknown command\n"),
                };

                if let Err(_) = writer.write_all(response.as_bytes()).await {
                    break;
                }
            }
        });
    }
}
