use crate::engine::StorageEngine;
use crate::resp::{RespHandler, RespValue};
use anyhow::Result;
use std::sync::Arc;
use tokio::net::TcpListener;

pub async fn start_server(engine: Arc<StorageEngine>) -> Result<()> {
    let addr = format!("0.0.0.0:{}", engine.config.port);
    let listener = TcpListener::bind(&addr).await?;

    tracing::info!("Redis-compatible Server listening on {}", addr);

    loop {
        let (socket, _) = listener.accept().await?;
        let engine = engine.clone();

        tokio::spawn(async move {
            let mut handler = RespHandler::new(socket);

            loop {
                let frame = match handler.read_value().await {
                    Ok(Some(v)) => v,
                    Ok(None) => break,
                    Err(e) => {
                        tracing::error!("Connection error: {}", e);
                        break;
                    }
                };

                let response = if let RespValue::Array(args) = frame {
                    if args.is_empty() {
                        RespValue::Error("Empty command".to_string())
                    } else {
                        let command_name = match &args[0] {
                            RespValue::BulkString(Some(b)) => {
                                String::from_utf8_lossy(b).to_string().to_uppercase()
                            }
                            RespValue::SimpleString(s) => s.to_uppercase(),
                            _ => String::new(),
                        };

                        match command_name.as_str() {
                            "PING" => RespValue::SimpleString("PONG".to_string()),

                            "SET" => {
                                if args.len() < 3 {
                                    RespValue::Error(
                                        "ERR wrong number of arguments for 'set' command"
                                            .to_string(),
                                    )
                                } else {
                                    let key = extract_bytes(&args[1]);
                                    let val = extract_bytes(&args[2]);

                                    if let (Some(k), Some(v)) = (key, val) {
                                        match engine.put(k, v).await {
                                            Ok(_) => RespValue::SimpleString("OK".to_string()),
                                            Err(e) => RespValue::Error(e.to_string()),
                                        }
                                    } else {
                                        RespValue::Error("ERR invalid key or value".to_string())
                                    }
                                }
                            }

                            "GET" => {
                                if args.len() < 2 {
                                    RespValue::Error(
                                        "ERR wrong number of arguments for 'get' command"
                                            .to_string(),
                                    )
                                } else {
                                    let key = extract_bytes(&args[1]);
                                    if let Some(k) = key {
                                        match engine.get(&k).await {
                                            Ok(Some(v)) => RespValue::BulkString(Some(v)),
                                            Ok(None) => RespValue::BulkString(None),
                                            Err(e) => RespValue::Error(e.to_string()),
                                        }
                                    } else {
                                        RespValue::Error("ERR invalid key".to_string())
                                    }
                                }
                            }

                            "DEL" => {
                                if args.len() < 2 {
                                    RespValue::Error(
                                        "ERR wrong number of arguments for 'del' command"
                                            .to_string(),
                                    )
                                } else {
                                    let key = extract_bytes(&args[1]);
                                    if let Some(k) = key {
                                        match engine.delete(k).await {
                                            Ok(_) => RespValue::Integer(1),
                                            Err(e) => RespValue::Error(e.to_string()),
                                        }
                                    } else {
                                        RespValue::Error("ERR invalid key".to_string())
                                    }
                                }
                            }

                            "KEYS" | "SCAN" => match engine.scan(..).await {
                                Ok(iter) => {
                                    let mut keys = Vec::new();
                                    for (k, _) in iter {
                                        keys.push(RespValue::BulkString(Some(k)));
                                        if keys.len() >= 1000 {
                                            break;
                                        }
                                    }
                                    RespValue::Array(keys)
                                }
                                Err(e) => RespValue::Error(e.to_string()),
                            },

                            "STATS" => {
                                let stats = engine.stats().await;
                                RespValue::SimpleString(stats)
                            }

                            _ => {
                                RespValue::Error(format!("ERR unknown command '{}'", command_name))
                            }
                        }
                    }
                } else {
                    RespValue::Error("ERR request must be an array".to_string())
                };

                if let Err(e) = handler.write_value(response).await {
                    tracing::error!("Write error: {}", e);
                    break;
                }
            }
        });
    }
}

fn extract_bytes(val: &RespValue) -> Option<Vec<u8>> {
    match val {
        RespValue::BulkString(Some(v)) => Some(v.clone()),
        RespValue::SimpleString(s) => Some(s.as_bytes().to_vec()),
        _ => None,
    }
}
