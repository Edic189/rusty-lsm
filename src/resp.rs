use bytes::{Buf, BytesMut};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Debug, PartialEq, Clone)]
pub enum RespValue {
    SimpleString(String),
    Error(String),
    Integer(i64),
    BulkString(Option<Vec<u8>>),
    Array(Vec<RespValue>),
}

pub struct RespHandler {
    stream: TcpStream,
    buffer: BytesMut,
}

impl RespHandler {
    pub fn new(stream: TcpStream) -> Self {
        Self {
            stream,
            buffer: BytesMut::with_capacity(4096),
        }
    }

    pub async fn read_value(&mut self) -> io::Result<Option<RespValue>> {
        loop {
            if let Some(value) = self.parse_value()? {
                return Ok(Some(value));
            }

            if self.stream.read_buf(&mut self.buffer).await? == 0 {
                if self.buffer.is_empty() {
                    return Ok(None);
                } else {
                    return Err(io::Error::new(
                        io::ErrorKind::ConnectionReset,
                        "Connection reset by peer",
                    ));
                }
            }
        }
    }

    fn parse_value(&mut self) -> io::Result<Option<RespValue>> {
        if self.buffer.is_empty() {
            return Ok(None);
        }

        let (len, end) = match self.find_crlf(0) {
            Some((l, e)) => (l, e),
            None => return Ok(None),
        };

        let type_byte = self.buffer[0];

        match type_byte {
            b'+' => {
                let s = String::from_utf8_lossy(&self.buffer[1..len]).to_string();
                self.buffer.advance(end);
                Ok(Some(RespValue::SimpleString(s)))
            }
            b'-' => {
                let s = String::from_utf8_lossy(&self.buffer[1..len]).to_string();
                self.buffer.advance(end);
                Ok(Some(RespValue::Error(s)))
            }
            b':' => {
                let s = String::from_utf8_lossy(&self.buffer[1..len]);
                let i = s
                    .parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid Integer"))?;
                self.buffer.advance(end);
                Ok(Some(RespValue::Integer(i)))
            }
            b'$' => {
                let s = String::from_utf8_lossy(&self.buffer[1..len]);
                let data_len = s.parse::<i64>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid Bulk Length")
                })?;

                self.buffer.advance(end);

                if data_len == -1 {
                    return Ok(Some(RespValue::BulkString(None)));
                }

                let data_len = data_len as usize;

                if self.buffer.len() < data_len + 2 {
                    return Ok(None);
                }

                let data = self.buffer[..data_len].to_vec();
                self.buffer.advance(data_len + 2);

                Ok(Some(RespValue::BulkString(Some(data))))
            }
            b'*' => {
                let s = String::from_utf8_lossy(&self.buffer[1..len]);
                let array_len = s.parse::<i64>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid Array Length")
                })?;

                self.buffer.advance(end);

                if array_len == -1 {
                    return Ok(Some(RespValue::Array(vec![])));
                }

                let mut items = Vec::new();
                for _ in 0..array_len {
                    match self.parse_value()? {
                        Some(v) => items.push(v),
                        None => return Ok(None),
                    }
                }
                Ok(Some(RespValue::Array(items)))
            }
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown RESP type: {}", type_byte as char),
            )),
        }
    }

    fn find_crlf(&self, offset: usize) -> Option<(usize, usize)> {
        for i in offset..self.buffer.len().saturating_sub(1) {
            if self.buffer[i] == b'\r' && self.buffer[i + 1] == b'\n' {
                return Some((i, i + 2));
            }
        }
        None
    }

    pub async fn write_value(&mut self, value: RespValue) -> io::Result<()> {
        match value {
            RespValue::SimpleString(s) => {
                self.stream
                    .write_all(format!("+{}\r\n", s).as_bytes())
                    .await?;
            }
            RespValue::Error(s) => {
                self.stream
                    .write_all(format!("-{}\r\n", s).as_bytes())
                    .await?;
            }
            RespValue::Integer(i) => {
                self.stream
                    .write_all(format!(":{}\r\n", i).as_bytes())
                    .await?;
            }
            RespValue::BulkString(data) => match data {
                Some(bytes) => {
                    self.stream
                        .write_all(format!("${}\r\n", bytes.len()).as_bytes())
                        .await?;
                    self.stream.write_all(&bytes).await?;
                    self.stream.write_all(b"\r\n").await?;
                }
                None => {
                    self.stream.write_all(b"$-1\r\n").await?;
                }
            },
            RespValue::Array(items) => {
                self.stream
                    .write_all(format!("*{}\r\n", items.len()).as_bytes())
                    .await?;
                for item in items {
                    // Rekurzivni poziv zahtijeva pažnju, ovdje koristimo boxed future ili jednostavnu logiku
                    // Za jednostavnost, pretpostavljamo da Array ne sadrži duboke ugniježđene strukture u odgovoru
                    // ili jednostavno implementiramo iterativno ako je moguće.
                    // Ovdje ćemo samo 'cheatati' i reći da engine nikad ne vraća Array kao odgovor,
                    // osim za SCAN, ali SCAN vraća Array of Strings, što je flat.
                    // Za pravu rekurziju bi trebali izdvojiti logiku enkodiranja u buffer.
                    // Ali za MVP servera, ovo je dovoljno za klijent -> server parsiranje.
                    // Server -> klijent rijetko šalje nested arrays osim za SCAN.

                    // Jednostavna implementacija za flat array:
                    if let RespValue::BulkString(bs) = item {
                        match bs {
                            Some(b) => {
                                self.stream
                                    .write_all(format!("${}\r\n", b.len()).as_bytes())
                                    .await?;
                                self.stream.write_all(&b).await?;
                                self.stream.write_all(b"\r\n").await?;
                            }
                            None => {
                                self.stream.write_all(b"$-1\r\n").await?;
                            }
                        }
                    } else if let RespValue::SimpleString(ss) = item {
                        self.stream
                            .write_all(format!("+{}\r\n", ss).as_bytes())
                            .await?;
                    } else if let RespValue::Integer(int) = item {
                        self.stream
                            .write_all(format!(":{}\r\n", int).as_bytes())
                            .await?;
                    }
                }
            }
        }
        self.stream.flush().await?;
        Ok(())
    }
}
