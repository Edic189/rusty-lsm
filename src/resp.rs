use bytes::{Buf, BytesMut};
use std::io::{self, Cursor};
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
            let mut cursor = Cursor::new(&self.buffer[..]);

            match check_and_parse(&mut cursor) {
                Ok(Some(value)) => {
                    let len = cursor.position() as usize;
                    self.buffer.advance(len);
                    return Ok(Some(value));
                }
                Ok(None) => {}
                Err(e) => return Err(e),
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
                    self.write_value_recursive(item).await?;
                }
            }
        }
        self.stream.flush().await?;
        Ok(())
    }

    async fn write_value_recursive(&mut self, value: RespValue) -> io::Result<()> {
        match value {
            RespValue::BulkString(Some(b)) => {
                self.stream
                    .write_all(format!("${}\r\n", b.len()).as_bytes())
                    .await?;
                self.stream.write_all(&b).await?;
                self.stream.write_all(b"\r\n").await?;
            }
            RespValue::SimpleString(ss) => {
                self.stream
                    .write_all(format!("+{}\r\n", ss).as_bytes())
                    .await?;
            }
            RespValue::Integer(int) => {
                self.stream
                    .write_all(format!(":{}\r\n", int).as_bytes())
                    .await?;
            }
            _ => {
                self.stream.write_all(b"$-1\r\n").await?;
            }
        }
        Ok(())
    }
}

fn check_and_parse(cursor: &mut Cursor<&[u8]>) -> io::Result<Option<RespValue>> {
    if !cursor.has_remaining() {
        return Ok(None);
    }

    let start_pos = cursor.position();
    let type_byte = cursor.get_u8();

    match type_byte {
        b'+' => {
            if let Some(line) = get_line(cursor) {
                return Ok(Some(RespValue::SimpleString(
                    String::from_utf8_lossy(line).to_string(),
                )));
            }
        }
        b'-' => {
            if let Some(line) = get_line(cursor) {
                return Ok(Some(RespValue::Error(
                    String::from_utf8_lossy(line).to_string(),
                )));
            }
        }
        b':' => {
            if let Some(line) = get_line(cursor) {
                let s = String::from_utf8_lossy(line);
                let i = s
                    .parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid Integer"))?;
                return Ok(Some(RespValue::Integer(i)));
            }
        }
        b'$' => {
            if let Some(line) = get_line(cursor) {
                let s = String::from_utf8_lossy(line);
                let len = s.parse::<i64>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid Bulk Length")
                })?;

                if len == -1 {
                    return Ok(Some(RespValue::BulkString(None)));
                }
                let len = len as usize;

                let remaining = cursor.remaining();
                if remaining < len + 2 {
                    cursor.set_position(start_pos);
                    return Ok(None);
                }

                let mut data = vec![0u8; len];
                cursor.copy_to_slice(&mut data);

                if cursor.get_u8() != b'\r' || cursor.get_u8() != b'\n' {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "Invalid Bulk String ending",
                    ));
                }

                return Ok(Some(RespValue::BulkString(Some(data))));
            }
        }
        b'*' => {
            if let Some(line) = get_line(cursor) {
                let s = String::from_utf8_lossy(line);
                let len = s.parse::<i64>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid Array Length")
                })?;

                if len == -1 {
                    return Ok(Some(RespValue::Array(vec![])));
                }

                let mut items = Vec::new();
                for _ in 0..len {
                    match check_and_parse(cursor)? {
                        Some(v) => items.push(v),
                        None => {
                            cursor.set_position(start_pos);
                            return Ok(None);
                        }
                    }
                }
                return Ok(Some(RespValue::Array(items)));
            }
        }
        _ => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Unknown RESP type: {}", type_byte as char),
            ))
        }
    }

    cursor.set_position(start_pos);
    Ok(None)
}

fn get_line<'a>(cursor: &mut Cursor<&'a [u8]>) -> Option<&'a [u8]> {
    let start = cursor.position() as usize;
    let end = cursor.get_ref().len();

    for i in start..end - 1 {
        if cursor.get_ref()[i] == b'\r' && cursor.get_ref()[i + 1] == b'\n' {
            cursor.set_position((i + 2) as u64);
            return Some(&cursor.get_ref()[start..i]);
        }
    }
    None
}
