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
            // 1. Pokušaj parsirati koristeći kursor (ne uništava buffer)
            let mut cursor = Cursor::new(&self.buffer[..]);

            match check_and_parse(&mut cursor) {
                Ok(Some(value)) => {
                    // 2. Uspjeh! Tek SADA mičemo podatke iz pravog buffera
                    let len = cursor.position() as usize;
                    self.buffer.advance(len);
                    return Ok(Some(value));
                }
                Ok(None) => {
                    // 3. Nema dovoljno podataka. Ne diraj buffer, čekaj još s mreže.
                }
                Err(e) => return Err(e),
            }

            // 4. Čitaj još podataka s mreže
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
        // ... (Ovaj dio ostaje isti kao prije, kopiraj ga iz stare verzije ili vidi dolje)
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
                    // Jednostavna rekurzija za ispis (za potrebe demoa)
                    // Pazi: ovo nije najefikasnije za duboke arraye, ali radi za Redis klijente
                    self.write_value_recursive(item).await?;
                }
            }
        }
        self.stream.flush().await?;
        Ok(())
    }

    // Pomoćna funkcija za rekurziju kod pisanja (da izbjegnemo ownership probleme s &mut self)
    // U pravoj produkciji bi ovo bilo dio write_value logike bez rekurzije async-a (BoxFuture)
    // Ali za ovaj MVP, kopirat ćemo logiku nakratko.
    // Zapravo, najlakše je da write_value samo zove ovo za array iteme:
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
                // Fallback za ostale tipove unutar arraya (pojednostavljeno)
                self.stream.write_all(b"$-1\r\n").await?;
            }
        }
        Ok(())
    }
}

// --- Čista funkcija za parsiranje koja koristi Kursor ---
fn check_and_parse(cursor: &mut Cursor<&[u8]>) -> io::Result<Option<RespValue>> {
    if !cursor.has_remaining() {
        return Ok(None);
    }

    // Pročitaj prvi bajt bez pomicanja pozicije trajno (Cursor to radi za nas)
    let start_pos = cursor.position();
    let type_byte = cursor.get_u8();

    match type_byte {
        b'+' => {
            // Simple String
            if let Some(line) = get_line(cursor) {
                return Ok(Some(RespValue::SimpleString(
                    String::from_utf8_lossy(line).to_string(),
                )));
            }
        }
        b'-' => {
            // Error
            if let Some(line) = get_line(cursor) {
                return Ok(Some(RespValue::Error(
                    String::from_utf8_lossy(line).to_string(),
                )));
            }
        }
        b':' => {
            // Integer
            if let Some(line) = get_line(cursor) {
                let s = String::from_utf8_lossy(line);
                let i = s
                    .parse::<i64>()
                    .map_err(|_| io::Error::new(io::ErrorKind::InvalidData, "Invalid Integer"))?;
                return Ok(Some(RespValue::Integer(i)));
            }
        }
        b'$' => {
            // Bulk String
            if let Some(line) = get_line(cursor) {
                let s = String::from_utf8_lossy(line);
                let len = s.parse::<i64>().map_err(|_| {
                    io::Error::new(io::ErrorKind::InvalidData, "Invalid Bulk Length")
                })?;

                if len == -1 {
                    return Ok(Some(RespValue::BulkString(None)));
                }
                let len = len as usize;

                // Provjeri imamo li dovoljno bajtova + CRLF
                let remaining = cursor.remaining();
                if remaining < len + 2 {
                    // NEMA DOVOLJNO PODATAKA - VRAĆAMO SE NA POČETAK
                    cursor.set_position(start_pos);
                    return Ok(None);
                }

                let mut data = vec![0u8; len];
                cursor.copy_to_slice(&mut data);

                // Preskoči CRLF
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
            // Array
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
                    // REKURZIVNO POZIVAMO PARSER
                    // Ako bilo koji element fali, cijeli Array se poništava
                    match check_and_parse(cursor)? {
                        Some(v) => items.push(v),
                        None => {
                            // Fali element - vraćamo sve na početak!
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

    // Ako smo došli ovdje, znači da get_line nije našao CRLF
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
