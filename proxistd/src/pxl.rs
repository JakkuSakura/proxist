use std::io::{Read, Write};

use crate::error::{Error, Result};

const MAGIC: [u8; 2] = *b"PX";
const VERSION: u8 = 1;
const HEADER_LEN: usize = 2 + 1 + 1 + 4 + 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum Op {
    Ping = 1,
    Pong = 2,
    Put = 3,
    Get = 4,
    Error = 255,
}

impl Op {
    pub fn from_u8(value: u8) -> Result<Self> {
        match value {
            1 => Ok(Op::Ping),
            2 => Ok(Op::Pong),
            3 => Ok(Op::Put),
            4 => Ok(Op::Get),
            255 => Ok(Op::Error),
            _ => Err(Error::Protocol("unknown op")),
        }
    }
}

#[derive(Debug, Clone)]
pub struct Frame {
    pub flags: u8,
    pub req_id: u32,
    pub op: Op,
    pub payload: Vec<u8>,
}

impl Frame {
    #[cfg(test)]
    pub fn encode(&self) -> Vec<u8> {
        let frame_len = HEADER_LEN + self.payload.len();
        let mut buf = Vec::with_capacity(4 + frame_len);
        buf.extend_from_slice(&(frame_len as u32).to_le_bytes());
        buf.extend_from_slice(&MAGIC);
        buf.push(VERSION);
        buf.push(self.flags);
        buf.extend_from_slice(&self.req_id.to_le_bytes());
        buf.push(self.op as u8);
        buf.extend_from_slice(&self.payload);
        buf
    }
}

pub fn read_frame<R: Read>(reader: &mut R) -> Result<Option<Frame>> {
    let mut len_buf = [0u8; 4];
    match reader.read_exact(&mut len_buf) {
        Ok(()) => {}
        Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(err) => return Err(err.into()),
    }
    let frame_len = u32::from_le_bytes(len_buf) as usize;
    if frame_len < HEADER_LEN {
        return Err(Error::Protocol("frame too short"));
    }
    let mut header = [0u8; HEADER_LEN];
    reader.read_exact(&mut header)?;
    if header[0..2] != MAGIC {
        return Err(Error::Protocol("bad magic"));
    }
    if header[2] != VERSION {
        return Err(Error::Protocol("unsupported version"));
    }
    let flags = header[3];
    let req_id = u32::from_le_bytes([header[4], header[5], header[6], header[7]]);
    let op = Op::from_u8(header[8])?;
    let payload_len = frame_len - HEADER_LEN;
    let mut payload = vec![0u8; payload_len];
    reader.read_exact(&mut payload)?;
    Ok(Some(Frame {
        flags,
        req_id,
        op,
        payload,
    }))
}

pub fn write_frame<W: Write>(writer: &mut W, frame: &Frame) -> Result<()> {
    let frame_len = HEADER_LEN + frame.payload.len();
    let mut header = [0u8; HEADER_LEN];
    header[0..2].copy_from_slice(&MAGIC);
    header[2] = VERSION;
    header[3] = frame.flags;
    header[4..8].copy_from_slice(&frame.req_id.to_le_bytes());
    header[8] = frame.op as u8;
    writer.write_all(&(frame_len as u32).to_le_bytes())?;
    writer.write_all(&header)?;
    writer.write_all(&frame.payload)?;
    Ok(())
}

pub fn encode_put_payload(table: &str, symbol: &str, ts: u64, value: &[u8]) -> Result<Vec<u8>> {
    let table_bytes = table.as_bytes();
    let symbol_bytes = symbol.as_bytes();
    if table_bytes.len() > u16::MAX as usize || symbol_bytes.len() > u16::MAX as usize {
        return Err(Error::InvalidData("table/symbol too long".to_string()));
    }
    let mut buf = Vec::with_capacity(
        2 + table_bytes.len() + 2 + symbol_bytes.len() + 8 + 4 + value.len(),
    );
    buf.extend_from_slice(&(table_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(table_bytes);
    buf.extend_from_slice(&(symbol_bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(symbol_bytes);
    buf.extend_from_slice(&ts.to_le_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
    Ok(buf)
}

pub fn decode_put_payload(payload: &[u8]) -> Result<(String, String, u64, Vec<u8>)> {
    let mut offset = 0;
    let table_len = read_u16(payload, &mut offset)? as usize;
    let table = read_bytes(payload, &mut offset, table_len)?;
    let symbol_len = read_u16(payload, &mut offset)? as usize;
    let symbol = read_bytes(payload, &mut offset, symbol_len)?;
    let ts = read_u64(payload, &mut offset)?;
    let value_len = read_u32(payload, &mut offset)? as usize;
    let value = read_vec(payload, &mut offset, value_len)?;
    Ok((
        String::from_utf8(table).map_err(|_| Error::InvalidData("table utf8".to_string()))?,
        String::from_utf8(symbol).map_err(|_| Error::InvalidData("symbol utf8".to_string()))?,
        ts,
        value,
    ))
}

pub fn decode_get_payload(payload: &[u8]) -> Result<(String, String)> {
    let mut offset = 0;
    let table_len = read_u16(payload, &mut offset)? as usize;
    let table = read_bytes(payload, &mut offset, table_len)?;
    let symbol_len = read_u16(payload, &mut offset)? as usize;
    let symbol = read_bytes(payload, &mut offset, symbol_len)?;
    Ok((
        String::from_utf8(table).map_err(|_| Error::InvalidData("table utf8".to_string()))?,
        String::from_utf8(symbol).map_err(|_| Error::InvalidData("symbol utf8".to_string()))?,
    ))
}

pub fn encode_get_response(ts: u64, value: &[u8]) -> Result<Vec<u8>> {
    if value.len() > u32::MAX as usize {
        return Err(Error::InvalidData("value too long".to_string()));
    }
    let mut buf = Vec::with_capacity(8 + 4 + value.len());
    buf.extend_from_slice(&ts.to_le_bytes());
    buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
    buf.extend_from_slice(value);
    Ok(buf)
}

pub fn encode_error_payload(msg: &str) -> Result<Vec<u8>> {
    let bytes = msg.as_bytes();
    if bytes.len() > u16::MAX as usize {
        return Err(Error::InvalidData("error message too long".to_string()));
    }
    let mut buf = Vec::with_capacity(2 + bytes.len());
    buf.extend_from_slice(&(bytes.len() as u16).to_le_bytes());
    buf.extend_from_slice(bytes);
    Ok(buf)
}

fn read_u16(payload: &[u8], offset: &mut usize) -> Result<u16> {
    if *offset + 2 > payload.len() {
        return Err(Error::Protocol("payload too short"));
    }
    let val = u16::from_le_bytes([payload[*offset], payload[*offset + 1]]);
    *offset += 2;
    Ok(val)
}

fn read_u32(payload: &[u8], offset: &mut usize) -> Result<u32> {
    if *offset + 4 > payload.len() {
        return Err(Error::Protocol("payload too short"));
    }
    let val = u32::from_le_bytes([
        payload[*offset],
        payload[*offset + 1],
        payload[*offset + 2],
        payload[*offset + 3],
    ]);
    *offset += 4;
    Ok(val)
}

fn read_u64(payload: &[u8], offset: &mut usize) -> Result<u64> {
    if *offset + 8 > payload.len() {
        return Err(Error::Protocol("payload too short"));
    }
    let val = u64::from_le_bytes([
        payload[*offset],
        payload[*offset + 1],
        payload[*offset + 2],
        payload[*offset + 3],
        payload[*offset + 4],
        payload[*offset + 5],
        payload[*offset + 6],
        payload[*offset + 7],
    ]);
    *offset += 8;
    Ok(val)
}

fn read_bytes(payload: &[u8], offset: &mut usize, len: usize) -> Result<Vec<u8>> {
    if *offset + len > payload.len() {
        return Err(Error::Protocol("payload too short"));
    }
    let slice = &payload[*offset..*offset + len];
    *offset += len;
    Ok(slice.to_vec())
}

fn read_vec(payload: &[u8], offset: &mut usize, len: usize) -> Result<Vec<u8>> {
    read_bytes(payload, offset, len)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_decode_frame_roundtrip() {
        let frame = Frame {
            flags: 0,
            req_id: 7,
            op: Op::Ping,
            payload: vec![1, 2, 3],
        };
        let buf = frame.encode();
        let mut cursor = std::io::Cursor::new(buf);
        let decoded = read_frame(&mut cursor).unwrap().unwrap();
        assert_eq!(decoded.flags, 0);
        assert_eq!(decoded.req_id, 7);
        assert_eq!(decoded.op, Op::Ping);
        assert_eq!(decoded.payload, vec![1, 2, 3]);
    }

    #[test]
    fn put_payload_roundtrip() {
        let payload = encode_put_payload("ticks", "AAPL", 42, b"v").unwrap();
        let (table, symbol, ts, value) = decode_put_payload(&payload).unwrap();
        assert_eq!(table, "ticks");
        assert_eq!(symbol, "AAPL");
        assert_eq!(ts, 42);
        assert_eq!(value, b"v");
    }
}
