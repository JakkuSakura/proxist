use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{Error, Result};
use crate::memstore::MemStore;
use crate::pxl::{
    decode_get_payload, decode_put_payload, encode_error_payload, encode_get_response, Frame, Op,
};
use crate::wal::Wal;

#[derive(Clone)]
pub struct Server {
    addr: String,
    mem: Arc<RwLock<MemStore>>,
    wal: Option<Arc<Mutex<Wal>>>,
}

impl Server {
    pub fn bind(addr: String, mem: Arc<RwLock<MemStore>>, wal: Option<Arc<Mutex<Wal>>>) -> Self {
        Self { addr, mem, wal }
    }

    pub fn serve(&self) -> Result<()> {
        let listener = TcpListener::bind(&self.addr)?;
        for stream in listener.incoming() {
            let stream = stream?;
            let mem = Arc::clone(&self.mem);
            let wal = self.wal.as_ref().map(Arc::clone);
            std::thread::spawn(move || {
                if let Err(err) = handle_connection(stream, mem, wal) {
                    eprintln!("conn error: {err}");
                }
            });
        }
        Ok(())
    }
}

fn handle_connection(
    mut stream: TcpStream,
    mem: Arc<RwLock<MemStore>>,
    wal: Option<Arc<Mutex<Wal>>>,
) -> Result<()> {
    stream.set_nodelay(true)?;
    loop {
        let frame = match crate::pxl::read_frame(&mut stream)? {
            Some(frame) => frame,
            None => return Ok(()),
        };

        let req_id = frame.req_id;
        let response = match handle_frame(frame, &mem, wal.as_ref()) {
            Ok(frame) => frame,
            Err(err) => Frame {
                flags: 0,
                req_id,
                op: Op::Error,
                payload: encode_error_payload(&err.to_string())?,
            },
        };

        crate::pxl::write_frame(&mut stream, &response)?;
    }
}

fn handle_frame(
    frame: Frame,
    mem: &Arc<RwLock<MemStore>>,
    wal: Option<&Arc<Mutex<Wal>>>,
) -> Result<Frame> {
    match frame.op {
        Op::Ping => Ok(Frame {
            flags: 0,
            req_id: frame.req_id,
            op: Op::Pong,
            payload: Vec::new(),
        }),
        Op::Put => {
            let (table, symbol, ts, value) = decode_put_payload(&frame.payload)?;
            if let Some(wal) = wal {
                let mut wal = wal.lock().map_err(|_| Error::Protocol("wal lock"))?;
                wal.append_put(&table, &symbol, ts, &value)?;
            }
            {
                let mut mem = mem.write().map_err(|_| Error::Protocol("mem lock"))?;
                mem.put(&table, &symbol, ts, value);
            }
            Ok(Frame {
                flags: 0,
                req_id: frame.req_id,
                op: Op::Pong,
                payload: Vec::new(),
            })
        }
        Op::Get => {
            let (table, symbol) = decode_get_payload(&frame.payload)?;
            let row = {
                let mem = mem.read().map_err(|_| Error::Protocol("mem lock"))?;
                mem.get_last(&table, &symbol).cloned()
            };
            match row {
                Some(row) => Ok(Frame {
                    flags: 0,
                    req_id: frame.req_id,
                    op: Op::Get,
                    payload: encode_get_response(row.ts, &row.value)?,
                }),
                None => Ok(Frame {
                    flags: 0,
                    req_id: frame.req_id,
                    op: Op::Error,
                    payload: encode_error_payload("not found")?,
                }),
            }
        }
        Op::Pong | Op::Error => Err(Error::Protocol("unexpected op")),
    }
}
