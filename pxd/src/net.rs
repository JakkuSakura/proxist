use std::net::{TcpListener, TcpStream};
use std::sync::{Arc, Mutex, RwLock};

use crate::error::{Error, Result};
use crate::memstore::MemStore;
use crate::pxl::{
    decode_delete_payload, decode_insert_payload, decode_query_payload, decode_schema_payload,
    decode_update_payload, encode_error_payload, encode_result_payload, Frame, Op,
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
        Op::Create => {
            let (table, schema) = decode_schema_payload(&frame.payload)?;
            if let Some(wal) = wal {
                let mut wal = wal.lock().map_err(|_| Error::Protocol("wal lock"))?;
                wal.append_create(&table, &schema)?;
            }
            {
                let mut mem = mem.write().map_err(|_| Error::Protocol("mem lock"))?;
                mem.create_table(&table, schema)?;
            }
            Ok(Frame {
                flags: 0,
                req_id: frame.req_id,
                op: Op::Pong,
                payload: Vec::new(),
            })
        }
        Op::Insert => {
            let (table, columns, rows) = decode_insert_payload(&frame.payload)?;
            if let Some(wal) = wal {
                let mut wal = wal.lock().map_err(|_| Error::Protocol("wal lock"))?;
                wal.append_insert(&table, &columns, &rows)?;
            }
            {
                let mut mem = mem.write().map_err(|_| Error::Protocol("mem lock"))?;
                mem.insert(&table, &columns, &rows)?;
            }
            Ok(Frame {
                flags: 0,
                req_id: frame.req_id,
                op: Op::Pong,
                payload: Vec::new(),
            })
        }
        Op::Update => {
            let (table, assignments, filter) = decode_update_payload(&frame.payload)?;
            if let Some(wal) = wal {
                let mut wal = wal.lock().map_err(|_| Error::Protocol("wal lock"))?;
                wal.append_update(&table, &assignments, filter.as_ref())?;
            }
            {
                let mut mem = mem.write().map_err(|_| Error::Protocol("mem lock"))?;
                mem.update(&table, &assignments, filter.as_ref())?;
            }
            Ok(Frame {
                flags: 0,
                req_id: frame.req_id,
                op: Op::Pong,
                payload: Vec::new(),
            })
        }
        Op::Delete => {
            let (table, filter) = decode_delete_payload(&frame.payload)?;
            if let Some(wal) = wal {
                let mut wal = wal.lock().map_err(|_| Error::Protocol("wal lock"))?;
                wal.append_delete(&table, filter.as_ref())?;
            }
            {
                let mut mem = mem.write().map_err(|_| Error::Protocol("mem lock"))?;
                mem.delete(&table, filter.as_ref())?;
            }
            Ok(Frame {
                flags: 0,
                req_id: frame.req_id,
                op: Op::Pong,
                payload: Vec::new(),
            })
        }
        Op::Query => {
            let plan = decode_query_payload(&frame.payload)?;
            let result = {
                let mem = mem.read().map_err(|_| Error::Protocol("mem lock"))?;
                mem.query(&plan)?
            };
            Ok(Frame {
                flags: 0,
                req_id: frame.req_id,
                op: Op::Result,
                payload: encode_result_payload(&result.schema, &result.rows)?,
            })
        }
        Op::Pong | Op::Error | Op::Result | Op::Schema => {
            Err(Error::Protocol("unexpected op"))
        }
    }
}
