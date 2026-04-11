use std::env;
use std::fs;
use std::io::{self, Read, Write};

mod compiler;

use compiler::{compile_to_frame, InputKind};

struct Args {
    kind: InputKind,
    inline: Option<String>,
    input: Option<String>,
    hex: bool,
    req_id: u32,
}

fn print_usage() {
    eprintln!(
        "Usage: pxc (--sql [SQL] | --prql [PRQL]) [--input <path>|-] [--req-id <u32>] [--hex]\n\
\n\
Options:\n\
  --sql [SQL]     Use SQL input. If SQL is omitted, read from --input or stdin.\n\
  --prql [PRQL]   Use PRQL input. If PRQL is omitted, read from --input or stdin.\n\
  --input <path>  Read input from file (or '-' for stdin).\n\
  --req-id <u32>  Request id to embed in frame (default: 0).\n\
  --hex           Output hex string instead of raw bytes.\n\
  -h, --help      Show this help message."
    );
}

fn parse_args() -> Result<Args, String> {
    let mut kind: Option<InputKind> = None;
    let mut inline: Option<String> = None;
    let mut input: Option<String> = None;
    let mut hex = false;
    let mut req_id: u32 = 0;

    let mut iter = env::args().skip(1).peekable();
    while let Some(arg) = iter.next() {
        match arg.as_str() {
            "-h" | "--help" => {
                print_usage();
                std::process::exit(0);
            }
            "--sql" => {
                kind = Some(InputKind::Sql);
                if let Some(next) = iter.peek() {
                    if !next.starts_with("--") {
                        inline = Some(iter.next().unwrap());
                    }
                }
            }
            "--prql" => {
                kind = Some(InputKind::Prql);
                if let Some(next) = iter.peek() {
                    if !next.starts_with("--") {
                        inline = Some(iter.next().unwrap());
                    }
                }
            }
            "--input" => {
                let path = iter
                    .next()
                    .ok_or_else(|| "--input requires a path".to_string())?;
                input = Some(path);
            }
            "--hex" => {
                hex = true;
            }
            "--req-id" => {
                let value = iter
                    .next()
                    .ok_or_else(|| "--req-id requires a value".to_string())?;
                req_id = value
                    .parse::<u32>()
                    .map_err(|_| "--req-id must be a u32".to_string())?;
            }
            other => return Err(format!("unexpected argument: {other}")),
        }
    }

    let kind = kind.ok_or_else(|| "must specify --sql or --prql".to_string())?;
    if inline.is_some() && input.is_some() {
        return Err("cannot use both inline input and --input".to_string());
    }

    Ok(Args {
        kind,
        inline,
        input,
        hex,
        req_id,
    })
}

fn read_source(args: &Args) -> Result<String, String> {
    if let Some(inline) = &args.inline {
        return Ok(inline.clone());
    }

    match &args.input {
        Some(path) if path == "-" => {
            let mut buffer = String::new();
            io::stdin()
                .read_to_string(&mut buffer)
                .map_err(|err| format!("failed to read stdin: {err}"))?;
            Ok(buffer)
        }
        Some(path) => fs::read_to_string(path)
            .map_err(|err| format!("failed to read input file: {err}")),
        None => {
            let mut buffer = String::new();
            io::stdin()
                .read_to_string(&mut buffer)
                .map_err(|err| format!("failed to read stdin: {err}"))?;
            Ok(buffer)
        }
    }
}

fn main() {
    let args = match parse_args() {
        Ok(args) => args,
        Err(err) => {
            eprintln!("error: {err}\n");
            print_usage();
            std::process::exit(2);
        }
    };

    let source = match read_source(&args) {
        Ok(source) => source,
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(2);
        }
    };

    let bytes = match compile_to_frame(&source, args.kind, args.req_id) {
        Ok(bytes) => bytes,
        Err(err) => {
            eprintln!("error: {err}");
            std::process::exit(2);
        }
    };

    if args.hex {
        let mut out = String::with_capacity(bytes.len() * 2);
        for byte in bytes {
            out.push_str(&format!("{:02x}", byte));
        }
        println!("{out}");
    } else {
        let mut stdout = io::stdout();
        if let Err(err) = stdout.write_all(&bytes) {
            eprintln!("error: failed to write output: {err}");
            std::process::exit(2);
        }
    }
}
