use codecrafters_kafka as kfk;
use kfk::{Header, Message};

use bytes::Bytes;
use std::io::Write;
use std::net::TcpListener;

fn main() {
    if let Err(e) = run() {
        eprintln!("{e}");
        std::process::exit(1);
    }
}

fn run() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("127.0.0.1:9092")?;

    for stream in listener.incoming() {
        let mut stream = stream?;
        let msg = Message::new(Header::new_v0(7));
        let bytes: Bytes = msg.into();
        stream.write_all(&bytes)?;
    }

    Ok(())
}
