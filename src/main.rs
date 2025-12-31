use codecrafters_kafka as kfk;
use kfk::{Request, Response, ResponseHeader};

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
        let req = Request::from_reader(&mut stream)?;
        let collaration_id = req.collaration_id();
        let msg = Response::new(ResponseHeader::new_v0(collaration_id), None);
        msg.send(&mut stream)?;
    }

    Ok(())
}
