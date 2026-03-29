use std::io::Write;
use std::net::{TcpStream, UdpSocket};
use std::thread;
use std::net::Shutdown;

fn main() -> std::io::Result<()> {

    let socket = UdpSocket::bind("127.0.0.1:0")?;
    let tickers = std::env::args().nth(1)
        .expect("usage: cargo run --bin udp_stream -- <tickers>");



    let socket_addr = socket.local_addr()?;

    let tcp = TcpStream::connect("127.0.0.1:7878")?;
    let stream_tcp = tcp.try_clone()?;

    let streaming_socket = socket.try_clone()?;

    let _process = thread::spawn(move || -> std::io::Result<()> {
        let cmd = format!("UDP_STREAM {socket_addr} {tickers}\n");

        let mut tcp = stream_tcp.try_clone()?;

        if let Err(e) = tcp.write_all(cmd.as_bytes()) {
            eprintln!("TCP write failed: {e}");
            return Err(e);
        };

        tcp.flush().unwrap();

        let _ = tcp.shutdown(Shutdown::Both);
    
        let mut buf = [0u8; 2048];
        
        loop {
    
            let (len, _) = match streaming_socket.recv_from(&mut buf) {
                Ok(len) => len,
                Err(e) => return Err(e),
            };
            //Парсинг данных с сокета в строку
            let _ = match std::str::from_utf8(&buf[..len]) {
                Ok(s) => println!("{s}"),
                Err(_) => continue,
            };
        }
    });

    thread::spawn(move || {
        loop {
            socket.send_to("PING\n".as_bytes(), "127.0.0.1:9000").unwrap();
        }
    });

    if let Err(e) =_process.join() {
        eprintln!("Failed to start streaming: {:?}", e);
    };

    Ok(())

}