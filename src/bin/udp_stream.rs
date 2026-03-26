use std::io::{Write,BufRead, BufReader, ErrorKind};
use std::net::{TcpStream, UdpSocket};
use std::thread;
use std::time::Duration;

fn main() -> std::io::Result<()> {

    let socket = UdpSocket::bind("127.0.0.1:0")?;
    let tickers = std::env::args().nth(1)
        .expect("usage: cargo run --bin udp_stream -- <tickers>");



    let socket_addr = socket.local_addr()?;

    let tcp = TcpStream::connect("127.0.0.1:7878")?;
    let stream_tcp = tcp.try_clone()?;

    let _process = thread::spawn(move || -> std::io::Result<()> {
        let cmd = format!("UDP_STREAM {socket_addr} {tickers}\n");

        let mut tcp = stream_tcp.try_clone()?;

        if let Err(e) = tcp.write_all(cmd.as_bytes()) {
            eprintln!("TCP write failed: {e}");
            return Err(e);
        };

        tcp.flush().unwrap();
    
        let mut buf = [0u8; 2048];
        
        loop {
    
            let (len, _) = match socket.recv_from(&mut buf) {
                Ok(len) => len,
                Err(_) => continue,
            };
            //Парсинг данных с сокета в строку
            let _ = match std::str::from_utf8(&buf[..len]) {
                Ok(s) => println!("{s}"),
                Err(_) => continue,
            };
        }
    });

    let mut hb_tcp = tcp.try_clone()?;

    let hb = thread::spawn(move || {
        let mut reader = BufReader::new(hb_tcp.try_clone().expect("clone failed"));
        reader
            .get_ref()
            .set_read_timeout(Some(Duration::from_secs(5)))
            .ok();

        loop {
            let hb_cmd = format!("PING\n");
            if hb_tcp.write_all(hb_cmd.as_bytes()).is_err() {
                eprintln!("Соединение разорвано");
                break;
            };
            hb_tcp.flush().unwrap();

            let mut line = String::new();
            match reader.read_line(&mut line) {
                Ok(0) => {
                    break;
                }
                Ok(_) if line.trim() == "PONG" => {}
                Ok(_) => {}
                Err(e) if e.kind() == ErrorKind::TimedOut || e.kind() == ErrorKind::WouldBlock => {
                    eprintln!("no PONG within 5s");
                    break;
                }
                Err(_) => {
                    break;
                }
            }

            thread::sleep(Duration::from_secs(2));
        }
    });


    hb.join().unwrap();

    Ok(())

}