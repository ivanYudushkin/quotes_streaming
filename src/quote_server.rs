use std::io::BufRead;
use std::io::BufReader;
use std::io::Write;
use std::net::TcpStream;
use std::time::Duration;
use std::net::{SocketAddr};
use crate::broadcaster;

pub enum ClientEvent {
    Disconnected,
    Exit
}

pub fn handle_client(stream: TcpStream) -> std::io::Result<ClientEvent> {
    // клонируем stream: один экземпляр для чтения (обёрнут в BufReader), другой — для записи
    let mut writer = match stream.try_clone() {
        Ok(s) => s,
        Err(e) => {
            eprintln!("Failed to clone stream: {e}");
            return Err(e);
        }
    };
    
    let mut reader = BufReader::new(stream);

    // send initial prompt
    let _ = writer.write_all(b"Welcome to the Ticker Sender!\n");
    let _ = writer.flush();

    let mut line = String::new();

    loop {
        line.clear();
        // read_line ждёт '\n' — nc отправляет строку по нажатию Enter
        match reader.read_line(&mut line) {
            Ok(0) => {
                // EOF — клиент закрыл соединение
                return Ok(ClientEvent::Disconnected);
            }
            Ok(_) => {
                let input = line.trim();
                if input.is_empty() {
                    let _ = writer.flush();
                    continue;
                }

                let mut parts = input.split_whitespace();
                let response = match parts.next() {
                    Some("STREAM") => {
                        let tickers = parts.next();

                        if let Some(tickers) = tickers {

                            let tickers = tickers.to_string();
                            let mut out = writer.try_clone()?;

                            std::thread::spawn(move || {
                                let _ = broadcaster::send_tickers_to_stream(&tickers, &mut out);
                            });
                           

                            "OK: stream started\r\n".to_string()

                        } else {
                            "ERROR: usage STREAM <tickers_list>\n".to_string()
                        }
                    }
                    
                    Some("UDP_STREAM") => {

                        let target_addr_str = match parts.next() {
                            Some(s) => s,
                            None => return Ok(ClientEvent::Disconnected)
                        };
                        
                        let target_addr: SocketAddr = match target_addr_str.parse() {
                            Ok(addr) => addr,
                            Err(e) => {
                                eprintln!("Failed to parse address {target_addr_str} to SocketAddr: {e}");
                                continue;
                            }
                        };

                        let tickers = parts.next();

                        if let Some(tickers) = tickers {

                            let tickers = tickers.to_string();
                            
                            std::thread::spawn(move || {
                                let _ = broadcaster::send_tickers_to_udp(&tickers, target_addr);
                            });

                            format!("OK: UDP stream started\r\nSocket address: {target_addr}\r\n").to_string()

                        } else {
                            "ERROR: usage UDP_STREAM <udp_addr> <tickers_list>\n".to_string()
                        }
                    }

                    Some("PING") => {
                        // Случайная задержка от 1 до 5 секунд
                        let delay_secs = (rand::random::<u64>() % 5) + 1;
                        std::thread::sleep(Duration::from_secs(delay_secs));
                        "PONG\n".to_string()
                    }

                    Some("EXIT") => {
                        let _ = writer.write_all(b"BYE\n");
                        let _ = writer.flush();
                        return Ok(ClientEvent::Exit);
                    }

                    _ => "ERROR: unknown command\n".to_string(),
                };

                // отправляем ответ и снова показываем prompt
                let _ = writer.write_all(response.as_bytes());
                let _ = writer.flush();
            }
            Err(_) => {
                // ошибка чтения — закрываем
                return Ok(ClientEvent::Disconnected);
            }
        }
    }
}