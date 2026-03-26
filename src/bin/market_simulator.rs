use quotes_streaming::stock::StockQuote;
use std::sync::{Arc, RwLock};
use std::time::{SystemTime, UNIX_EPOCH};
use std::thread;
use std::time::Duration;
use rand::Rng;

use std::io::BufRead;
use std::fs::File;
use std::io::BufReader;
use std::net::UdpSocket;


fn set_random_quote(quote: &Arc<RwLock<StockQuote>>) {
    let mut rng = rand::thread_rng();
    let timestamp_ms = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;

    let mut q = quote.write().unwrap();
    q.price *= rng.gen_range(0.99..=1.01);
    q.volume += 1;
    q.timestamp = timestamp_ms;
}

fn main() -> std::io::Result<()>{

    let f = File::open("tickers.txt").unwrap();
    let reader = BufReader::new(f);

    //Для доступа к вектору из разных потоков
    let mut quotes_vec: Vec<Arc<RwLock<StockQuote>>> = Vec::new();

    //Прохожу по файлу, добавляю тикеры в ыектор
    for line in reader.lines() {
        let line = line.unwrap();
        let new_quote = line.trim();
        let stock_quote = StockQuote::new_random(new_quote);

        quotes_vec.push(Arc::new(RwLock::new(stock_quote)));
    }

    //Создаю UDP сокет для отправки данных о тикерах
    let socket = match UdpSocket::bind("127.0.0.1:8080") {
        Ok(bind) => bind,
        Err(e)  => {
            eprintln!("bind failed: {e}");
            return Err(e);
        }
    };
    
    let socket = Arc::new(socket);

    println!("market sender on socket: {}", socket.local_addr().unwrap());
    
    //Вектор потоков по бумагам
    let mut handles = Vec::new();

    //По каждому тикеру генерирую рандомную активность(рандомное число тредеров на бумагу)
    for quote in &quotes_vec {
        let mut rng = rand::thread_rng();
        let traders = rng.gen_range(1..=5);

        for _ in 0..traders {

            let quote = Arc::clone(quote);
            let socket = Arc::clone(&socket);

            let handle = std::thread::spawn(move || {

                loop {

                    let mut rng = rand::thread_rng();
                    //Случайная частота торгов
                    thread::sleep(Duration::from_millis(rng.gen_range(100..=1000)));

                    set_random_quote(&quote);

                    if let Ok(qu) = quote.try_read() {
                        let msg = qu.to_string();
                        socket.send_to(msg.as_bytes(), "127.0.0.1:8081").unwrap();
                    }
                    // {
                    //     let qu = quote.read().unwrap();
                    //     let msg = format!("{}", qu.to_string());
                    //     // Отправляю сообщение по бумаге в UDP
                    //     socket.send_to(msg.as_bytes(), "127.0.0.1:8081").unwrap();
                    // }

                }
            });

            handles.push(handle);

        }
    }

    //Все потоки выполняются параллельно 
    for handle in handles {
        handle.join().unwrap(); // это будет ждать вечно, так как потоки бесконечные
    }

    Ok(())

}
