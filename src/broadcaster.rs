use std::net::UdpSocket;
use std::sync::mpsc::{self, Receiver, Sender};
use std::sync::{Mutex, Once, OnceLock};
use std::io::Write;
use std::thread;

//Инициализируем статик переменные. Нам нужен общий список подписчиков на рассылки, который должен сгенерироваться один раз и жить весь цикл выполнения программы
static START_MARKET_LOOP: Once = Once::new();
//Для каждого клиента свой канал. Изменение из разных потоков, поэтому мьютекс
static SUBSCRIBERS: OnceLock<Mutex<Vec<Sender<String>>>> = OnceLock::new();

//Получение списка подписчиков либо инициализация(если их нет)
fn subscribers() -> &'static Mutex<Vec<Sender<String>>> {
    SUBSCRIBERS.get_or_init(|| Mutex::new(Vec::new()))
}

//Функция, которая запускается только один раз, потому что работает на всех пользователей и не нужно ее отдельно запускать для каждого. Читает с сокета данные о котировках, которые генерируются в market_sender
fn start_market_loop_once() {
    START_MARKET_LOOP.call_once(|| {
        thread::spawn(|| {
            //Подключение к сокету
            let market_socket = match UdpSocket::bind("127.0.0.1:8081") {
                Ok(socket) => socket,
                Err(e) => {
                    eprintln!("Failed to bind socket: {e}");
                    return;
                }
            };

            let mut buf = [0u8; 2048];

            loop {
                //Чтение данных с сокета
                let (len, _) = match market_socket.recv_from(&mut buf) {
                    Ok(len) => len,
                    Err(_) => continue,
                };
                //Парсинг данных с сокета в строку
                let msg = match std::str::from_utf8(&buf[..len]) {
                    Ok(s) => s.trim().to_string(),
                    Err(_) => continue,
                };
                //Проходжимся по списку подписчиков и пытаемся отправить им данные в канал, которые получили с сокета, Если при отправке возникает ошибка, то считаем подписчика недействительным и удаляем его
                let mut subs = subscribers().lock().unwrap();
                subs.retain(|tx| tx.send(msg.clone()).is_ok());

            }
        });
    });
}

//Создание отдельного канала для пользователя. Добавление пользователя в глобальный список подписчиков. Возвращает Receiver для дальнейшей фильтрации тикеров
fn subscribe_user() -> Receiver<String> {
    let (tx,rx) = mpsc::channel::<String>();
    subscribers().lock().unwrap().push(tx);
    rx
}

//Функция выводит в косоль TCP подключения инфо только о запрошенных тикерах
pub fn send_tickers_to_stream(
    tickers: &str,
    out: &mut impl Write,
) -> std::io::Result<()> {

    //Запускает если еще не запущен
    start_market_loop_once();

    //Добавляет подписчика, получает Receiver для него
    let rx = subscribe_user();
    let target_tickers: Vec<&str> = tickers.split(',').map(|s| s.trim()).collect();

    //Обрабатывает сообщение из канала, парсит только нужные тикеры и выводит инфо в консоль
    for msg in rx {
        let ticker = msg.split('|').next().unwrap_or("");
        if target_tickers.contains(&ticker) {
            out.write_all(msg.as_bytes())?;
            out.write_all(b"\r\n")?;
            out.flush()?;
        }
    };

    Ok(())

}




































// pub fn send_tickers_to_stream(
//     tickers: &str,
//     out: &mut impl Write,
// ) -> std::io::Result<()> {
//     let target_tickers: Vec<&str> = tickers.split(',').map(|s| s.trim()).collect();
//     let market_socket = UdpSocket::bind("127.0.0.1:8081")?;

//     let (tx, rx) = mpsc::channel::<String>();

//     thread::spawn(move || {
//         let mut buf = [0u8; 2048];
//         loop {

//             let (len, _) = match market_socket.recv_from(&mut buf) {
//                 Ok(len) => len,
//                 Err(_) => continue
//             };

//             let msg = match std::str::from_utf8(&buf[..len]) {
//                 Ok(s) => s.trim().to_string(),
//                 Err(_) => continue,
//             };

//             if tx.send(msg).is_err() {
//                 continue;
//             }
//         }
//     });    

//     for msg in rx {
//         let ticker = msg.split('|').next().unwrap_or("");
//         if target_tickers.contains(&ticker) {
//             out.write_all(msg.as_bytes())?;
//             out.write_all(b"\r\n")?;
//             out.flush()?;
//         }
//     }

//     Ok(())
// }

