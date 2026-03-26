use quotes_streaming::quote_server::{handle_client, ClientEvent};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::net::TcpListener;

fn main() -> std::io::Result<()> {

    //Адрес для подключения пользователей
    let listener = match TcpListener::bind("127.0.0.1:7878") {
        Ok(bind) => bind,
        Err(e) => {
            eprintln!("Failed to bind TCP Listener: {e}");
            return Err(e);
        }
    };
    println!("Server listening on port 7878");

    //Счетчик активных
    let active_connections = Arc::new(AtomicUsize::new(0));


    //Счетчик секунд без подключений
    let idle_secs = Arc::new(AtomicUsize::new(0));
    //Максимальное число секунд, которое reciever работает без подключений
    // let idle_timeout_secs: usize = 60;


    //Клонирование для основного потока
    let active_connections_in_process = active_connections.clone();
    let idle_secs_in_process = idle_secs.clone();

    //Основной поток
    let process = thread::spawn(move || {
        for stream in listener.incoming() {
            match stream {
                Ok(stream) => {
                    
                    let active_connections_for_user = active_connections_in_process.clone();
                    let no_conn_since_for_user = idle_secs_in_process.clone();
                    //Увеличиваем счетчик активных подключений
                    active_connections_for_user.fetch_add(1, Ordering::SeqCst);
                    //Новое подключение
                    thread::spawn(move || {
                        match handle_client(stream) {
                            Ok(ClientEvent::Disconnected) => println!("Client disconnected"),
                            Ok(ClientEvent::Exit) => println!("Client sent EXIT"),
                            Err(e) => eprintln!("Client handler error: {}", e),
                        }
                    //Уменьшаем счетчик активных соединений
                        active_connections_for_user.fetch_sub(1, Ordering::SeqCst);
                    //Обнуляем счетчик
                        no_conn_since_for_user.store(0, Ordering::SeqCst);
                    });

                        
    
                }
                Err(e) => eprintln!("Connection failed: {}", e),
            }
        }
    });

    if let Err(e) = process.join() {
        eprintln!("process thread panicked: {:?}", e);
    }
    //Клонирование для потока проверки таймаута
    // let active_connections_check = active_connections.clone();
    // let idle_secs_w = idle_secs.clone();


    // //Поток который проверяет сколько секунд прошло с момента отключения последнего пользователя
    // //Если таймаут - выходим из потока
    // let timeout_check = thread::spawn(move || {
    //     loop {
    //         let active = active_connections_check.load(Ordering::SeqCst);
    
    //         if active == 0 {
    //             //Добавляем одну секунду к счетчику
    //             let cur = idle_secs_w.fetch_add(1, Ordering::SeqCst) + 1;
    //             //Проверяем, что не таймаут
    //             if cur >= idle_timeout_secs {
    //                 //Выход из цикла
    //                 break;
    //             }
    //         } else {
    //             idle_secs_w.store(0, Ordering::SeqCst);
    //         }
    //         //Ждем секунду и заново
    //         thread::sleep(Duration::from_secs(1));
    //     }
    // });

    // //Если поток завершился reciever завершает работу
    // timeout_check.join().unwrap();

    Ok(())

}