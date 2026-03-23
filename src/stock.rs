use rand::Rng;

#[derive(Debug, Clone)]
pub struct StockQuote {
    pub ticker: String,
    pub price: f64,
    pub volume: u32,
    pub timestamp: u64,
}

impl StockQuote {
    
    pub fn new_random(name: &str) -> Self {
        let mut rng = rand::thread_rng();

        StockQuote { 
            ticker: name.to_string(),
            price: rng.gen_range(10.0..=10000.0),
            volume: 0, 
            timestamp: 0
        }
    }

    pub fn to_string(&self) -> String {
        format!("{}|{}|{}|{}", self.ticker, self.price, self.volume, self.timestamp)
    }
}

