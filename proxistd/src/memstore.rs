use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Row {
    pub ts: u64,
    pub value: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct MemStore {
    data: HashMap<String, HashMap<String, Vec<Row>>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn put(&mut self, table: &str, symbol: &str, ts: u64, value: Vec<u8>) {
        let symbols = match self.data.get_mut(table) {
            Some(symbols) => symbols,
            None => {
                self.data.insert(table.to_string(), HashMap::new());
                self.data.get_mut(table).expect("inserted map")
            }
        };
        let entry = symbols.entry(symbol.to_string()).or_insert_with(Vec::new);
        entry.push(Row { ts, value });
    }

    pub fn get_last(&self, table: &str, symbol: &str) -> Option<&Row> {
        self.data
            .get(table)
            .and_then(|symbols| symbols.get(symbol))
            .and_then(|rows| rows.last())
    }
}

#[cfg(test)]
mod tests {
    use super::MemStore;

    #[test]
    fn put_and_get_last_returns_latest_row() {
        let mut store = MemStore::new();
        store.put("ticks", "AAPL", 1, vec![0x01]);
        store.put("ticks", "AAPL", 2, vec![0x02]);

        let row = store
            .get_last("ticks", "AAPL")
            .expect("expected last row");
        assert_eq!(row.ts, 2);
        assert_eq!(row.value, vec![0x02]);
    }

    #[test]
    fn get_last_none_for_missing_key() {
        let store = MemStore::new();
        assert!(store.get_last("ticks", "MSFT").is_none());
    }
}
