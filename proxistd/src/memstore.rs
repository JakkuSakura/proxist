use std::collections::HashMap;

#[derive(Debug, Clone)]
pub struct Row {
    pub ts: u64,
    pub value: Vec<u8>,
}

#[derive(Debug, Default)]
pub struct MemStore {
    data: HashMap<(String, String), Vec<Row>>,
}

impl MemStore {
    pub fn new() -> Self {
        Self {
            data: HashMap::new(),
        }
    }

    pub fn put(&mut self, table: &str, symbol: &str, ts: u64, value: Vec<u8>) {
        let key = (table.to_string(), symbol.to_string());
        let entry = self.data.entry(key).or_insert_with(Vec::new);
        entry.push(Row { ts, value });
    }

    pub fn get_last(&self, table: &str, symbol: &str) -> Option<&Row> {
        let key = (table.to_string(), symbol.to_string());
        self.data.get(&key).and_then(|rows| rows.last())
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
