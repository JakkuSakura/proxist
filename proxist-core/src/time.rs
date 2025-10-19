use std::time::{Duration, SystemTime, UNIX_EPOCH};

fn to_micros(ts: SystemTime) -> i64 {
    match ts.duration_since(UNIX_EPOCH) {
        Ok(duration) => duration.as_micros() as i64,
        Err(err) => {
            let duration = err.duration();
            -(duration.as_micros() as i64)
        }
    }
}

fn from_micros(micros: i64) -> SystemTime {
    if micros >= 0 {
        UNIX_EPOCH + Duration::from_micros(micros as u64)
    } else {
        UNIX_EPOCH - Duration::from_micros((-micros) as u64)
    }
}

pub mod serde_micros {
    use super::{from_micros, to_micros};
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::SystemTime;

    pub fn serialize<S>(ts: &SystemTime, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_i64(to_micros(*ts))
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<SystemTime, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micros = i64::deserialize(deserializer)?;
        Ok(from_micros(micros))
    }
}

pub mod serde_opt_micros {
    use super::{from_micros, to_micros};
    use serde::{Deserialize, Deserializer, Serializer};
    use std::time::SystemTime;

    pub fn serialize<S>(value: &Option<SystemTime>, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        match value {
            Some(ts) => serializer.serialize_some(&to_micros(*ts)),
            None => serializer.serialize_none(),
        }
    }

    pub fn deserialize<'de, D>(deserializer: D) -> Result<Option<SystemTime>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let micros = Option::<i64>::deserialize(deserializer)?;
        Ok(micros.map(from_micros))
    }
}
