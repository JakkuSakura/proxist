use std::time::Duration;

use crate::{Config, Result};

#[derive(thiserror::Error, Debug)]
pub enum Error {
    #[error("http: {0}")]
    Http(String),
    #[error("transport: {0}")]
    Transport(String),
}

pub struct ChClient {
    base_url: String,
    agent: ureq::Agent,
}

impl ChClient {
    pub fn new(config: &Config) -> Result<Self> {
        let agent = ureq::AgentBuilder::new()
            .timeout_connect(Duration::from_secs(5))
            .timeout_read(Duration::from_secs(30))
            .timeout_write(Duration::from_secs(30))
            .build();
        Ok(Self {
            base_url: config.clickhouse_url.clone(),
            agent,
        })
    }

    pub fn execute(&self, sql: &str) -> Result<()> {
        let resp = self
            .agent
            .post(&self.base_url)
            .set("Content-Type", "text/plain")
            .send_string(sql);

        match resp {
            Ok(response) => {
                let status = response.status();
                if status >= 200 && status < 300 {
                    Ok(())
                } else {
                    let body = response.into_string().unwrap_or_default();
                    Err(crate::Error::ClickHouse(Error::Http(format!(
                        "status {}: {body}",
                        status
                    ))
                    .to_string()))
                }
            }
            Err(ureq::Error::Status(code, response)) => {
                let body = response.into_string().unwrap_or_default();
                Err(crate::Error::ClickHouse(
                    Error::Http(format!("status {}: {body}", code)).to_string(),
                ))
            }
            Err(err) => Err(crate::Error::ClickHouse(
                Error::Transport(err.to_string()).to_string(),
            )),
        }
    }
}
