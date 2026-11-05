use clap::{Parser, Subcommand};
use std::net::IpAddr;

#[derive(Debug, Parser)]
#[command(version, about = "A small, durable HTTP event store")]
pub struct Cli {
    #[command(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    Serve(Config),
}

#[derive(Debug, Clone, clap::Args)]
pub struct Config {
    #[arg(long, env = "EVENTSTORE_HOST", default_value = "127.0.0.1")]
    pub host: IpAddr,
    #[arg(long, env = "EVENTSTORE_PORT", default_value_t = 8080)]
    pub port: u16,
    #[arg(long, env = "EVENTSTORE_DATABASE", default_value = "./data/events.db")]
    pub database: String,
    #[arg(long, env = "EVENTSTORE_API_KEY", hide_env_values = true)]
    pub api_key: Option<String>,
    #[arg(long, env = "EVENTSTORE_LOG_LEVEL", default_value = "info")]
    pub log_level: String,
    #[arg(long, env = "EVENTSTORE_MAX_EVENT_SIZE", default_value_t = 1_048_576)]
    pub max_event_size: usize,
    #[arg(long, env = "EVENTSTORE_MAX_BATCH_SIZE", default_value_t = 100)]
    pub max_batch_size: usize,
    #[arg(long, env = "EVENTSTORE_MAX_BODY_SIZE", default_value_t = 4_194_304)]
    pub max_body_size: usize,
}

impl Config {
    pub fn validate(&mut self) -> Result<(), String> {
        if self.api_key.as_ref().is_some_and(|key| key.is_empty()) {
            self.api_key = None;
        }
        if !self.host.is_loopback() && self.api_key.is_none() {
            return Err(
                "a non-loopback bind address requires EVENTSTORE_API_KEY or --api-key".into(),
            );
        }
        if self.max_event_size == 0 || self.max_batch_size == 0 || self.max_body_size == 0 {
            return Err("size and batch limits must be greater than zero".into());
        }
        if self.max_batch_size > 10_000 || self.max_body_size > 256 * 1024 * 1024 {
            return Err("maximum batch size is 10000 and maximum body size is 256 MiB".into());
        }
        if self.database.is_empty() {
            return Err("database path must not be empty".into());
        }
        Ok(())
    }
}
