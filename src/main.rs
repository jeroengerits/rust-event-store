use clap::Parser;
use eventstore::{
    api,
    config::{Cli, Command},
    storage::Storage,
};
use std::{error::Error, net::SocketAddr};
use tokio_util::sync::CancellationToken;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let Cli {
        command: Command::Serve(mut config),
    } = Cli::parse();
    apply_cloud_defaults(
        &mut config,
        std::env::var_os("EVENTSTORE_PORT").is_some() || has_arg("--port"),
        std::env::var_os("EVENTSTORE_DATABASE").is_some() || has_arg("--database"),
        std::env::var("PORT").ok().as_deref(),
        std::env::var("DATABASE_URL").ok().as_deref(),
    )?;
    config.validate().map_err(std::io::Error::other)?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_new(&config.log_level)?)
        .init();
    let storage = Storage::open(&config.database).await?;
    let address = SocketAddr::new(config.host, config.port);
    let listener = tokio::net::TcpListener::bind(address).await?;
    let shutdown = CancellationToken::new();
    let app = api::router(storage, config, shutdown.clone());
    tracing::info!(%address, "listening");
    let server_shutdown = shutdown.clone();
    let server = async move {
        axum::serve(listener, app)
            .with_graceful_shutdown(server_shutdown.cancelled_owned())
            .await
    };
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        _ = shutdown_signal() => {
            tracing::info!("shutting down");
            shutdown.cancel();
            match tokio::time::timeout(std::time::Duration::from_secs(10), &mut server).await {
                Ok(result) => result?,
                Err(_) => tracing::warn!("graceful shutdown deadline expired; closing connections"),
            }
        }
    }
    Ok(())
}

fn has_arg(name: &str) -> bool {
    std::env::args_os().any(|arg| {
        arg == name
            || arg
                .to_str()
                .and_then(|arg| arg.strip_prefix(name))
                .is_some_and(|suffix| suffix.starts_with('='))
    })
}

fn apply_cloud_defaults(
    config: &mut eventstore::config::Config,
    port_overridden: bool,
    database_overridden: bool,
    port: Option<&str>,
    database_url: Option<&str>,
) -> Result<(), std::num::ParseIntError> {
    if !port_overridden && let Some(port) = port {
        config.port = port.parse()?;
    }
    if !database_overridden && let Some(database_url) = database_url {
        config.database = database_url.to_owned();
    }
    Ok(())
}

async fn shutdown_signal() {
    #[cfg(unix)]
    {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut terminate) => {
                tokio::select! { _ = tokio::signal::ctrl_c() => {}, _ = terminate.recv() => {} }
            }
            Err(error) => {
                tracing::warn!(%error, "could not register SIGTERM handler");
                let _ = tokio::signal::ctrl_c().await;
            }
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests {
    use super::*;
    use eventstore::config::Config;

    fn config() -> Config {
        Config {
            host: "127.0.0.1".parse().unwrap(),
            port: 8080,
            database: "".into(),
            api_key: None,
            log_level: "info".into(),
            max_event_size: 1_048_576,
            max_batch_size: 100,
            max_body_size: 4_194_304,
        }
    }

    #[test]
    fn cloud_defaults_and_explicit_overrides_are_independent() {
        for (port_override, database_override) in
            [(false, false), (false, true), (true, false), (true, true)]
        {
            let mut config = config();
            apply_cloud_defaults(
                &mut config,
                port_override,
                database_override,
                Some("9000"),
                Some("postgres://localhost/store"),
            )
            .unwrap();
            assert_eq!(config.port, if port_override { 8080 } else { 9000 });
            assert_eq!(
                config.database,
                if database_override {
                    ""
                } else {
                    "postgres://localhost/store"
                }
            );
        }
    }

    #[test]
    fn invalid_cloud_port_is_rejected_unless_overridden() {
        assert!(apply_cloud_defaults(&mut config(), false, false, Some("invalid"), None).is_err());
        assert!(apply_cloud_defaults(&mut config(), true, false, Some("invalid"), None).is_ok());
    }

    #[test]
    fn missing_cloud_defaults_leave_configuration_unchanged() {
        let mut config = config();
        apply_cloud_defaults(&mut config, false, false, None, None).unwrap();
        assert_eq!(config.port, 8080);
        assert_eq!(config.database, "");
    }
}
