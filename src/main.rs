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
    // EVENTSTORE_PORT and --port take precedence over Cloud's PORT.
    if std::env::var_os("EVENTSTORE_PORT").is_none()
        && !std::env::args_os()
            .any(|arg| arg == "--port" || arg.to_string_lossy().starts_with("--port="))
        && let Ok(port) = std::env::var("PORT")
    {
        config.port = port.parse()?;
    }
    if std::env::var_os("EVENTSTORE_DATABASE").is_none()
        && !std::env::args_os()
            .any(|arg| arg == "--database" || arg.to_string_lossy().starts_with("--database="))
        && let Ok(url) = std::env::var("DATABASE_URL")
    {
        config.database = url;
    }
    config.validate().map_err(std::io::Error::other)?;
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(EnvFilter::try_new(&config.log_level)?)
        .init();
    if !config.database.starts_with("postgres://")
        && !config.database.starts_with("postgresql://")
        && !config.database.starts_with("sqlite:")
        && let Some(parent) = std::path::Path::new(&config.database)
            .parent()
            .filter(|p| !p.as_os_str().is_empty())
    {
        tokio::fs::create_dir_all(parent).await?;
    }
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
