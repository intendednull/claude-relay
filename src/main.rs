use std::net::SocketAddr;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;

use relay::build_router;
use relay::cli::Cli;
use relay::config::Config;
use relay::state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let config_path = cli.resolve_config_path()?;
    let config = Config::load(&config_path)?;
    let listen_addr: SocketAddr = config.listen_addr()?;

    init_tracing();

    let state = AppState::new(Arc::new(config), cli.capture_errors.clone())?;

    let app = build_router(state);
    let listener = tokio::net::TcpListener::bind(listen_addr).await?;

    tracing::info!(
        address = %listen_addr,
        config_path = %config_path.display(),
        "relay starting"
    );

    axum::serve(listener, app).await?;

    Ok(())
}

fn init_tracing() {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}
