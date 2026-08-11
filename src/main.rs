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
    let loaded = Config::load(&config_path)?;
    // Everything checkable is checked here, before the listener binds: none of
    // it should first fail on a request an operator has to diagnose, and a
    // detection rule that can never fire fails silently by nature.
    let listen_addr: SocketAddr = loaded.config.listen_addr()?;
    loaded.config.anthropic_base_url()?;
    loaded.config.state_file()?;
    loaded.config.detect.validate()?;

    init_tracing();

    let state = AppState::new(
        Arc::new(loaded.config),
        cli.capture_errors.clone(),
        loaded.digest,
    )?;

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
