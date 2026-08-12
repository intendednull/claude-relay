use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
use clap::Parser;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt;
use tracing_subscriber::util::SubscriberInitExt;

use relay::build_router;
use relay::cli::Cli;
use relay::config::Config;
use relay::log_file::{DEFAULT_MAX_BYTES, LogFile};
use relay::state::AppState;

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();

    let config_path = cli.resolve_config_path()?;
    let loaded = Config::load(&config_path)?;
    // Checked here, before the listener binds: none of it should first fail on a
    // request an operator has to diagnose. `AppState::new` — also before the
    // bind — validates the `[detect]`, `[notify]`, `[profiles.*]` and
    // `[policy]` rules, all of which fail silently by nature when they are
    // wrong.
    let listen_addr: SocketAddr = loaded.config.listen_addr()?;
    loaded.config.anthropic_base_url()?;
    loaded.config.state_file()?;

    init_tracing(cli.log_file.as_deref())?;

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

/// `--log-file` adds a destination rather than moving one: stderr keeps every
/// line either way, and the one `EnvFilter` above both means `RUST_LOG` scopes
/// them alike. Fails before the listener binds if the file cannot be opened —
/// the flag was passed explicitly, so logging nowhere is not a quiet fallback.
///
/// The file gets a layer of its own rather than a tee of stderr's for one
/// reason: `with_ansi(false)`. Teed, every line would land on disk wrapped in
/// the colour escapes stderr wants, in a file whose whole purpose is being read
/// later.
fn init_tracing(log_file: Option<&Path>) -> Result<()> {
    let filter = EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info"));
    let file_layer = match log_file {
        Some(path) => Some(
            tracing_subscriber::fmt::layer()
                .with_ansi(false)
                .with_writer(LogFile::open(path, DEFAULT_MAX_BYTES)?),
        ),
        None => None,
    };
    tracing_subscriber::registry()
        .with(filter)
        .with(tracing_subscriber::fmt::layer().with_writer(std::io::stderr))
        .with(file_layer)
        .init();
    Ok(())
}
