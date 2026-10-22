//! Lance table service: schema-driven tables on S3 Express One Zone, driven by JSON requests
//! from the Go client (lancego). See docs/decisions.md.

pub mod app;
pub mod config;
pub mod error;
pub mod handler;
pub mod ledger;
pub mod lock;
pub mod ops;
pub mod protocol;
pub mod spec;
pub mod trino;

/// JSON logs to stderr (CloudWatch); level from RUST_LOG, default info.
pub fn init_tracing() {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info,lance=warn,aws_config=warn"));
    tracing_subscriber::fmt()
        .json()
        .with_env_filter(filter)
        .with_current_span(false)
        .without_time()
        .with_writer(std::io::stderr)
        .init();
}
