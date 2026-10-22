//! Runs requests locally against the real bucket and ledger, with the profile in config.toml.
//!
//!   lance-cli '{"op":"table_info","table":"ventas","detail":true}'
//!   lance-cli @request.json
//!
//! Prints the response envelope, exactly as the Lambda would return it. A Function URL event
//! (`{"requestContext":{"http":...},...}`) goes to the Trino HTTP API instead: the status and
//! headers go to stderr and the streamed body to stdout.

use std::io::Write;
use std::sync::Arc;

use futures::StreamExt;
use lance_service::app::App;
use lance_service::config::Config;
use lance_service::{handler, trino};

fn main() -> anyhow::Result<()> {
    let cfg = Config::load()?;
    cfg.export_aws_env();
    lance_service::init_tracing();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        anyhow::bail!("usage: lance-cli '<request json>' | @file.json ...");
    }
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let app = Arc::new(App::new(cfg).await?);
        for arg in args {
            let text = match arg.strip_prefix('@') {
                Some(path) => std::fs::read_to_string(path)?,
                None => arg,
            };
            let payload: serde_json::Value = serde_json::from_str(&text)?;
            let request_id = format!("local-{}", uuid::Uuid::new_v4());
            if trino::is_http_event(&payload) {
                let response = trino::handle(app.clone(), &request_id, payload).await;
                eprintln!("{:?}", response.metadata_prelude);
                let mut body = response.stream;
                let mut stdout = std::io::stdout().lock();
                while let Some(chunk) = body.next().await {
                    stdout.write_all(&chunk?)?;
                }
                writeln!(stdout)?;
            } else {
                let out = handler::handle(&app, &request_id, payload).await;
                println!("{}", serde_json::to_string_pretty(&out)?);
            }
        }
        Ok(())
    })
}
