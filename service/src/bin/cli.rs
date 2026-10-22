//! Runs requests locally against the real bucket and ledger, with the profile in config.toml.
//!
//!   lance-cli '{"op":"table_info","table":"ventas","detail":true}'
//!   lance-cli @request.json
//!
//! Prints the response envelope, exactly as the Lambda would return it.

use lance_service::app::App;
use lance_service::config::Config;
use lance_service::handler;

fn main() -> anyhow::Result<()> {
    let cfg = Config::load()?;
    cfg.export_aws_env();
    lance_service::init_tracing();

    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.is_empty() {
        anyhow::bail!("usage: lance-cli '<request json>' | @file.json ...");
    }
    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let app = App::new(cfg).await?;
        for arg in args {
            let text = match arg.strip_prefix('@') {
                Some(path) => std::fs::read_to_string(path)?,
                None => arg,
            };
            let payload: serde_json::Value = serde_json::from_str(&text)?;
            let request_id = format!("local-{}", uuid::Uuid::new_v4());
            let out = handler::handle(&app, &request_id, payload).await;
            println!("{}", serde_json::to_string_pretty(&out)?);
        }
        Ok(())
    })
}
