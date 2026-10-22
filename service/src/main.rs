//! Lambda entry point (provided.al2023): one App per container, reused by warm invocations.

use std::sync::Arc;

use lambda_runtime::{LambdaEvent, service_fn};
use lance_service::app::App;
use lance_service::config::Config;
use lance_service::handler;
use serde_json::Value;

fn main() -> anyhow::Result<()> {
    let cfg = Config::load()?;
    // Before the runtime starts any thread.
    cfg.export_aws_env();
    lance_service::init_tracing();

    tokio::runtime::Builder::new_multi_thread().enable_all().build()?.block_on(async {
        let app = Arc::new(App::new(cfg).await?);
        lambda_runtime::run(service_fn(move |event: LambdaEvent<Value>| {
            let app = app.clone();
            async move {
                let request_id = event.context.request_id.clone();
                Ok::<Value, lambda_runtime::Error>(handler::handle(&app, &request_id, event.payload).await)
            }
        }))
        .await
        .map_err(|e| anyhow::anyhow!(e))
    })
}
