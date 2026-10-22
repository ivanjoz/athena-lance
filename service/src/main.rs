//! Lambda entry point (provided.al2023): one App per container, reused by warm invocations.
//!
//! Two kinds of events: direct `Invoke` requests (`{"op": ...}`, the Go client) get the
//! buffered JSON envelope; Function URL events (the Trino HTTP API, ingest only) get a streamed
//! HTTP response (the URL's invoke mode is RESPONSE_STREAM).

use std::sync::Arc;

use lambda_runtime::{FunctionResponse, LambdaEvent, service_fn};
use lance_service::app::App;
use lance_service::config::Config;
use lance_service::{handler, trino};
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
                let response = if trino::is_http_event(&event.payload) {
                    FunctionResponse::StreamingResponse(trino::handle(app, &request_id, event.payload).await)
                } else {
                    FunctionResponse::BufferedResponse(handler::handle(&app, &request_id, event.payload).await)
                };
                Ok::<_, lambda_runtime::Error>(response)
            }
        }))
        .await
        .map_err(|e| anyhow::anyhow!(e))
    })
}
