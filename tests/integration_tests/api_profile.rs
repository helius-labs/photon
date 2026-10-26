use sea_orm::DatabaseConnection;
use serde::Serialize;
use std::{
    fs::{File, OpenOptions},
    io::Write,
    sync::{Arc, Mutex},
    time::Instant,
};
use tracing::{field::Visit, span::Attributes, Event, Id, Subscriber};
use tracing_subscriber::{
    filter::filter_fn, layer::Context, prelude::*, registry::LookupSpan, EnvFilter, Layer,
};

const API_TARGET: &str = "photon_indexer::api::api";
const DB_TARGET: &str = "photon_api_profile::database";

#[derive(Default, Serialize)]
struct QuerySample {
    sql: String,
    elapsed_ns: u64,
    failed: bool,
}

impl Visit for QuerySample {
    fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
        if field.name() == "sql" {
            self.sql = value.to_string();
        }
    }

    fn record_u64(&mut self, field: &tracing::field::Field, value: u64) {
        if field.name() == "elapsed_ns" {
            self.elapsed_ns = value;
        }
    }

    fn record_bool(&mut self, field: &tracing::field::Field, value: bool) {
        if field.name() == "failed" {
            self.failed = value;
        }
    }

    fn record_debug(&mut self, _field: &tracing::field::Field, _value: &dyn std::fmt::Debug) {}
}

struct ApiCall {
    started: Instant,
    method: &'static str,
    test: String,
    failed: bool,
    queries: Vec<QuerySample>,
}

struct ApiProfileLayer {
    output: Option<Arc<Mutex<File>>>,
}

impl<S> Layer<S> for ApiProfileLayer
where
    S: Subscriber + for<'a> LookupSpan<'a>,
{
    fn on_new_span(&self, attributes: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
        if attributes.metadata().target() != API_TARGET {
            return;
        }
        if let Some(span) = ctx.span(id) {
            span.extensions_mut().insert(ApiCall {
                started: Instant::now(),
                method: attributes.metadata().name(),
                test: std::thread::current()
                    .name()
                    .unwrap_or("unnamed")
                    .to_string(),
                failed: false,
                queries: Vec::new(),
            });
        }
    }

    fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
        let Some(scope) = ctx.event_scope(event) else {
            return;
        };
        for span in scope {
            let mut extensions = span.extensions_mut();
            let Some(call) = extensions.get_mut::<ApiCall>() else {
                continue;
            };
            if event.metadata().target() == DB_TARGET {
                let mut query = QuerySample::default();
                event.record(&mut query);
                call.queries.push(query);
            } else if event.metadata().target() == API_TARGET
                && event.metadata().fields().field("error").is_some()
            {
                call.failed = true;
            }
        }
    }

    fn on_close(&self, id: Id, ctx: Context<'_, S>) {
        let Some(span) = ctx.span(&id) else {
            return;
        };
        let mut extensions = span.extensions_mut();
        let Some(call) = extensions.remove::<ApiCall>() else {
            return;
        };
        let elapsed_ns = call.started.elapsed().as_nanos();
        let record = serde_json::json!({
            "method": call.method,
            "test": call.test,
            "success": !call.failed,
            "elapsed_ns": elapsed_ns,
            "database_queries": call.queries.len(),
            "database_elapsed_ns": call.queries.iter().map(|q| u128::from(q.elapsed_ns)).sum::<u128>(),
            "queries": call.queries,
        });
        if let Some(output) = &self.output {
            writeln!(output.lock().expect("Profile output lock"), "{record}")
                .expect("Write API profile");
        } else {
            eprintln!("PHOTON_API_PROFILE {record}");
        }
    }
}

pub fn setup_logging(env_filter: String) {
    let output = std::env::var_os("PHOTON_API_PROFILE_OUT").map(|path| {
        Arc::new(Mutex::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(path)
                .expect("Open PHOTON_API_PROFILE_OUT"),
        ))
    });
    tracing_subscriber::registry()
        .with(
            ApiProfileLayer { output }.with_filter(filter_fn(|metadata| {
                metadata.target() == API_TARGET || metadata.target() == DB_TARGET
            })),
        )
        .with(
            tracing_subscriber::fmt::layer()
                .with_test_writer()
                .with_filter(EnvFilter::new(env_filter)),
        )
        .init();
}

pub fn instrument_connection(mut conn: DatabaseConnection) -> DatabaseConnection {
    conn.set_metric_callback(|info| {
        tracing::trace!(
            target: DB_TARGET,
            sql = info.statement.sql.as_str(),
            elapsed_ns = u64::try_from(info.elapsed.as_nanos()).unwrap_or(u64::MAX),
            failed = info.failed,
        );
    });
    conn
}
