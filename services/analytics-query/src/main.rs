use std::env;
use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::routing::get;
use axum::{Json, Router};
use crush_analytics_query::{AnalyticsEngine, AnalyticsRequest};
use serde::Deserialize;

#[derive(Clone)]
struct AppState {
    engine: Arc<AnalyticsEngine>,
}

#[derive(Debug, Deserialize)]
struct PlanRequest {
    dataset: String,
    limit: usize,
}

#[tokio::main]
async fn main() {
    let lakehouse_root = env::var("LAKEHOUSE_ROOT").unwrap_or_else(|_| "/lakehouse".to_owned());
    let state = AppState {
        engine: Arc::new(AnalyticsEngine::new(lakehouse_root)),
    };
    let app = Router::new()
        .route("/healthz", get(|| async { "ok" }))
        .route("/v1/analytics/plan", get(plan))
        .with_state(state);
    let listener = tokio::net::TcpListener::bind("0.0.0.0:8090")
        .await
        .expect("bind analytics-query");
    axum::serve(listener, app)
        .await
        .expect("serve analytics-query");
}

async fn plan(
    State(state): State<AppState>,
    headers: HeaderMap,
    axum::extract::Query(request): axum::extract::Query<PlanRequest>,
) -> Result<Json<crush_analytics_query::QueryPlan>, (StatusCode, String)> {
    let tenant_id = headers
        .get("x-crush-tenant-id")
        .and_then(|value| value.to_str().ok())
        .filter(|value| !value.is_empty())
        .ok_or((
            StatusCode::BAD_REQUEST,
            "X-CRUSH-Tenant-ID is required".to_owned(),
        ))?;
    if headers.get("purpose-of-use").is_none() {
        return Err((
            StatusCode::BAD_REQUEST,
            "Purpose-Of-Use is required".to_owned(),
        ));
    }
    let plan = state
        .engine
        .plan(&AnalyticsRequest {
            tenant_id: tenant_id.to_owned(),
            dataset: request.dataset,
            limit: request.limit,
        })
        .map_err(|error| (StatusCode::BAD_REQUEST, error.to_string()))?;
    Ok(Json(plan))
}
