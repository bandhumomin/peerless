mod proxy;
mod state;
mod stream;

use std::sync::Arc;

use axum::extract::{Request as AxumRequest, State};
use axum::response::Response as AxumResponse;
use axum::routing::get;
use axum::Router;
use worker::*;

pub use state::AppState;

pub fn router(state: Arc<AppState>) -> Router {
    Router::new()
        .route(
            "/api/v1/tracks/{id}/stream",
            get(stream::stream_handler),
        )
        .fallback(fallback_handler)
        .with_state(state)
}

#[worker::send]
async fn fallback_handler(
    State(state): State<Arc<AppState>>,
    req: AxumRequest,
) -> AxumResponse {
    let uri = req.uri().clone();
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or(uri.path());
    let target_url = format!("{}{path_and_query}", state.upstream_url);

    let (parts, body) = req.into_parts();
    proxy::forward_request(&target_url, &parts.method, &parts.headers, body, None).await
}

#[event(fetch)]
async fn fetch(
    req: Request,
    env: Env,
    _ctx: Context,
) -> Result<Response> {
    let state = AppState::from_env(&env);
    let path = req.path();

    if path.starts_with("/api/v1/tracks/") && path.ends_with("/stream") {
        return stream::handle_native_stream(req, &state).await;
    }

    proxy::forward_native(req, &state).await
}

