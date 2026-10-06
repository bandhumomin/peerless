mod proxy;
mod state;
mod stream;

use std::sync::Arc;

use axum::extract::{Request, State};
use axum::response::Response;
use axum::routing::get;
use axum::Router;
use tower_service::Service;
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
    req: Request,
) -> Response {
    let uri = req.uri().clone();
    let path_and_query = uri.path_and_query().map(|pq| pq.as_str()).unwrap_or(uri.path());
    let target_url = format!("{}{path_and_query}", state.upstream_url);

    let (parts, body) = req.into_parts();
    proxy::forward_request(&target_url, &parts.method, &parts.headers, body, None).await
}

#[event(fetch)]
async fn fetch(
    req: HttpRequest,
    env: Env,
    _ctx: Context,
) -> Result<axum::http::Response<axum::body::Body>> {
    let state = Arc::new(AppState::from_env(&env));
    let mut app = router(state);
    Ok(app.call(req).await?)
}
