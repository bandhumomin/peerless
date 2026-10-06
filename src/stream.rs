use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, Method, Response, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;
use worker::CfProperties;

use crate::proxy::forward_request;
use crate::state::AppState;

#[derive(Debug, Deserialize)]
pub struct StreamQuery {
    pub ticket: Option<String>,
}

#[worker::send]
pub async fn stream_handler(
    State(state): State<Arc<AppState>>,
    Path(id): Path<i32>,
    method: Method,
    headers: HeaderMap,
    Query(query): Query<StreamQuery>,
) -> Response<Body> {
    let ticket = match query.ticket.as_deref().map(str::trim).filter(|t| !t.is_empty()) {
        Some(t) => t,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                [(header::CONTENT_TYPE, "application/json")],
                r#"{"error":"Missing 'ticket' query parameter"}"#,
            )
                .into_response();
        }
    };

    let range_str = headers
        .get(header::RANGE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("full");

    let range_clean = range_str.replace(' ', "");
    let cache_key = format!("https://edge.peerless/tracks/{id}/range/{range_clean}");
    let target_url = format!("{}/api/v1/tracks/{id}/stream?ticket={ticket}", state.upstream_url);

    let mut cf = CfProperties::default();
    cf.cache_everything = Some(true);
    cf.cache_key = Some(cache_key);
    cf.cache_ttl = Some(2592000);

    forward_request(&target_url, &method, &headers, Body::empty(), Some(cf)).await
}
