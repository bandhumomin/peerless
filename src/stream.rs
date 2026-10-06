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

    let target_url = format!("{}/api/v1/tracks/{id}/stream?ticket={ticket}", state.upstream_url);
    let cache_key = format!("{}/api/v1/tracks/{id}/stream?range={range_clean}", state.upstream_url);

    let mut cf = CfProperties::default();
    cf.cache_everything = Some(true);
    cf.cache_key = Some(cache_key);
    cf.cache_ttl = Some(2592000);

    forward_request(&target_url, &method, &headers, Body::empty(), Some(cf)).await
}

pub async fn handle_native_stream(
    req: worker::Request,
    state: &AppState,
) -> worker::Result<worker::Response> {
    let url = req.url()?;
    let path = url.path();

    let id_str = match path
        .strip_prefix("/api/v1/tracks/")
        .and_then(|s| s.strip_suffix("/stream"))
    {
        Some(s) => s,
        None => return worker::Response::error("Invalid stream path", 400),
    };

    let ticket = match url
        .query_pairs()
        .find(|(k, _)| k == "ticket")
        .map(|(_, v)| v.trim().to_string())
    {
        Some(t) if !t.is_empty() => t,
        _ => {
            let headers = worker::Headers::new();
            let _ = headers.set("content-type", "application/json");
            return Ok(worker::Response::builder()
                .with_status(401)
                .with_headers(headers)
                .fixed(br#"{"error":"Missing 'ticket' query parameter"}"#.to_vec()));
        }
    };

    let range_str = req
        .headers()
        .get("range")?
        .unwrap_or_else(|| "full".to_string());
    let range_clean = range_str.replace(' ', "");

    let target_url = format!("{}/api/v1/tracks/{id_str}/stream?ticket={ticket}", state.upstream_url);
    let cache_key = format!("{}/api/v1/tracks/{id_str}/stream?range={range_clean}", state.upstream_url);

    let mut cf = worker::CfProperties::default();
    cf.cache_everything = Some(true);
    cf.cache_key = Some(cache_key);
    cf.cache_ttl = Some(2592000);

    let mut init = worker::RequestInit::new();
    init.with_method(req.method());
    init.with_cf_properties(cf);

    let forward_headers = worker::Headers::new();
    for (k, v) in req.headers().entries() {
        if !k.eq_ignore_ascii_case("host") && !k.eq_ignore_ascii_case("connection") {
            let _ = forward_headers.append(&k, &v);
        }
    }
    init.with_headers(forward_headers);

    let upstream_req = worker::Request::new_with_init(&target_url, &init)?;
    worker::Fetch::Request(upstream_req).send().await
}

