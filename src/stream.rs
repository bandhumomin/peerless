use std::sync::Arc;

use axum::body::Body;
use axum::extract::{Path, Query, State};
use axum::http::{header, HeaderMap, Method, Response, StatusCode};
use axum::response::IntoResponse;
use serde::Deserialize;

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

    #[cfg(target_arch = "wasm32")]
    let cache_url = {
        let range_str = headers
            .get(header::RANGE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("full");
        let range_clean = range_str.replace(' ', "");
        format!("https://edge.peerless/tracks/{id}/range/{range_clean}")
    };

    #[cfg(target_arch = "wasm32")]
    {
        if method == Method::GET || method == Method::HEAD {
            let cache = worker::Cache::default();
            if let Ok(Some(mut cached_resp)) = cache.get(&cache_url, true).await {
                let status_code = cached_resp
                    .headers()
                    .get("X-Original-Status")
                    .ok()
                    .flatten()
                    .and_then(|s| s.parse::<u16>().ok())
                    .unwrap_or_else(|| cached_resp.status_code());

                let mut builder = Response::builder()
                    .status(StatusCode::from_u16(status_code).unwrap_or(StatusCode::OK))
                    .header("X-Peerless-Cache", "HIT");

                for (name, value) in cached_resp.headers().entries() {
                    if !name.eq_ignore_ascii_case("X-Original-Status") {
                        if let (Ok(n), Ok(v)) = (
                            axum::http::HeaderName::from_bytes(name.as_bytes()),
                            axum::http::HeaderValue::from_str(&value),
                        ) {
                            if let Some(h) = builder.headers_mut() {
                                h.insert(n, v);
                            }
                        }
                    }
                }

                if method == Method::HEAD {
                    return builder.body(Body::empty()).unwrap_or_else(|_| (StatusCode::OK, Body::empty()).into_response());
                }

                if let Ok(bytes) = cached_resp.bytes().await {
                    return builder.body(Body::from(bytes)).unwrap_or_else(|_| (StatusCode::OK, Body::empty()).into_response());
                }
            }
        }
    }

    let target_url = format!("{}/api/v1/tracks/{id}/stream?ticket={ticket}", state.upstream_url);
    let origin_resp = forward_request(&target_url, &method, &headers, Body::empty(), None).await;

    #[cfg(target_arch = "wasm32")]
    {
        if (origin_resp.status() == StatusCode::OK || origin_resp.status() == StatusCode::PARTIAL_CONTENT)
            && method == Method::GET
        {
            let (parts, body) = origin_resp.into_parts();
            if let Ok(bytes) = axum::body::to_bytes(body, 64 * 1024 * 1024).await {
                let cache_bytes = bytes.to_vec();
                let return_bytes = bytes;

                if let Ok(mut c_resp) = worker::Response::from_bytes(cache_bytes) {
                    let _ = c_resp.headers_mut().set("cache-control", "public, max-age=2592000, immutable");
                    let _ = c_resp.headers_mut().set("X-Original-Status", &parts.status.as_u16().to_string());
                    for (k, v) in parts.headers.iter() {
                        if let Ok(v_str) = v.to_str() {
                            let _ = c_resp.headers_mut().set(k.as_str(), v_str);
                        }
                    }

                    let cache = worker::Cache::default();
                    let _ = cache.put(&cache_url, c_resp).await;
                }

                let mut res = Response::from_parts(parts, Body::from(return_bytes));
                res.headers_mut().insert("X-Peerless-Cache", axum::http::HeaderValue::from_static("MISS"));
                return res;
            } else {
                return (StatusCode::BAD_GATEWAY, "Failed to read origin body").into_response();
            }
        }
    }

    let mut res = origin_resp;
    res.headers_mut().insert("X-Peerless-Cache", axum::http::HeaderValue::from_static("MISS"));
    res
}
