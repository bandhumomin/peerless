use axum::body::Body;
use axum::http::{HeaderMap, Method, Response, StatusCode};
use axum::response::IntoResponse;
use worker::{CfProperties, Fetch, Headers, Request, RequestInit};

pub async fn forward_request(
    target_url: &str,
    method: &Method,
    headers: &HeaderMap,
    body: Body,
    cf: Option<CfProperties>,
) -> Response<Body> {
    if target_url.is_empty() || target_url.starts_with('/') {
        return (
            StatusCode::BAD_GATEWAY,
            "UPSTREAM_URL is not configured in Cloudflare environment",
        )
            .into_response();
    }

    let worker_method = match *method {
        Method::GET => worker::Method::Get,
        Method::POST => worker::Method::Post,
        Method::PUT => worker::Method::Put,
        Method::DELETE => worker::Method::Delete,
        Method::HEAD => worker::Method::Head,
        Method::PATCH => worker::Method::Patch,
        Method::OPTIONS => worker::Method::Options,
        _ => worker::Method::Get,
    };

    let mut init = RequestInit::new();
    init.with_method(worker_method);

    if let Some(cf_props) = cf {
        init.with_cf_properties(cf_props);
    }

    let h = Headers::new();
    let mut is_websocket = false;

    for (k, v) in headers.iter() {
        let key = k.as_str();
        if key.eq_ignore_ascii_case("upgrade") {
            if let Ok(v_str) = v.to_str() {
                if v_str.to_ascii_lowercase().contains("websocket") {
                    is_websocket = true;
                }
                let _ = h.append(key, v_str);
            }
        } else if !key.eq_ignore_ascii_case("host") && !key.eq_ignore_ascii_case("connection") {
            if let Ok(v_str) = v.to_str() {
                let _ = h.append(key, v_str);
            }
        }
    }

    if is_websocket {
        let _ = h.set("Connection", "Upgrade");
    }

    init.with_headers(h);

    if *method != Method::GET && *method != Method::HEAD {
        let body_bytes = match axum::body::to_bytes(body, 64 * 1024 * 1024).await {
            Ok(bytes) => bytes,
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    format!("Failed to read request body: {e}"),
                )
                    .into_response();
            }
        };

        if !body_bytes.is_empty() {
            #[cfg(target_arch = "wasm32")]
            {
                let array = worker::js_sys::Uint8Array::from(body_bytes.as_ref());
                init.with_body(Some(array.into()));
            }
        }
    }

    match Request::new_with_init(target_url, &init) {
        Ok(worker_req) => match Fetch::Request(worker_req).send().await {
            Ok(resp) => {
                let response: axum::http::Response<axum::body::Body> = resp.into();
                response
            }
            Err(e) => (
                StatusCode::BAD_GATEWAY,
                format!("Upstream fetch error: {e}"),
            )
                .into_response(),
        },
        Err(e) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Failed to create upstream request: {e}"),
        )
            .into_response(),
    }
}
