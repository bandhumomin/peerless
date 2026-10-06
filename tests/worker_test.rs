use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use tower_service::Service;

use peerless::{router, AppState};

fn test_state(upstream_url: &str) -> Arc<AppState> {
    Arc::new(AppState::new(upstream_url))
}

#[tokio::test]
async fn test_default_state() {
    let state = AppState::default();
    assert_eq!(state.upstream_url, "");
}

#[tokio::test]
async fn test_track_stream_missing_ticket_returns_unauthorized() {
    let state = test_state("http://localhost:4444");
    let mut app = router(state);
    let req = Request::builder()
        .uri("/api/v1/tracks/415/stream")
        .method("GET")
        .body(Body::empty())
        .unwrap();

    let res = app.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_track_stream_empty_ticket_returns_unauthorized() {
    let state = test_state("http://localhost:4444");
    let mut app = router(state);
    let req = Request::builder()
        .uri("/api/v1/tracks/415/stream?ticket=")
        .method("GET")
        .body(Body::empty())
        .unwrap();

    let res = app.call(req).await.unwrap();
    assert_eq!(res.status(), StatusCode::UNAUTHORIZED);
}
