use worker::Env;

#[derive(Clone, Debug, Default)]
pub struct AppState {
    pub upstream_url: String,
}

impl AppState {
    pub fn new(upstream_url: impl Into<String>) -> Self {
        Self {
            upstream_url: upstream_url.into().trim().trim_end_matches('/').to_string(),
        }
    }

    pub fn from_env(env: &Env) -> Self {
        let upstream_url = env
            .var("UPSTREAM_URL")
            .ok()
            .map(|v| v.to_string())
            .unwrap_or_default();

        Self::new(upstream_url)
    }
}
