use std::sync::Arc;

use binance_sdk::common::config::{ConfigurationRestApi, HttpAgent};
use binance_sdk::common::errors::ConnectorError;
use binance_sdk::common::utils::http_request;
use httpmock::prelude::*;
use reqwest::Method;
use serde_json::Value;

fn configuration(server: &MockServer) -> ConfigurationRestApi {
    ConfigurationRestApi::builder()
        .agent(HttpAgent(Arc::new(reqwest::ClientBuilder::no_proxy)))
        .base_path(server.url(""))
        .build()
        .expect("build local SDK configuration")
}

async fn request(server: &MockServer, path: &str) -> ConnectorError {
    let request = reqwest::Client::new()
        .request(Method::GET, server.url(path))
        .build()
        .expect("build local rate-limit request");
    match http_request::<Value>(request, &configuration(server)).await {
        Ok(_) => panic!("rate-limit response must fail"),
        Err(error) => error,
    }
}

#[tokio::test]
async fn too_many_requests_preserves_structured_retry_after() {
    let server = MockServer::start();
    let response = server.mock(|when, then| {
        when.method(GET).path("/429");
        then.status(429)
            .header("retry-after", "7")
            .header(
                "x-debug-url",
                "https://api.binance.com/path?timestamp=123&signature=secret",
            )
            .json_body(serde_json::json!({"code": -1003, "msg": "too many requests"}));
    });

    let error = request(&server, "/429").await;

    assert_eq!(error.retry_after_ms(), Some(7_000));
    assert!(matches!(
        error,
        ConnectorError::TooManyRequestsError {
            code: Some(-1003),
            retry_after_ms: Some(7_000),
            ..
        }
    ));
    let rendered = error.to_string();
    assert!(!rendered.contains("signature="));
    assert!(!rendered.contains("timestamp="));
    assert!(!rendered.contains("secret"));
    assert!(!rendered.contains("x-debug-url"));
    response.assert();
}

#[tokio::test]
async fn rate_limit_ban_preserves_structured_retry_after() {
    let server = MockServer::start();
    let response = server.mock(|when, then| {
        when.method(GET).path("/418");
        then.status(418)
            .header("Retry-After", "61")
            .json_body(serde_json::json!({"code": -1003, "msg": "IP banned"}));
    });

    let error = request(&server, "/418").await;

    assert_eq!(error.retry_after_ms(), Some(61_000));
    assert!(matches!(
        error,
        ConnectorError::RateLimitBanError {
            retry_after_ms: Some(61_000),
            ..
        }
    ));
    response.assert();
}

#[tokio::test]
async fn missing_or_invalid_retry_after_is_none_without_leaking_raw_header() {
    for (path, header) in [("/missing", None), ("/invalid", Some("signed-secret"))] {
        let server = MockServer::start();
        let response = server.mock(|when, then| {
            when.method(GET).path(path);
            let then = then
                .status(429)
                .json_body(serde_json::json!({"code": -1003, "msg": "limited"}));
            if let Some(header) = header {
                then.header("Retry-After", header);
            }
        });

        let error = request(&server, path).await;

        assert_eq!(error.retry_after_ms(), None);
        assert!(!error.to_string().contains("signed-secret"));
        response.assert();
    }
}
