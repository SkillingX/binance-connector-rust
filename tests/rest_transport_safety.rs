use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::thread;
use std::time::{Duration, Instant};

use binance_sdk::common::config::{ConfigurationRestApi, HttpAgent};
use binance_sdk::common::utils::http_request;
use reqwest::Method;
use serde_json::Value;

fn local_probe_agent() -> HttpAgent {
    HttpAgent(Arc::new(reqwest::ClientBuilder::no_proxy))
}

fn spawn_truncated_response_server() -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local SDK transport probe");
    listener
        .set_nonblocking(true)
        .expect("make local SDK transport probe non-blocking");
    let address = listener.local_addr().expect("read local probe address");
    let requests = Arc::new(AtomicUsize::new(0));
    let request_count = Arc::clone(&requests);

    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let mut request = [0_u8; 4096];
                    let _ = stream.read(&mut request);
                    request_count.fetch_add(1, Ordering::SeqCst);
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\nconnection: close\r\n\r\n{}",
                        )
                        .expect("write deliberately truncated response");
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("local SDK transport probe failed: {error}"),
            }
        }
    });

    (format!("http://{address}/mutation"), requests, handle)
}

fn spawn_dropped_connection_server() -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local SDK execute-error probe");
    listener
        .set_nonblocking(true)
        .expect("make local SDK execute-error probe non-blocking");
    let address = listener.local_addr().expect("read local probe address");
    let requests = Arc::new(AtomicUsize::new(0));
    let request_count = Arc::clone(&requests);

    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((stream, _)) => {
                    request_count.fetch_add(1, Ordering::SeqCst);
                    drop(stream);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("local SDK execute-error probe failed: {error}"),
            }
        }
    });

    (format!("http://{address}/mutation"), requests, handle)
}

fn closed_local_url(path_and_query: &str) -> String {
    let listener = TcpListener::bind("127.0.0.1:0").expect("reserve closed local port");
    let address = listener.local_addr().expect("read reserved local port");
    drop(listener);
    format!("http://{address}/{path_and_query}")
}

fn spawn_recovering_get_server() -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local SDK retry probe");
    listener
        .set_nonblocking(true)
        .expect("make local SDK retry probe non-blocking");
    let address = listener.local_addr().expect("read local retry address");
    let requests = Arc::new(AtomicUsize::new(0));
    let request_count = Arc::clone(&requests);
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let attempt = request_count.fetch_add(1, Ordering::SeqCst);
                    let mut request = [0_u8; 4096];
                    let _ = stream.read(&mut request);
                    let response = if attempt == 0 {
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 100\r\nconnection: close\r\n\r\n{}"
                    } else {
                        "HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}"
                    };
                    stream
                        .write_all(response.as_bytes())
                        .expect("write SDK read response");
                    if attempt > 0 {
                        break;
                    }
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("local SDK retry probe failed: {error}"),
            }
        }
    });
    (format!("http://{address}/read"), requests, handle)
}

fn spawn_recovering_execute_get_server() -> (String, Arc<AtomicUsize>, thread::JoinHandle<()>) {
    let listener = TcpListener::bind("127.0.0.1:0").expect("bind local SDK execute retry probe");
    listener
        .set_nonblocking(true)
        .expect("make local SDK execute retry probe non-blocking");
    let address = listener.local_addr().expect("read local retry address");
    let requests = Arc::new(AtomicUsize::new(0));
    let request_count = Arc::clone(&requests);
    let handle = thread::spawn(move || {
        let deadline = Instant::now() + Duration::from_secs(1);
        while Instant::now() < deadline {
            match listener.accept() {
                Ok((mut stream, _)) => {
                    let attempt = request_count.fetch_add(1, Ordering::SeqCst);
                    if attempt == 0 {
                        drop(stream);
                        continue;
                    }
                    let mut request = [0_u8; 4096];
                    let _ = stream.read(&mut request);
                    stream
                        .write_all(
                            b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: 2\r\nconnection: close\r\n\r\n{}",
                        )
                        .expect("write recovered SDK read response");
                    break;
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("local SDK execute retry probe failed: {error}"),
            }
        }
    });
    (format!("http://{address}/read"), requests, handle)
}

#[tokio::test]
async fn post_is_not_replayed_after_response_body_failure() {
    let (url, requests, server) = spawn_truncated_response_server();
    let configuration = ConfigurationRestApi::builder()
        .retries(1)
        .agent(local_probe_agent())
        .build()
        .expect("build SDK transport config");
    let request = reqwest::Client::new()
        .request(Method::POST, url)
        .build()
        .expect("build mutation request");

    let result = http_request::<Value>(request, &configuration).await;

    server.join().expect("local SDK transport probe exits");
    assert!(result.is_err(), "truncated mutation response must fail");
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "a response-body failure must not replay a mutation"
    );
}

#[tokio::test]
async fn delete_is_not_replayed_after_response_body_failure() {
    let (url, requests, server) = spawn_truncated_response_server();
    let configuration = ConfigurationRestApi::builder()
        .retries(1)
        .agent(local_probe_agent())
        .build()
        .expect("build SDK transport config");
    let request = reqwest::Client::new()
        .request(Method::DELETE, url)
        .build()
        .expect("build cancellation request");

    let result = http_request::<Value>(request, &configuration).await;

    server.join().expect("local SDK transport probe exits");
    assert!(result.is_err(), "truncated cancellation response must fail");
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "a response-body failure must not replay a cancellation"
    );
}

#[tokio::test]
async fn delete_is_not_replayed_after_execute_failure() {
    let (url, requests, server) = spawn_dropped_connection_server();
    let configuration = ConfigurationRestApi::builder()
        .retries(2)
        .backoff(1)
        .agent(local_probe_agent())
        .build()
        .expect("build SDK transport config");
    let request = reqwest::Client::new()
        .request(Method::DELETE, url)
        .build()
        .expect("build cancellation request");

    let result = http_request::<Value>(request, &configuration).await;

    server.join().expect("local SDK transport probe exits");
    assert!(result.is_err(), "dropped cancellation request must fail");
    assert_eq!(
        requests.load(Ordering::SeqCst),
        1,
        "an execute failure must not replay a cancellation"
    );
}

#[tokio::test]
async fn zero_retries_returns_execute_error_without_panicking() {
    let configuration = ConfigurationRestApi::builder()
        .retries(0)
        .agent(local_probe_agent())
        .build()
        .expect("build SDK transport config");
    let request = reqwest::Client::new()
        .request(Method::GET, closed_local_url("read"))
        .build()
        .expect("build read request");

    let result = http_request::<Value>(request, &configuration).await;

    assert!(result.is_err());
}

#[tokio::test]
async fn transport_errors_do_not_expose_signed_query_parameters() {
    let configuration = ConfigurationRestApi::builder()
        .retries(1)
        .agent(local_probe_agent())
        .build()
        .expect("build SDK transport config");
    let signed_url = closed_local_url("mutation?timestamp=123&signature=test-secret");
    let request = reqwest::Client::new()
        .request(Method::POST, signed_url)
        .build()
        .expect("build signed mutation request");

    let error = match http_request::<Value>(request, &configuration).await {
        Ok(_) => panic!("closed port must fail"),
        Err(error) => error.to_string(),
    };

    assert!(!error.contains("signature="));
    assert!(!error.contains("timestamp="));
    assert!(!error.contains("test-secret"));
    assert!(!error.contains("http://"));
    assert!(!error.contains("/mutation"));
}

#[tokio::test]
async fn get_keeps_one_bounded_retry_after_response_body_failure() {
    let (url, requests, server) = spawn_recovering_get_server();
    let configuration = ConfigurationRestApi::builder()
        .retries(1)
        .backoff(1)
        .agent(local_probe_agent())
        .build()
        .expect("build SDK transport config");
    let request = reqwest::Client::new()
        .request(Method::GET, url)
        .build()
        .expect("build read request");

    let response = http_request::<Value>(request, &configuration)
        .await
        .expect("GET retries once and recovers");
    let data = response.data().await.expect("parse recovered GET response");

    server.join().expect("local SDK retry probe exits");
    assert_eq!(data, serde_json::json!({}));
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn get_keeps_configured_retry_after_execute_failure() {
    let (url, requests, server) = spawn_recovering_execute_get_server();
    let configuration = ConfigurationRestApi::builder()
        .retries(1)
        .backoff(1)
        .agent(local_probe_agent())
        .build()
        .expect("build SDK transport config");
    let request = reqwest::Client::new()
        .request(Method::GET, url)
        .build()
        .expect("build read request");

    let result = http_request::<Value>(request, &configuration).await;

    server.join().expect("local SDK execute retry probe exits");
    let response = result.expect("GET execute failure retries once and recovers");
    let data = response.data().await.expect("parse recovered GET response");
    assert_eq!(data, serde_json::json!({}));
    assert_eq!(requests.load(Ordering::SeqCst), 2);
}
