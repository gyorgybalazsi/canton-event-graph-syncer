//! An `https://` ledger URL must connect over TLS, and an `http://` one in plaintext.
//!
//! Each test points a tonic endpoint at a local TCP listener and reads the first
//! bytes the client sends. A TLS ClientHello opens with the handshake record type
//! 0x16; plaintext HTTP/2 opens with the connection preface `PRI * HTTP/2.0`.
//! Without tonic's TLS features, the https connect fails before any byte is sent.

use std::time::Duration;
use tokio::io::AsyncReadExt;
use tokio::net::TcpListener;
use tonic::transport::Endpoint;

async fn first_bytes_sent(scheme: &str) -> Vec<u8> {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("{scheme}://localhost:{port}");
    let connect = tokio::spawn(async move {
        // The handshake can never finish against this listener; only the
        // first bytes matter, so the result is dropped.
        let _ = Endpoint::new(url)
            .unwrap()
            .connect_timeout(Duration::from_secs(5))
            .connect()
            .await;
    });
    let (mut socket, _) = tokio::time::timeout(Duration::from_secs(5), listener.accept())
        .await
        .expect("the client never opened a TCP connection")
        .unwrap();
    let mut buf = vec![0u8; 14];
    tokio::time::timeout(Duration::from_secs(5), socket.read_exact(&mut buf))
        .await
        .expect("the client sent nothing")
        .unwrap();
    connect.abort();
    buf
}

#[tokio::test]
async fn an_https_url_opens_with_a_tls_client_hello() {
    let bytes = first_bytes_sent("https").await;
    assert_eq!(
        bytes[0], 0x16,
        "expected a TLS handshake record, got {bytes:?}"
    );
}

#[tokio::test]
async fn an_http_url_stays_plaintext() {
    let bytes = first_bytes_sent("http").await;
    assert_eq!(&bytes, b"PRI * HTTP/2.0");
}
