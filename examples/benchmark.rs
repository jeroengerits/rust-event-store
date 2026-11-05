use clap::Parser;
use serde_json::{Value, json};
use std::{
    error::Error,
    io,
    time::{Duration, Instant},
};
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader},
    net::TcpStream,
    task::JoinSet,
    time::timeout,
};
use uuid::Uuid;

type Result<T> = std::result::Result<T, Box<dyn Error + Send + Sync>>;

#[derive(Parser, Debug, Clone)]
#[command(about = "HTTP event-store benchmark (writes persistent data)")]
struct Args {
    /// Host:port of an existing HTTP server; TLS and proxies are not supported.
    #[arg(long, default_value = "127.0.0.1:8080")]
    target: String,
    #[arg(long, env = "EVENTSTORE_API_KEY", hide_env_values = true)]
    api_key: Option<String>,
    /// Append requests per writer, for each append phase.
    #[arg(long, default_value_t = 100)]
    requests: usize,
    #[arg(long, default_value_t = 4)]
    concurrency: usize,
    #[arg(long, default_value_t = 10)]
    batch_size: usize,
    /// Number of ASCII bytes in each event's data.payload string.
    #[arg(long, default_value_t = 128)]
    payload_bytes: usize,
    #[arg(long, default_value_t = 100)]
    page_size: usize,
    #[arg(long, default_value_t = 10)]
    subscription_samples: usize,
    #[arg(long, default_value_t = 30)]
    timeout_seconds: u64,
}

fn err(message: impl Into<String>) -> Box<dyn Error + Send + Sync> {
    io::Error::other(message.into()).into()
}

fn headers(args: &Args, method: &str, path: &str, body: &[u8], close: bool) -> Result<String> {
    if args.target.contains(['\r', '\n'])
        || args
            .api_key
            .as_deref()
            .is_some_and(|s| s.contains(['\r', '\n']))
    {
        return Err(err("invalid HTTP header value"));
    }
    Ok(format!(
        "{method} {path} HTTP/1.1\r\nHost: {}\r\n{}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: {}\r\n\r\n",
        args.target,
        args.api_key
            .as_ref()
            .map(|v| format!("x-api-key: {v}\r\n"))
            .unwrap_or_default(),
        body.len(),
        if close { "close" } else { "keep-alive" }
    ))
}

async fn connect(
    args: &Args,
    method: &str,
    path: &str,
    body: &[u8],
    close: bool,
) -> Result<BufReader<TcpStream>> {
    let mut socket = TcpStream::connect(&args.target).await?;
    socket
        .write_all(headers(args, method, path, body, close)?.as_bytes())
        .await?;
    socket.write_all(body).await?;
    Ok(BufReader::new(socket))
}

async fn response_headers(reader: &mut BufReader<TcpStream>) -> Result<(u16, bool)> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Err(err("EOF before HTTP status"));
    }
    let status = line
        .split_whitespace()
        .nth(1)
        .ok_or_else(|| err("invalid HTTP status"))?
        .parse()?;
    let mut chunked = false;
    loop {
        line.clear();
        if reader.read_line(&mut line).await? == 0 {
            return Err(err("EOF in HTTP headers"));
        }
        if line == "\r\n" {
            break;
        }
        if line.to_ascii_lowercase().starts_with("transfer-encoding:")
            && line.to_ascii_lowercase().contains("chunked")
        {
            chunked = true;
        }
    }
    Ok((status, chunked))
}

// Axum sends SSE as HTTP/1.1 chunks. Decode chunks rather than assuming event boundaries
// coincide with network reads. The same decoder also handles chunked ordinary responses.
async fn chunk(reader: &mut BufReader<TcpStream>) -> Result<Vec<u8>> {
    let mut line = String::new();
    if reader.read_line(&mut line).await? == 0 {
        return Err(err("EOF in chunk header"));
    }
    let size = usize::from_str_radix(line.trim().split(';').next().unwrap_or(""), 16)?;
    if size == 0 {
        return Ok(Vec::new());
    }
    let mut bytes = vec![0; size];
    reader.read_exact(&mut bytes).await?;
    let mut crlf = [0; 2];
    reader.read_exact(&mut crlf).await?;
    if crlf != *b"\r\n" {
        return Err(err("invalid chunk terminator"));
    }
    Ok(bytes)
}

async fn request(args: &Args, method: &str, path: &str, body: &[u8]) -> Result<Value> {
    timeout(Duration::from_secs(args.timeout_seconds), async {
        let mut reader = connect(args, method, path, body, true).await?;
        let (status, chunked) = response_headers(&mut reader).await?;
        let mut bytes = Vec::new();
        if chunked {
            loop {
                let part = chunk(&mut reader).await?;
                if part.is_empty() {
                    break;
                }
                bytes.extend(part);
            }
        } else {
            reader.read_to_end(&mut bytes).await?;
        }
        if status != 200 {
            return Err(err(format!(
                "{method} {path}: HTTP {status}: {}",
                String::from_utf8_lossy(&bytes)
            )));
        }
        Ok(serde_json::from_slice(&bytes)?)
    })
    .await
    .map_err(|_| err(format!("{method} {path} timed out")))?
}

fn body(count: usize, payload: &str) -> Result<Vec<u8>> {
    let events: Vec<_> = (0..count).map(|_| json!({"id": Uuid::now_v7(), "type": "benchmark", "data": {"payload": payload}, "metadata": null})).collect();
    Ok(serde_json::to_vec(
        &json!({"expected_revision": null, "events": events}),
    )?)
}

fn report(name: &str, samples: &mut [Duration], elapsed: Duration, events: usize) {
    samples.sort_unstable();
    let percentile = |p: usize| {
        samples[((samples.len() * p).div_ceil(100)).saturating_sub(1)].as_secs_f64() * 1000.0
    };
    println!(
        "{name}: requests={} events={} elapsed_s={:.3} requests_per_s={:.2} events_per_s={:.2} latency_ms_p50={:.3} p95={:.3} p99={:.3}",
        samples.len(),
        events,
        elapsed.as_secs_f64(),
        samples.len() as f64 / elapsed.as_secs_f64(),
        events as f64 / elapsed.as_secs_f64(),
        percentile(50),
        percentile(95),
        percentile(99)
    );
}

async fn append_phase(
    args: &Args,
    run: &str,
    phase: &str,
    writers: usize,
    batch: usize,
    payload: &str,
) -> Result<Vec<String>> {
    let mut tasks = JoinSet::new();
    let mut streams = Vec::new();
    let start = Instant::now();
    for writer in 0..writers {
        let stream = format!("bench-{run}-{phase}-{writer}");
        streams.push(stream.clone());
        let args = args.clone();
        let payload = payload.to_owned();
        tasks.spawn(async move {
            let mut samples = Vec::new();
            for _ in 0..args.requests {
                let data = body(batch, &payload)?;
                let start = Instant::now();
                request(
                    &args,
                    "POST",
                    &format!("/v1/streams/{stream}/events"),
                    &data,
                )
                .await?;
                samples.push(start.elapsed());
            }
            Ok::<_, Box<dyn Error + Send + Sync>>(samples)
        });
    }
    let mut samples = Vec::new();
    while let Some(result) = tasks.join_next().await {
        samples.extend(result??);
    }
    report(
        phase,
        &mut samples,
        start.elapsed(),
        writers * args.requests * batch,
    );
    Ok(streams)
}

async fn read_phase(args: &Args, streams: &[String], global: bool) -> Result<()> {
    let start = Instant::now();
    let mut samples = Vec::new();
    let mut count = 0;
    let targets: Vec<&str> = if global {
        vec![""]
    } else {
        streams.iter().map(String::as_str).collect()
    };
    for stream in targets {
        let mut cursor = 0i64;
        loop {
            let path = if global {
                format!("/v1/events?after={cursor}&limit={}", args.page_size)
            } else {
                format!(
                    "/v1/streams/{stream}/events?from={cursor}&limit={}",
                    args.page_size
                )
            };
            let t = Instant::now();
            let response = request(args, "GET", &path, &[]).await?;
            samples.push(t.elapsed());
            let events = response["events"]
                .as_array()
                .ok_or_else(|| err("missing events array"))?;
            count += events.len();
            if events.is_empty() {
                break;
            }
            let next = response[if global {
                "next_after"
            } else {
                "next_revision"
            }]
            .as_i64()
            .ok_or_else(|| err("missing read cursor"))?;
            if next <= cursor {
                return Err(err("read cursor did not advance"));
            }
            cursor = next;
        }
    }
    report(
        if global { "global_read" } else { "stream_read" },
        &mut samples,
        start.elapsed(),
        count,
    );
    Ok(())
}

async fn subscription(args: &Args, run: &str, payload: &str) -> Result<()> {
    let stream = format!("bench-{run}-subscription");
    let path = format!("/v1/streams/{stream}/events");
    let baseline = request(args, "POST", &path, &body(1, payload)?).await?;
    let after = baseline["last_position"]
        .as_i64()
        .ok_or_else(|| err("missing last_position"))?;
    timeout(Duration::from_secs(args.timeout_seconds), async {
        let mut reader = connect(
            args,
            "GET",
            &format!("/v1/subscriptions?after={after}"),
            &[],
            false,
        )
        .await?;
        let (status, chunked) = response_headers(&mut reader).await?;
        if status != 200 || !chunked {
            return Err(err(format!(
                "subscription expected HTTP 200 chunked, got status {status}, chunked={chunked}"
            )));
        }
        let mut buffer = Vec::new();
        let mut samples = Vec::new();
        let phase_start = Instant::now();
        for _ in 0..args.subscription_samples {
            let previous_samples = samples.len();
            let data = body(1, payload)?;
            let id = serde_json::from_slice::<Value>(&data)?["events"][0]["id"]
                .as_str()
                .ok_or_else(|| err("missing event ID"))?
                .to_owned();
            let start = Instant::now();
            request(args, "POST", &path, &data).await?;
            loop {
                while let Some(end) = buffer.windows(2).position(|w| w == b"\n\n") {
                    let frame: Vec<u8> = buffer.drain(..end + 2).collect();
                    for line in frame.split(|b| *b == b'\n') {
                        if let Some(data) = line.strip_prefix(b"data: ") {
                            let event: Value = serde_json::from_slice(data)?;
                            if event["id"].as_str() == Some(&id) {
                                samples.push(start.elapsed());
                                break;
                            }
                        }
                    }
                }
                // The current sample is complete when its matching frame was seen.
                if samples.len() > previous_samples {
                    break;
                }
                let part = chunk(&mut reader).await?;
                if part.is_empty() {
                    return Err(err("subscription ended"));
                }
                buffer.extend(part);
            }
        }
        report(
            "subscription_delivery",
            &mut samples,
            phase_start.elapsed(),
            args.subscription_samples,
        );
        Ok(())
    })
    .await
    .map_err(|_| err("subscription phase timed out"))?
}

#[tokio::main]
async fn main() -> Result<()> {
    let args = Args::parse();
    if args.requests == 0
        || args.concurrency == 0
        || args.batch_size == 0
        || args.subscription_samples == 0
        || !(1..=1000).contains(&args.page_size)
        || args.timeout_seconds == 0
    {
        return Err(err(
            "counts and timeout must be positive; page size must be 1..=1000",
        ));
    }
    let run = Uuid::now_v7().to_string();
    let payload = "x".repeat(args.payload_bytes);
    println!(
        "run={run} target={} requests_per_writer={} concurrency={} batch_size={} payload_bytes={} page_size={} subscription_samples={} timeout_seconds={} auth={}",
        args.target,
        args.requests,
        args.concurrency,
        args.batch_size,
        args.payload_bytes,
        args.page_size,
        args.subscription_samples,
        args.timeout_seconds,
        args.api_key.is_some()
    );
    append_phase(&args, &run, "single_append", 1, 1, &payload).await?;
    append_phase(&args, &run, "batch_append", 1, args.batch_size, &payload).await?;
    let streams = append_phase(
        &args,
        &run,
        "concurrent_append",
        args.concurrency,
        args.batch_size,
        &payload,
    )
    .await?;
    read_phase(&args, &streams, false).await?;
    read_phase(&args, &[], true).await?;
    subscription(&args, &run, &payload).await?;
    Ok(())
}
