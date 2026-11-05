# Benchmarking

`examples/benchmark.rs` is a small HTTP load generator for an **already running** event-store server. It creates persistent events and does not delete them. Use a disposable, isolated database, not production data. It does not configure or start the server. The client supports direct HTTP/1.1 only (no TLS, proxy, or URL path prefix). All errors, non-200 responses and timeouts abort the run; partial runs still leave data behind.

## Run

Build once to keep compilation out of the measurements, then run against your chosen server:

```sh
cargo build --release --example benchmark
./target/release/examples/benchmark --target 127.0.0.1:8080 \
  --requests 100 --concurrency 4 --batch-size 10 --payload-bytes 128 \
  --page-size 100 --subscription-samples 10 --timeout-seconds 30
```

For an authenticated server, set `EVENTSTORE_API_KEY` in the client's environment; the value is not printed. Run `./target/release/examples/benchmark --help` for options. Defaults are shown above. The target is `host:port`, not a URL. Configure the server's maximum batch, event and request-body sizes to admit the chosen workload; the default server maximum batch size is 100. The payload size is the number of ASCII bytes in `data.payload`, **not** the serialized event or request size. Each event additionally has an ID, type, JSON framing and metadata.

Each run prints its unique run ID and client settings, then:

* `single_append`: one writer, one event per request.
* `batch_append`: one writer, configured batch size per request.
* `concurrent_append`: configured writers, each issuing requests sequentially to a separate new stream, with configured batch size. This is independent-stream contention, not same-stream optimistic-concurrency testing. All append requests use `expected_revision: null`. Each phase sends `requests` requests per writer; events have unique UUIDv7 IDs.
* `stream_read`: sequentially pages the concurrent phase's streams from revision zero until an empty page, including that empty page in the request count.
* `global_read`: sequentially pages the **entire database** from position zero until an empty page, including existing and benchmark events. Run with no other writers; a changing global feed changes the count and can prevent completion. The subscription events are appended after this read.
* `subscription_delivery`: writes one baseline event, subscribes after its position, then sequentially appends and waits for the matching event by ID. Timing begins immediately before the HTTP append and ends when the client parses the SSE frame, after it has read the append response. Thus this is an end-to-end upper bound on what this particular client observes, not commit-to-notification latency; the event may already be buffered when the response finishes. No parallel subscription load is generated.

For each phase the tool reports wall-clock elapsed seconds, requests/s, events/s and nearest-rank p50/p95/p99 request latency in milliseconds. For subscription, “requests” means measured append/delivery samples. Append latency includes TCP connection establishment, server work, response transfer and JSON parsing, but excludes event/JSON creation. Every normal request opens a new connection; SSE uses one persistent HTTP/1.1 chunked connection. Phase throughput includes client-side event creation, scheduling, and between-request overhead, while latency does not. Read latency includes parsing the complete JSON response. Counts are successful operations only; an error aborts rather than producing a misleading success rate. `--timeout-seconds` applies to each normal request and to the **whole** subscription phase including setup, so increase it for large sample counts.

## Reproducibility and interpretation

Record the exact command and output, git revision, release/debug build, hardware and OS, client/server placement and network, server configuration (including logging level, limits and authentication), SQLite database path and storage medium, existing database size, page cache state, and other load. Use the same server build and settings when comparing. The server implementation currently configures SQLite DELETE journaling with synchronous FULL; record the actual deployed version and settings rather than assuming they match. These timings include the server's configured commit path but **do not independently verify fsync, crash recovery, or physical durability**. Storage hardware, filesystem and virtualization can affect durability and results. Do not compare them directly to a benchmark using weaker durability settings.

Use a fresh database for each comparable run, or explicitly report its initial contents; this matters especially for global reads and indexes. Warm-up and measured runs should be separate, with the same described procedure for every comparison. Repeat runs and report variation, rather than treating a single percentile or throughput number as definitive. The phases run in a fixed order, so later phases see a larger, warmer database. Single and batch results also differ in events per request; compare both requests/s and events/s. The client and server share CPU and memory when run on the same machine, and logging may dominate small operations. This is a closed-loop generator (each writer waits for a response), not a fixed-rate or saturation-independent latency test; it can hide queueing under overload. There is no retry, correctness audit, percentile confidence interval, or durability test.

## Memory methodology

The tool does **not** measure memory. Measure the server process separately, not the benchmark client. For example on Linux, sample `ps -o pid,rss,vsz,etime -p <server-pid>` before load, during each phase, and after load, or use the service's cgroup `memory.current`/`memory.peak` where available. On macOS use `ps -o pid,rss,vsz,etime -p <server-pid>` or Activity Monitor. RSS is resident process memory, not heap allocation or total system cache; cgroup accounting may include filesystem cache and other processes. Record sampling interval, baseline, peak, platform, and whether client and server share a host. Page cache, database and journal activity, allocator retention, and concurrent processes complicate attribution. For phase-specific peaks, use an external timestamped sampler or run controlled repetitions; do not infer per-event memory usage from RSS alone.
