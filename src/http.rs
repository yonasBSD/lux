use bytes::BytesMut;
use std::sync::Arc;
use std::time::Instant;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpSocket;

use crate::cmd;
use crate::pubsub::Broker;
use crate::store::Store;

pub async fn start_http_server(
    port: u16,
    store: Arc<Store>,
    broker: Broker,
) -> std::io::Result<()> {
    let addr: std::net::SocketAddr = format!("0.0.0.0:{port}").parse().unwrap();
    let socket = TcpSocket::new_v4()?;
    socket.set_reuseaddr(true)?;
    socket.bind(addr)?;
    let listener = socket.listen(1024)?;
    println!("lux http api ready on 0.0.0.0:{port}");

    loop {
        let (mut socket, _) = listener.accept().await?;
        let store = store.clone();
        let broker = broker.clone();

        tokio::spawn(async move {
            loop {
                match handle_request(&mut socket, &store, &broker).await {
                    Ok(true) => continue,
                    _ => break,
                }
            }
        });
    }
}

async fn handle_request(
    socket: &mut tokio::net::TcpStream,
    store: &Arc<Store>,
    broker: &Broker,
) -> std::io::Result<bool> {
    let mut buf = vec![0u8; 65536];
    let mut data = Vec::new();

    loop {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            return Ok(false);
        }
        data.extend_from_slice(&buf[..n]);

        if data.windows(4).any(|w| w == b"\r\n\r\n") {
            break;
        }
    }

    let header_end = data.windows(4).position(|w| w == b"\r\n\r\n").unwrap() + 4;
    let header_str = String::from_utf8_lossy(&data[..header_end]);

    let content_length: usize = header_str
        .lines()
        .find(|l| l.to_ascii_lowercase().starts_with("content-length:"))
        .and_then(|l| l.split_once(':'))
        .and_then(|(_, v)| v.trim().parse().ok())
        .unwrap_or(0);

    let total_needed = header_end + content_length;
    while data.len() < total_needed {
        let n = socket.read(&mut buf).await?;
        if n == 0 {
            break;
        }
        data.extend_from_slice(&buf[..n]);
    }

    let request = String::from_utf8_lossy(&data);

    let (method, full_path, headers, body) = parse_http_request(&request);

    if method == "OPTIONS" {
        let response = format!(
            "HTTP/1.1 204 No Content\r\n\
             Access-Control-Allow-Origin: *\r\n\
             Access-Control-Allow-Methods: GET, POST, PUT, DELETE, OPTIONS\r\n\
             Access-Control-Allow-Headers: Authorization, Content-Type\r\n\
             Content-Length: 0\r\n\r\n"
        );
        socket.write_all(response.as_bytes()).await?;
        return Ok(true);
    }

    let password = std::env::var("LUX_PASSWORD").unwrap_or_default();
    if !password.is_empty() {
        let auth = headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case("authorization"))
            .map(|(_, v)| v.as_str())
            .unwrap_or("");

        let token = auth.strip_prefix("Bearer ").unwrap_or("");
        if token != password {
            let body = r#"{"error":"unauthorized"}"#;
            return send_json(socket, 401, "Unauthorized", body).await;
        }
    }

    let (path, query_string) = match full_path.split_once('?') {
        Some((p, q)) => (p.to_string(), q.to_string()),
        None => (full_path.clone(), String::new()),
    };
    let params = parse_query_string(&query_string);

    let (status, status_text, result) =
        route_request(&method, &path, &body, &params, store, broker);

    send_json(socket, status, status_text, &result).await
}

async fn send_json(
    socket: &mut tokio::net::TcpStream,
    status: u16,
    status_text: &str,
    body: &str,
) -> std::io::Result<bool> {
    let response = format!(
        "HTTP/1.1 {status} {status_text}\r\n\
         Content-Type: application/json\r\n\
         Access-Control-Allow-Origin: *\r\n\
         Content-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    socket.write_all(response.as_bytes()).await?;
    Ok(true)
}

fn parse_http_request(raw: &str) -> (String, String, Vec<(String, String)>, String) {
    let parts: Vec<&str> = raw.splitn(2, "\r\n\r\n").collect();
    let header_section = parts[0];
    let body = parts.get(1).unwrap_or(&"").to_string();

    let mut lines = header_section.lines();
    let request_line = lines.next().unwrap_or("");
    let mut tokens = request_line.split_whitespace();
    let method = tokens.next().unwrap_or("GET").to_string();
    let path = tokens.next().unwrap_or("/").to_string();

    let mut headers = Vec::new();
    for line in lines {
        if let Some((k, v)) = line.split_once(':') {
            headers.push((k.trim().to_string(), v.trim().to_string()));
        }
    }

    (method, path, headers, body)
}

fn parse_query_string(qs: &str) -> Vec<(String, String)> {
    if qs.is_empty() {
        return Vec::new();
    }
    qs.split('&')
        .filter_map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            let k = url_decode(k);
            let v = url_decode(v);
            if k.is_empty() {
                None
            } else {
                Some((k, v))
            }
        })
        .collect()
}

fn url_decode(s: &str) -> String {
    let mut result = Vec::with_capacity(s.len());
    let bytes = s.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b'%' && i + 2 < bytes.len() {
            if let Ok(byte) =
                u8::from_str_radix(std::str::from_utf8(&bytes[i + 1..i + 3]).unwrap_or(""), 16)
            {
                result.push(byte);
                i += 3;
                continue;
            }
        } else if bytes[i] == b'+' {
            result.push(b' ');
            i += 1;
            continue;
        }
        result.push(bytes[i]);
        i += 1;
    }
    String::from_utf8_lossy(&result).to_string()
}

fn get_param<'a>(params: &'a [(String, String)], key: &str) -> Option<&'a str> {
    params
        .iter()
        .find(|(k, _)| k == key)
        .map(|(_, v)| v.as_str())
}

fn route_request(
    method: &str,
    path: &str,
    body: &str,
    params: &[(String, String)],
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let path = path.trim_start_matches('/');
    let segments: Vec<&str> = path.split('/').filter(|s| !s.is_empty()).collect();

    if segments.is_empty() || (segments.len() == 1 && segments[0] == "v1") {
        return (
            200,
            "OK",
            r#"{"lux":"ok","version":""#.to_string() + env!("CARGO_PKG_VERSION") + r#""}"#,
        );
    }

    let base = if segments[0] == "v1" {
        &segments[1..]
    } else {
        &segments[..]
    };

    match (method, base) {
        // ── exec (escape hatch) ──
        ("POST", ["exec"]) => ok(handle_exec(body, store, broker)),

        // ── KV routes ──
        ("GET", ["kv", key]) => ok(exec_simple(store, broker, &["GET", key])),
        ("PUT", ["kv", key]) => {
            let parsed: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
            let value = parsed["value"].as_str().unwrap_or("");
            if let Some(ex) = parsed["ex"].as_u64() {
                ok(exec_simple(
                    store,
                    broker,
                    &["SET", key, value, "EX", &ex.to_string()],
                ))
            } else {
                ok(exec_simple(store, broker, &["SET", key, value]))
            }
        }
        ("DELETE", ["kv", key]) => ok(exec_simple(store, broker, &["DEL", key])),
        ("POST", ["kv", key, "incr"]) => ok(exec_simple(store, broker, &["INCR", key])),
        ("POST", ["kv", key, "decr"]) => ok(exec_simple(store, broker, &["DECR", key])),
        ("GET", ["kv", key, "hash"]) => ok(exec_simple(store, broker, &["HGETALL", key])),
        ("GET", ["kv", key, "list"]) => {
            let start = get_param(params, "start").unwrap_or("0");
            let stop = get_param(params, "stop").unwrap_or("-1");
            ok(exec_simple(store, broker, &["LRANGE", key, start, stop]))
        }
        ("GET", ["kv", key, "set"]) => ok(exec_simple(store, broker, &["SMEMBERS", key])),
        ("GET", ["kv", key, "zset"]) => {
            let min = get_param(params, "min").unwrap_or("-inf");
            let max = get_param(params, "max").unwrap_or("+inf");
            ok(exec_simple(
                store,
                broker,
                &["ZRANGEBYSCORE", key, min, max, "WITHSCORES"],
            ))
        }
        ("GET", ["keys"]) => {
            let pattern = get_param(params, "pattern").unwrap_or("*");
            ok(exec_simple(store, broker, &["KEYS", pattern]))
        }
        ("GET", ["dbsize"]) => ok(exec_simple(store, broker, &["DBSIZE"])),
        ("GET", ["ping"]) => ok(exec_simple(store, broker, &["PING"])),

        // ── Table routes (PostgREST-style) ──
        ("GET", ["tables"]) => ok(exec_simple(store, broker, &["TLIST"])),
        ("POST", ["tables"]) => route_table_create(body, store, broker),
        ("GET", ["tables", table]) => route_table_query(table, params, store, broker),
        ("GET", ["tables", table, "schema"]) => ok(exec_simple(store, broker, &["TSCHEMA", table])),
        ("GET", ["tables", table, "count"]) => ok(exec_simple(store, broker, &["TCOUNT", table])),
        ("GET", ["tables", table, id]) => ok(exec_simple(store, broker, &["TGET", table, id])),
        ("POST", ["tables", table]) => route_table_insert(table, body, store, broker),
        ("PUT", ["tables", table, id]) => route_table_update(table, id, body, store, broker),
        ("DELETE", ["tables", table, id]) => ok(exec_simple(store, broker, &["TDEL", table, id])),
        ("DELETE", ["tables", table]) => ok(exec_simple(store, broker, &["TDROP", table])),

        // ── Time Series routes ──
        ("GET", ["ts"]) => {
            let filter = get_param(params, "filter").unwrap_or("");
            if filter.is_empty() {
                (
                    400,
                    "Bad Request",
                    r#"{"error":"filter parameter required"}"#.to_string(),
                )
            } else {
                let mut args = vec!["TSMRANGE", "-", "+", "FILTER", filter];
                if let Some(agg) = get_param(params, "agg") {
                    if let Some(bucket) = get_param(params, "bucket") {
                        args.push("AGGREGATION");
                        args.push(agg);
                        args.push(bucket);
                    }
                }
                ok(exec_simple(store, broker, &args))
            }
        }
        ("GET", ["ts", key]) => route_ts_range(key, params, store, broker),
        ("POST", ["ts", key]) => route_ts_add(key, body, store, broker),
        ("GET", ["ts", key, "info"]) => ok(exec_simple(store, broker, &["TSINFO", key])),
        ("GET", ["ts", key, "latest"]) => ok(exec_simple(store, broker, &["TSGET", key])),

        // ── Vector routes ──
        ("POST", ["vectors", "search"]) => route_vector_search(body, store, broker),
        ("POST", ["vectors", key]) => route_vector_set(key, body, store, broker),
        ("GET", ["vectors", key]) => ok(exec_simple(store, broker, &["VGET", key])),
        ("DELETE", ["vectors", key]) => ok(exec_simple(store, broker, &["DEL", key])),
        ("GET", ["vectors"]) => ok(exec_simple(store, broker, &["VCARD"])),

        // ── Legacy flat routes (backwards compat) ──
        ("GET", ["get", key]) => ok(exec_simple(store, broker, &["GET", key])),
        ("POST", ["set", key]) => {
            let parsed: serde_json::Value = serde_json::from_str(body).unwrap_or_default();
            let value = parsed["value"].as_str().unwrap_or("");
            if let Some(ex) = parsed["ex"].as_u64() {
                ok(exec_simple(
                    store,
                    broker,
                    &["SET", key, value, "EX", &ex.to_string()],
                ))
            } else {
                ok(exec_simple(store, broker, &["SET", key, value]))
            }
        }
        ("POST", ["del", key]) => ok(exec_simple(store, broker, &["DEL", key])),
        ("POST", ["incr", key]) => ok(exec_simple(store, broker, &["INCR", key])),
        ("POST", ["decr", key]) => ok(exec_simple(store, broker, &["DECR", key])),
        ("GET", ["hgetall", key]) => ok(exec_simple(store, broker, &["HGETALL", key])),
        ("GET", ["keys", pattern]) => ok(exec_simple(store, broker, &["KEYS", pattern])),

        _ => (404, "Not Found", r#"{"error":"not found"}"#.to_string()),
    }
}

fn ok(result: String) -> (u16, &'static str, String) {
    (200, "OK", result)
}

// ── Table handlers ──

fn route_table_create(
    body: &str,
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return (
                400,
                "Bad Request",
                r#"{"error":"invalid json"}"#.to_string(),
            )
        }
    };

    let name = match parsed["name"].as_str() {
        Some(n) => n,
        None => {
            return (
                400,
                "Bad Request",
                r#"{"error":"missing name"}"#.to_string(),
            )
        }
    };

    let columns = match parsed["columns"].as_array() {
        Some(cols) => cols,
        None => {
            return (
                400,
                "Bad Request",
                r#"{"error":"missing columns array"}"#.to_string(),
            )
        }
    };

    let mut args: Vec<String> = vec!["TCREATE".to_string(), name.to_string()];
    for col in columns {
        if let Some(s) = col.as_str() {
            args.push(s.to_string());
        } else if let Some(obj) = col.as_object() {
            let col_name = obj.get("name").and_then(|v| v.as_str()).unwrap_or("");
            let col_type = obj.get("type").and_then(|v| v.as_str()).unwrap_or("str");
            let unique = obj.get("unique").and_then(|v| v.as_bool()).unwrap_or(false);
            if unique {
                args.push(format!("{col_name}:{col_type}:unique"));
            } else {
                args.push(format!("{col_name}:{col_type}"));
            }
        }
    }

    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    ok(exec_simple(store, broker, &refs))
}

fn route_table_query(
    table: &str,
    params: &[(String, String)],
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let mut args: Vec<String> = vec!["TQUERY".to_string(), table.to_string()];

    if let Some(where_clause) = get_param(params, "where") {
        args.push("WHERE".to_string());
        for part in where_clause.split_whitespace() {
            args.push(part.to_string());
        }
    }

    if let Some(join) = get_param(params, "join") {
        args.push("JOIN".to_string());
        args.push(join.to_string());
    }

    if let Some(order) = get_param(params, "order") {
        args.push("ORDER".to_string());
        args.push("BY".to_string());
        for part in order.split_whitespace() {
            args.push(part.to_string());
        }
    }

    if let Some(limit) = get_param(params, "limit") {
        args.push("LIMIT".to_string());
        args.push(limit.to_string());
    }

    if let Some(offset) = get_param(params, "offset") {
        args.push("OFFSET".to_string());
        args.push(offset.to_string());
    }

    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    ok(exec_simple(store, broker, &refs))
}

fn route_table_insert(
    table: &str,
    body: &str,
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return (
                400,
                "Bad Request",
                r#"{"error":"invalid json"}"#.to_string(),
            )
        }
    };

    let obj = match parsed.as_object() {
        Some(o) => o,
        None => {
            return (
                400,
                "Bad Request",
                r#"{"error":"expected json object"}"#.to_string(),
            )
        }
    };

    let mut args: Vec<String> = vec!["TINSERT".to_string(), table.to_string()];
    for (k, v) in obj {
        args.push(k.clone());
        match v {
            serde_json::Value::String(s) => args.push(s.clone()),
            serde_json::Value::Number(n) => args.push(n.to_string()),
            serde_json::Value::Bool(b) => args.push(b.to_string()),
            serde_json::Value::Null => args.push("*".to_string()),
            _ => args.push(v.to_string()),
        }
    }

    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    ok(exec_simple(store, broker, &refs))
}

fn route_table_update(
    table: &str,
    id: &str,
    body: &str,
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return (
                400,
                "Bad Request",
                r#"{"error":"invalid json"}"#.to_string(),
            )
        }
    };

    let obj = match parsed.as_object() {
        Some(o) => o,
        None => {
            return (
                400,
                "Bad Request",
                r#"{"error":"expected json object"}"#.to_string(),
            )
        }
    };

    let mut args: Vec<String> = vec!["TUPDATE".to_string(), table.to_string(), id.to_string()];
    for (k, v) in obj {
        args.push(k.clone());
        match v {
            serde_json::Value::String(s) => args.push(s.clone()),
            serde_json::Value::Number(n) => args.push(n.to_string()),
            serde_json::Value::Bool(b) => args.push(b.to_string()),
            _ => args.push(v.to_string()),
        }
    }

    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    ok(exec_simple(store, broker, &refs))
}

// ── Time Series handlers ──

fn route_ts_range(
    key: &str,
    params: &[(String, String)],
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let from = get_param(params, "from").unwrap_or("-");
    let to = get_param(params, "to").unwrap_or("+");

    let mut args: Vec<String> = vec![
        "TSRANGE".to_string(),
        key.to_string(),
        from.to_string(),
        to.to_string(),
    ];

    if let Some(agg) = get_param(params, "agg") {
        if let Some(bucket) = get_param(params, "bucket") {
            args.push("AGGREGATION".to_string());
            args.push(agg.to_string());
            args.push(bucket.to_string());
        }
    }

    if let Some(count) = get_param(params, "count") {
        args.push("COUNT".to_string());
        args.push(count.to_string());
    }

    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    ok(exec_simple(store, broker, &refs))
}

fn route_ts_add(
    key: &str,
    body: &str,
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return (
                400,
                "Bad Request",
                r#"{"error":"invalid json"}"#.to_string(),
            )
        }
    };

    let timestamp = parsed["timestamp"].as_str().unwrap_or("*").to_string();
    let value = match parsed.get("value") {
        Some(serde_json::Value::Number(n)) => n.to_string(),
        Some(serde_json::Value::String(s)) => s.clone(),
        _ => {
            return (
                400,
                "Bad Request",
                r#"{"error":"missing value"}"#.to_string(),
            )
        }
    };

    let mut args: Vec<String> = vec!["TSADD".to_string(), key.to_string(), timestamp, value];

    if let Some(retention) = parsed.get("retention").and_then(|v| v.as_u64()) {
        args.push("RETENTION".to_string());
        args.push(retention.to_string());
    }

    if let Some(labels) = parsed.get("labels").and_then(|v| v.as_object()) {
        args.push("LABELS".to_string());
        for (k, v) in labels {
            args.push(k.clone());
            args.push(v.as_str().unwrap_or("").to_string());
        }
    }

    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    ok(exec_simple(store, broker, &refs))
}

// ── Vector handlers ──

fn route_vector_set(
    key: &str,
    body: &str,
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return (
                400,
                "Bad Request",
                r#"{"error":"invalid json"}"#.to_string(),
            )
        }
    };

    let vector = match parsed.get("vector").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => {
            return (
                400,
                "Bad Request",
                r#"{"error":"missing vector array"}"#.to_string(),
            )
        }
    };

    let dim = vector.len().to_string();
    let mut args: Vec<String> = vec!["VSET".to_string(), key.to_string(), dim];
    for v in vector {
        args.push(v.as_f64().unwrap_or(0.0).to_string());
    }

    if let Some(meta) = parsed.get("metadata") {
        args.push("META".to_string());
        args.push(meta.to_string());
    }

    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    ok(exec_simple(store, broker, &refs))
}

fn route_vector_search(
    body: &str,
    store: &Arc<Store>,
    broker: &Broker,
) -> (u16, &'static str, String) {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => {
            return (
                400,
                "Bad Request",
                r#"{"error":"invalid json"}"#.to_string(),
            )
        }
    };

    let vector = match parsed.get("vector").and_then(|v| v.as_array()) {
        Some(arr) => arr,
        None => {
            return (
                400,
                "Bad Request",
                r#"{"error":"missing vector array"}"#.to_string(),
            )
        }
    };

    let k = parsed.get("k").and_then(|v| v.as_u64()).unwrap_or(10);
    let dim = vector.len().to_string();

    let mut args: Vec<String> = vec!["VSEARCH".to_string(), dim.clone()];
    for v in vector {
        args.push(v.as_f64().unwrap_or(0.0).to_string());
    }
    args.push("K".to_string());
    args.push(k.to_string());

    if let Some(filter_field) = parsed.get("filter").and_then(|v| v.as_str()) {
        if let Some(filter_val) = parsed.get("filter_value").and_then(|v| v.as_str()) {
            args.push("FILTER".to_string());
            args.push(filter_field.to_string());
            args.push(filter_val.to_string());
        }
    }

    args.push("META".to_string());

    let refs: Vec<&str> = args.iter().map(|s| s.as_str()).collect();
    ok(exec_simple(store, broker, &refs))
}

// ── Command execution ──

fn handle_exec(body: &str, store: &Arc<Store>, broker: &Broker) -> String {
    let parsed: serde_json::Value = match serde_json::from_str(body) {
        Ok(v) => v,
        Err(_) => return r#"{"error":"invalid json"}"#.to_string(),
    };

    let command = match parsed.get("command") {
        Some(serde_json::Value::Array(arr)) => arr
            .iter()
            .map(|v| v.as_str().unwrap_or("").to_string())
            .collect::<Vec<_>>(),
        Some(serde_json::Value::String(s)) => s.split_whitespace().map(String::from).collect(),
        _ => return r#"{"error":"missing command"}"#.to_string(),
    };

    if command.is_empty() {
        return r#"{"error":"empty command"}"#.to_string();
    }

    exec_simple(
        store,
        broker,
        &command.iter().map(|s| s.as_str()).collect::<Vec<_>>(),
    )
}

fn exec_simple(store: &Arc<Store>, broker: &Broker, args: &[&str]) -> String {
    let arg_bytes: Vec<&[u8]> = args.iter().map(|s| s.as_bytes() as &[u8]).collect();
    let mut out = BytesMut::with_capacity(1024);
    let now = Instant::now();

    let _guard = crate::SCRIPT_GATE.read();
    let result = cmd::execute(store, broker, &arg_bytes, &mut out, now);

    match result {
        cmd::CmdResult::Written => resp_to_json(&out),
        cmd::CmdResult::Authenticated => r#"{"result":"OK"}"#.to_string(),
        cmd::CmdResult::Publish { channel, message } => {
            let count = broker.publish(&channel, message);
            format!(r#"{{"result":{count}}}"#)
        }
        _ => r#"{"result":"OK"}"#.to_string(),
    }
}

// ── RESP to JSON translation ──

fn resp_to_json(buf: &[u8]) -> String {
    let s = std::str::from_utf8(buf).unwrap_or("");
    if s.is_empty() {
        return r#"{"result":null}"#.to_string();
    }

    match s.as_bytes()[0] {
        b'+' => {
            let val = s[1..].trim_end_matches("\r\n");
            format!(r#"{{"result":"{}"}}"#, escape_json(val))
        }
        b'-' => {
            let val = s[1..].trim_end_matches("\r\n");
            format!(r#"{{"error":"{}"}}"#, escape_json(val))
        }
        b':' => {
            let val = s[1..].trim_end_matches("\r\n");
            format!(r#"{{"result":{val}}}"#)
        }
        b'$' => {
            let nl = s.find("\r\n").unwrap_or(s.len());
            let len: i64 = s[1..nl].parse().unwrap_or(-1);
            if len < 0 {
                r#"{"result":null}"#.to_string()
            } else {
                let start = nl + 2;
                let end = start + len as usize;
                let val = &s[start..end.min(s.len())];
                format!(r#"{{"result":"{}"}}"#, escape_json(val))
            }
        }
        b'*' => {
            let parsed = parse_resp_array(s);
            format!(r#"{{"result":{}}}"#, parsed)
        }
        _ => {
            format!(r#"{{"result":"{}"}}"#, escape_json(s.trim()))
        }
    }
}

fn parse_resp_array(s: &str) -> String {
    let nl = match s.find("\r\n") {
        Some(i) => i,
        None => return "[]".to_string(),
    };
    let count: i64 = s[1..nl].parse().unwrap_or(-1);
    if count < 0 {
        return "null".to_string();
    }
    if count == 0 {
        return "[]".to_string();
    }

    let mut items = Vec::new();
    let mut pos = nl + 2;
    let bytes = s.as_bytes();

    for _ in 0..count {
        if pos >= bytes.len() {
            break;
        }
        match bytes[pos] {
            b'$' => {
                let end = find_crlf(s, pos);
                let len: i64 = s[pos + 1..end].parse().unwrap_or(-1);
                if len < 0 {
                    items.push("null".to_string());
                    pos = end + 2;
                } else {
                    let start = end + 2;
                    let val_end = start + len as usize;
                    let val = &s[start..val_end.min(s.len())];
                    items.push(format!(r#""{}""#, escape_json(val)));
                    pos = val_end + 2;
                }
            }
            b':' => {
                let end = find_crlf(s, pos);
                let val = &s[pos + 1..end];
                items.push(val.to_string());
                pos = end + 2;
            }
            b'+' => {
                let end = find_crlf(s, pos);
                let val = &s[pos + 1..end];
                items.push(format!(r#""{}""#, escape_json(val)));
                pos = end + 2;
            }
            b'-' => {
                let end = find_crlf(s, pos);
                let val = &s[pos + 1..end];
                items.push(format!(r#""{}""#, escape_json(val)));
                pos = end + 2;
            }
            b'*' => {
                let sub = &s[pos..];
                let parsed = parse_resp_array(sub);
                items.push(parsed);
                pos += skip_resp_element(sub);
            }
            _ => {
                let end = find_crlf(s, pos);
                let val = &s[pos..end];
                items.push(format!(r#""{}""#, escape_json(val)));
                pos = end + 2;
            }
        }
    }

    format!("[{}]", items.join(","))
}

fn find_crlf(s: &str, from: usize) -> usize {
    s[from..].find("\r\n").map(|i| from + i).unwrap_or(s.len())
}

fn skip_resp_element(s: &str) -> usize {
    if s.is_empty() {
        return 0;
    }
    match s.as_bytes()[0] {
        b'$' => {
            let nl = find_crlf(s, 0);
            let len: i64 = s[1..nl].parse().unwrap_or(-1);
            if len < 0 {
                nl + 2
            } else {
                nl + 2 + len as usize + 2
            }
        }
        b':' | b'+' | b'-' => {
            let nl = find_crlf(s, 0);
            nl + 2
        }
        b'*' => {
            let nl = find_crlf(s, 0);
            let count: i64 = s[1..nl].parse().unwrap_or(-1);
            let mut pos = nl + 2;
            for _ in 0..count.max(0) {
                pos += skip_resp_element(&s[pos..]);
            }
            pos
        }
        _ => {
            let nl = find_crlf(s, 0);
            nl + 2
        }
    }
}

fn escape_json(s: &str) -> String {
    s.replace('\\', "\\\\")
        .replace('"', "\\\"")
        .replace('\n', "\\n")
        .replace('\r', "\\r")
        .replace('\t', "\\t")
}
