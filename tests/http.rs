use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn find_lux_binary() -> Option<std::path::PathBuf> {
    let exe = std::env::current_exe().ok()?;
    let target_dir = exe.parent()?.parent()?.parent()?;
    let release = target_dir.join("release").join("lux");
    if release.exists() {
        return Some(release);
    }
    let debug = target_dir.join("debug").join("lux");
    if debug.exists() {
        return Some(debug);
    }
    None
}

struct LuxServer {
    child: std::process::Child,
    tmpdir: std::path::PathBuf,
}

impl Drop for LuxServer {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
        let _ = std::fs::remove_dir_all(&self.tmpdir);
    }
}

fn start_lux(resp_port: u16, http_port: u16, password: &str) -> LuxServer {
    let bin = find_lux_binary().expect("no lux binary found");
    let tmpdir = std::env::temp_dir().join(format!(
        "lux_http_test_{}_{}",
        std::process::id(),
        http_port
    ));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let mut cmd = std::process::Command::new(&bin);
    cmd.env("LUX_PORT", resp_port.to_string())
        .env("LUX_HTTP_PORT", http_port.to_string())
        .env("LUX_SHARDS", "4")
        .env("LUX_SAVE_INTERVAL", "0")
        .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null());

    if !password.is_empty() {
        cmd.env("LUX_PASSWORD", password);
    }

    let child = cmd.spawn().expect("failed to start lux");
    let server = LuxServer { child, tmpdir };

    for _ in 0..40 {
        if TcpStream::connect(format!("127.0.0.1:{http_port}")).is_ok() {
            return server;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("lux http did not start on port {http_port}");
}

fn http_request(
    port: u16,
    method: &str,
    path: &str,
    body: Option<&str>,
    auth: Option<&str>,
) -> (u16, String) {
    let mut stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_secs(5)))
        .unwrap();

    let body_str = body.unwrap_or("");
    let mut req = format!(
        "{method} {path} HTTP/1.1\r\nHost: localhost\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
        body_str.len()
    );
    if let Some(token) = auth {
        req.push_str(&format!("Authorization: Bearer {token}\r\n"));
    }
    req.push_str("\r\n");
    req.push_str(body_str);

    stream.write_all(req.as_bytes()).unwrap();

    let mut response = Vec::new();
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(n) => response.extend_from_slice(&buf[..n]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(_) => break,
        }
    }

    let resp = String::from_utf8_lossy(&response).to_string();
    let status = resp
        .lines()
        .next()
        .and_then(|l| l.split_whitespace().nth(1))
        .and_then(|s| s.parse().ok())
        .unwrap_or(0);

    let body = resp.splitn(2, "\r\n\r\n").nth(1).unwrap_or("").to_string();
    (status, body)
}

fn get(port: u16, path: &str, auth: &str) -> String {
    http_request(port, "GET", path, None, Some(auth)).1
}

fn post(port: u16, path: &str, body: &str, auth: &str) -> String {
    http_request(port, "POST", path, Some(body), Some(auth)).1
}

fn put(port: u16, path: &str, body: &str, auth: &str) -> String {
    http_request(port, "PUT", path, Some(body), Some(auth)).1
}

fn delete(port: u16, path: &str, auth: &str) -> (u16, String) {
    http_request(port, "DELETE", path, None, Some(auth))
}

#[test]
fn http_health_check() {
    let _server = start_lux(17600, 17601, "");
    let resp = get(17601, "/v1", "");
    assert!(resp.contains("\"lux\""), "health: {resp}");
    assert!(resp.contains("\"version\""), "version: {resp}");
}

#[test]
fn http_auth_required() {
    let _server = start_lux(17602, 17603, "secret");

    let (status, body) = http_request(17603, "GET", "/v1/ping", None, None);
    assert_eq!(status, 401, "no auth: {body}");

    let (status, body) = http_request(17603, "GET", "/v1/ping", None, Some("wrong"));
    assert_eq!(status, 401, "wrong auth: {body}");

    let resp = get(17603, "/v1/ping", "secret");
    assert!(resp.contains("PONG"), "correct auth: {resp}");
}

#[test]
fn http_set_get_del() {
    let _server = start_lux(17604, 17605, "");

    let resp = post(17605, "/v1/set/mykey", r#"{"value":"hello"}"#, "");
    assert!(resp.contains("\"OK\""), "set: {resp}");

    let resp = get(17605, "/v1/get/mykey", "");
    assert!(resp.contains("\"hello\""), "get: {resp}");

    let resp = post(17605, "/v1/del/mykey", "", "");
    assert!(resp.contains("1"), "del: {resp}");

    let resp = get(17605, "/v1/get/mykey", "");
    assert!(resp.contains("null"), "get after del: {resp}");
}

#[test]
fn http_incr_decr() {
    let _server = start_lux(17606, 17607, "");

    let resp = post(17607, "/v1/incr/counter", "", "");
    assert!(resp.contains("1"), "incr: {resp}");

    let resp = post(17607, "/v1/incr/counter", "", "");
    assert!(resp.contains("2"), "incr2: {resp}");

    let resp = post(17607, "/v1/decr/counter", "", "");
    assert!(resp.contains("1"), "decr: {resp}");
}

#[test]
fn http_exec_arbitrary() {
    let _server = start_lux(17608, 17609, "");

    let resp = post(17609, "/v1/exec", r#"{"command":["SET","foo","bar"]}"#, "");
    assert!(resp.contains("\"OK\""), "exec set: {resp}");

    let resp = post(17609, "/v1/exec", r#"{"command":"GET foo"}"#, "");
    assert!(resp.contains("\"bar\""), "exec get: {resp}");

    let resp = post(
        17609,
        "/v1/exec",
        r#"{"command":["HSET","h1","f1","v1","f2","v2"]}"#,
        "",
    );
    assert!(resp.contains("2"), "exec hset: {resp}");

    let resp = post(17609, "/v1/exec", r#"{"command":["HGETALL","h1"]}"#, "");
    assert!(resp.contains("f1"), "exec hgetall: {resp}");
    assert!(resp.contains("v1"), "exec hgetall val: {resp}");
}

#[test]
fn http_exec_tables() {
    let _server = start_lux(17610, 17611, "");

    let resp = post(
        17611,
        "/v1/exec",
        r#"{"command":["TCREATE","users","name:str","age:int"]}"#,
        "",
    );
    assert!(resp.contains("\"OK\""), "tcreate: {resp}");

    let resp = post(
        17611,
        "/v1/exec",
        r#"{"command":["TINSERT","users","name","Alice","age","28"]}"#,
        "",
    );
    assert!(resp.contains("1"), "tinsert: {resp}");

    let resp = post(17611, "/v1/exec", r#"{"command":["TQUERY","users"]}"#, "");
    assert!(resp.contains("Alice"), "tquery: {resp}");
}

#[test]
fn http_cors_options() {
    let _server = start_lux(17612, 17613, "secret");
    let (status, _) = http_request(17613, "OPTIONS", "/v1/exec", None, None);
    assert_eq!(status, 204, "options should return 204");
}

#[test]
fn http_not_found() {
    let _server = start_lux(17614, 17615, "");
    let resp = get(17615, "/v1/nonexistent", "");
    assert!(resp.contains("not found"), "404: {resp}");
}

#[test]
fn http_kv_crud() {
    let _server = start_lux(17616, 17617, "");

    let resp = put(17617, "/v1/kv/mykey", r#"{"value":"hello"}"#, "");
    assert!(resp.contains("\"OK\""), "put: {resp}");

    let resp = get(17617, "/v1/kv/mykey", "");
    assert!(resp.contains("\"hello\""), "get: {resp}");

    let resp = post(17617, "/v1/kv/mykey/incr", "", "");
    assert!(
        resp.contains("error"),
        "incr on string should error: {resp}"
    );

    let (status, body) = delete(17617, "/v1/kv/mykey", "");
    assert_eq!(status, 200, "delete status");
    assert!(body.contains("1"), "delete: {body}");

    let resp = get(17617, "/v1/kv/mykey", "");
    assert!(resp.contains("null"), "get after delete: {resp}");
}

#[test]
fn http_kv_incr_decr() {
    let _server = start_lux(17618, 17619, "");

    let resp = post(17619, "/v1/kv/counter/incr", "", "");
    assert!(resp.contains("1"), "incr: {resp}");
    let resp = post(17619, "/v1/kv/counter/incr", "", "");
    assert!(resp.contains("2"), "incr2: {resp}");
    let resp = post(17619, "/v1/kv/counter/decr", "", "");
    assert!(resp.contains("1"), "decr: {resp}");
}

#[test]
fn http_tables_rest() {
    let _server = start_lux(17620, 17621, "");

    let resp = post(
        17621,
        "/v1/tables",
        r#"{"name":"users","columns":["name:str","age:int"]}"#,
        "",
    );
    assert!(resp.contains("\"OK\""), "create table: {resp}");

    let resp = get(17621, "/v1/tables", "");
    assert!(resp.contains("users"), "list tables: {resp}");

    let resp = post(
        17621,
        "/v1/tables/users",
        r#"{"name":"Alice","age":"28"}"#,
        "",
    );
    assert!(resp.contains("1"), "insert: {resp}");

    let resp = post(
        17621,
        "/v1/tables/users",
        r#"{"name":"Bob","age":"35"}"#,
        "",
    );
    assert!(resp.contains("2"), "insert2: {resp}");

    let resp = get(17621, "/v1/tables/users", "");
    assert!(resp.contains("Alice"), "query all: {resp}");
    assert!(resp.contains("Bob"), "query all bob: {resp}");

    let resp = get(17621, "/v1/tables/users/1", "");
    assert!(resp.contains("Alice"), "get by id: {resp}");

    let resp = put(17621, "/v1/tables/users/1", r#"{"name":"Alicia"}"#, "");
    assert!(resp.contains("\"OK\""), "update: {resp}");

    let resp = get(17621, "/v1/tables/users/1", "");
    assert!(resp.contains("Alicia"), "get after update: {resp}");

    let resp = get(17621, "/v1/tables/users?where=age+>+30", "");
    assert!(resp.contains("Bob"), "query where: {resp}");
    assert!(!resp.contains("Alicia"), "query where excludes: {resp}");

    let resp = get(17621, "/v1/tables/users/count", "");
    assert!(resp.contains("2"), "count: {resp}");

    let resp = get(17621, "/v1/tables/users/schema", "");
    assert!(resp.contains("name"), "schema: {resp}");
    assert!(resp.contains("age"), "schema age: {resp}");

    let (status, _) = delete(17621, "/v1/tables/users/1", "");
    assert_eq!(status, 200, "delete row");

    let resp = get(17621, "/v1/tables/users/count", "");
    assert!(resp.contains("1"), "count after delete: {resp}");
}

#[test]
fn http_timeseries_rest() {
    let _server = start_lux(17622, 17623, "");

    let resp = post(
        17623,
        "/v1/ts/cpu:host1",
        r#"{"timestamp":"1000","value":72.5,"labels":{"host":"server1","metric":"cpu"}}"#,
        "",
    );
    assert!(resp.contains("result"), "tsadd: {resp}");

    let resp = post(
        17623,
        "/v1/ts/cpu:host1",
        r#"{"timestamp":"2000","value":75.0}"#,
        "",
    );
    assert!(resp.contains("result"), "tsadd2: {resp}");

    let resp = post(
        17623,
        "/v1/ts/cpu:host1",
        r#"{"timestamp":"3000","value":68.2}"#,
        "",
    );
    assert!(resp.contains("result"), "tsadd3: {resp}");

    let resp = get(17623, "/v1/ts/cpu:host1/latest", "");
    assert!(resp.contains("68.2"), "tsget latest: {resp}");

    let resp = get(17623, "/v1/ts/cpu:host1", "");
    assert!(resp.contains("72.5"), "tsrange: {resp}");
    assert!(resp.contains("75"), "tsrange2: {resp}");

    let resp = get(17623, "/v1/ts/cpu:host1?from=1000&to=2000", "");
    assert!(resp.contains("72.5"), "tsrange from/to: {resp}");

    let resp = get(17623, "/v1/ts/cpu:host1/info", "");
    assert!(resp.contains("result"), "tsinfo: {resp}");
}

#[test]
fn http_vectors_rest() {
    let _server = start_lux(17624, 17625, "");

    let resp = post(
        17625,
        "/v1/vectors/doc:1",
        r#"{"vector":[0.1,0.2,0.3],"metadata":{"title":"hello"}}"#,
        "",
    );
    assert!(resp.contains("result"), "vset: {resp}");

    let resp = post(
        17625,
        "/v1/vectors/doc:2",
        r#"{"vector":[0.9,0.1,0.0],"metadata":{"title":"other"}}"#,
        "",
    );
    assert!(resp.contains("result"), "vset2: {resp}");

    let resp = get(17625, "/v1/vectors/doc:1", "");
    assert!(resp.contains("0.1"), "vget: {resp}");

    let resp = get(17625, "/v1/vectors", "");
    assert!(resp.contains("2"), "vcard: {resp}");

    let resp = post(
        17625,
        "/v1/vectors/search",
        r#"{"vector":[0.1,0.2,0.3],"k":2}"#,
        "",
    );
    assert!(resp.contains("doc:1"), "vsearch: {resp}");

    let (status, _) = delete(17625, "/v1/vectors/doc:2", "");
    assert_eq!(status, 200, "delete vector");

    let resp = get(17625, "/v1/vectors", "");
    assert!(resp.contains("1"), "vcard after delete: {resp}");
}

#[test]
fn http_kv_data_types() {
    let _server = start_lux(17626, 17627, "");

    post(
        17627,
        "/v1/exec",
        r#"{"command":["HSET","myhash","f1","v1","f2","v2"]}"#,
        "",
    );
    let resp = get(17627, "/v1/kv/myhash/hash", "");
    assert!(resp.contains("f1"), "hash: {resp}");
    assert!(resp.contains("v1"), "hash val: {resp}");

    post(
        17627,
        "/v1/exec",
        r#"{"command":["RPUSH","mylist","a","b","c"]}"#,
        "",
    );
    let resp = get(17627, "/v1/kv/mylist/list", "");
    assert!(resp.contains("\"a\""), "list: {resp}");
    assert!(resp.contains("\"c\""), "list c: {resp}");

    post(
        17627,
        "/v1/exec",
        r#"{"command":["SADD","myset","x","y","z"]}"#,
        "",
    );
    let resp = get(17627, "/v1/kv/myset/set", "");
    assert!(resp.contains("\"x\""), "set: {resp}");

    post(
        17627,
        "/v1/exec",
        r#"{"command":["ZADD","myzset","1","a","2","b","3","c"]}"#,
        "",
    );
    let resp = get(17627, "/v1/kv/myzset/zset", "");
    assert!(resp.contains("\"a\""), "zset: {resp}");
    assert!(resp.contains("\"b\""), "zset b: {resp}");
}
