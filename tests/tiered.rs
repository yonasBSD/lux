use std::io::{Read, Write};
use std::net::TcpStream;
use std::thread;
use std::time::Duration;

fn resp_cmd(args: &[&str]) -> Vec<u8> {
    let mut buf = format!("*{}\r\n", args.len());
    for arg in args {
        buf.push_str(&format!("${}\r\n{}\r\n", arg.len(), arg));
    }
    buf.into_bytes()
}

fn read_all(stream: &mut TcpStream) -> String {
    let mut data = Vec::with_capacity(4096);
    let mut buf = [0u8; 8192];
    loop {
        match stream.read(&mut buf) {
            Ok(0) => break,
            Ok(len) => data.extend_from_slice(&buf[..len]),
            Err(e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
            Err(e) if e.kind() == std::io::ErrorKind::TimedOut => break,
            Err(_) => break,
        }
    }
    String::from_utf8_lossy(&data).to_string()
}

fn send(stream: &mut TcpStream, args: &[&str]) -> String {
    stream.write_all(&resp_cmd(args)).unwrap();
    thread::sleep(Duration::from_millis(50));
    read_all(stream)
}

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

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    stream
}

fn wait_for_port(port: u16) {
    for _ in 0..40 {
        if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("server did not start on port {port}");
}

struct TestServer {
    port: u16,
    child: std::process::Child,
    tmpdir: std::path::PathBuf,
}

impl TestServer {
    fn start(port: u16) -> Self {
        let bin = find_lux_binary().expect("no lux binary found");
        let tmpdir = std::env::temp_dir().join(format!("lux_tiered_test_{port}"));
        let _ = std::fs::remove_dir_all(&tmpdir);
        std::fs::create_dir_all(&tmpdir).unwrap();

        let child = std::process::Command::new(&bin)
            .env("LUX_PORT", port.to_string())
            .env("LUX_SHARDS", "4")
            .env("LUX_MAXMEMORY", "100kb")
            .env("LUX_MAXMEMORY_POLICY", "allkeys-lru")
            .env("LUX_STORAGE_MODE", "tiered")
            .env("LUX_STORAGE_DIR", tmpdir.join("storage").to_str().unwrap())
            .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
            .env("LUX_SAVE_INTERVAL", "0")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to start lux");

        wait_for_port(port);
        TestServer {
            port,
            child,
            tmpdir,
        }
    }

    fn restart(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
        thread::sleep(Duration::from_millis(500));

        let bin = find_lux_binary().expect("no lux binary found");
        self.child = std::process::Command::new(&bin)
            .env("LUX_PORT", self.port.to_string())
            .env("LUX_SHARDS", "4")
            .env("LUX_MAXMEMORY", "10mb")
            .env("LUX_MAXMEMORY_POLICY", "allkeys-lru")
            .env("LUX_STORAGE_MODE", "tiered")
            .env(
                "LUX_STORAGE_DIR",
                self.tmpdir.join("storage").to_str().unwrap(),
            )
            .env("LUX_DATA_DIR", self.tmpdir.to_str().unwrap())
            .env("LUX_SAVE_INTERVAL", "0")
            .stdout(std::process::Stdio::null())
            .stderr(std::process::Stdio::null())
            .spawn()
            .expect("failed to restart lux");

        wait_for_port(self.port);
    }

    fn conn(&self) -> TcpStream {
        connect(self.port)
    }
}

impl Drop for TestServer {
    fn drop(&mut self) {
        self.child.kill().ok();
        self.child.wait().ok();
        let _ = std::fs::remove_dir_all(&self.tmpdir);
    }
}

fn fill_memory(conn: &mut TcpStream, count: usize) {
    let val = "x".repeat(10000);
    for i in 0..count {
        send(conn, &["SET", &format!("filler:{i}"), &val]);
    }
}

#[test]
fn tiered_cold_string_read() {
    let srv = TestServer::start(17100);
    let mut c = srv.conn();
    send(&mut c, &["SET", "mykey", "myvalue"]);
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["GET", "mykey"]);
    assert!(resp.contains("myvalue"), "cold GET failed: {resp}");
}

#[test]
fn tiered_cold_hash_read() {
    let srv = TestServer::start(17101);
    let mut c = srv.conn();
    send(
        &mut c,
        &["HSET", "myhash", "f1", "v1", "f2", "v2", "f3", "v3"],
    );
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["HGETALL", "myhash"]);
    assert!(resp.contains("f1"), "cold HGETALL missing f1: {resp}");
    assert!(resp.contains("v2"), "cold HGETALL missing v2: {resp}");
    assert!(resp.contains("f3"), "cold HGETALL missing f3: {resp}");
}

#[test]
fn tiered_cold_list_read() {
    let srv = TestServer::start(17102);
    let mut c = srv.conn();
    send(&mut c, &["LPUSH", "mylist", "a", "b", "c"]);
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["LRANGE", "mylist", "0", "-1"]);
    assert!(resp.contains("a"), "cold LRANGE missing a: {resp}");
    assert!(resp.contains("b"), "cold LRANGE missing b: {resp}");
    assert!(resp.contains("c"), "cold LRANGE missing c: {resp}");
}

#[test]
fn tiered_cold_set_read() {
    let srv = TestServer::start(17103);
    let mut c = srv.conn();
    send(&mut c, &["SADD", "myset", "x", "y", "z"]);
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["SMEMBERS", "myset"]);
    assert!(resp.contains("x"), "cold SMEMBERS missing x: {resp}");
    assert!(resp.contains("y"), "cold SMEMBERS missing y: {resp}");
}

#[test]
fn tiered_cold_sorted_set_read() {
    let srv = TestServer::start(17104);
    let mut c = srv.conn();
    send(&mut c, &["ZADD", "myzset", "1.5", "alpha", "2.5", "beta"]);
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["ZRANGE", "myzset", "0", "-1", "WITHSCORES"]);
    assert!(resp.contains("alpha"), "cold ZRANGE missing alpha: {resp}");
    assert!(resp.contains("beta"), "cold ZRANGE missing beta: {resp}");
}

#[test]
fn tiered_cold_key_mutation() {
    let srv = TestServer::start(17105);
    let mut c = srv.conn();
    send(&mut c, &["HSET", "h", "f1", "v1", "f2", "v2"]);
    fill_memory(&mut c, 20);
    send(&mut c, &["HSET", "h", "f3", "v3"]);
    let resp = send(&mut c, &["HGETALL", "h"]);
    assert!(resp.contains("f1"), "mutation lost f1: {resp}");
    assert!(resp.contains("f2"), "mutation lost f2: {resp}");
    assert!(resp.contains("f3"), "mutation missing f3: {resp}");
}

#[test]
fn tiered_cold_list_mutation() {
    let srv = TestServer::start(17106);
    let mut c = srv.conn();
    send(&mut c, &["LPUSH", "l", "a", "b", "c"]);
    fill_memory(&mut c, 20);
    send(&mut c, &["LPUSH", "l", "d"]);
    let resp = send(&mut c, &["LLEN", "l"]);
    assert!(resp.contains(":4"), "LLEN should be 4: {resp}");
}

#[test]
fn tiered_cold_incr() {
    let srv = TestServer::start(17107);
    let mut c = srv.conn();
    send(&mut c, &["SET", "counter", "100"]);
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["INCR", "counter"]);
    assert!(
        resp.contains(":101"),
        "INCR cold counter should be 101: {resp}"
    );
}

#[test]
fn tiered_del_cold_key() {
    let srv = TestServer::start(17108);
    let mut c = srv.conn();
    send(&mut c, &["SET", "delme", "exists"]);
    fill_memory(&mut c, 20);
    let del_resp = send(&mut c, &["DEL", "delme"]);
    assert!(
        del_resp.contains(":1"),
        "DEL cold key should return 1: {del_resp}"
    );
    let exists_resp = send(&mut c, &["EXISTS", "delme"]);
    assert!(
        exists_resp.contains(":0"),
        "EXISTS after DEL should be 0: {exists_resp}"
    );
}

#[test]
fn tiered_exists_cold_key() {
    let srv = TestServer::start(17109);
    let mut c = srv.conn();
    send(&mut c, &["SET", "ekey", "val"]);
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["EXISTS", "ekey"]);
    assert!(resp.contains(":1"), "EXISTS cold key should be 1: {resp}");
}

#[test]
fn tiered_type_cold_key() {
    let srv = TestServer::start(17110);
    let mut c = srv.conn();
    send(&mut c, &["HSET", "tyh", "f", "v"]);
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["TYPE", "tyh"]);
    assert!(resp.contains("hash"), "TYPE cold hash: {resp}");
}

#[test]
fn tiered_keys_includes_cold() {
    let srv = TestServer::start(17111);
    let mut c = srv.conn();
    send(&mut c, &["SET", "coldpattern:1", "a"]);
    send(&mut c, &["SET", "coldpattern:2", "b"]);
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["KEYS", "coldpattern:*"]);
    assert!(
        resp.contains("coldpattern:1"),
        "KEYS should include cold key 1: {resp}"
    );
    assert!(
        resp.contains("coldpattern:2"),
        "KEYS should include cold key 2: {resp}"
    );
}

#[test]
fn tiered_dbsize_includes_cold() {
    let srv = TestServer::start(17112);
    let mut c = srv.conn();
    for i in 0..10 {
        send(&mut c, &["SET", &format!("dbkey:{i}"), "val"]);
    }
    fill_memory(&mut c, 20);
    let resp = send(&mut c, &["DBSIZE"]);
    let size: i64 = resp
        .trim()
        .strip_prefix(':')
        .unwrap_or("0")
        .trim()
        .parse()
        .unwrap_or(0);
    assert!(size >= 30, "DBSIZE should include cold keys: {size}");
}

#[test]
fn tiered_wal_crash_recovery() {
    let mut srv = TestServer::start(17113);
    let mut c = srv.conn();
    send(&mut c, &["SET", "wal_str", "survives"]);
    send(&mut c, &["HSET", "wal_hash", "f1", "v1", "f2", "v2"]);
    send(&mut c, &["LPUSH", "wal_list", "a", "b", "c"]);
    send(&mut c, &["SADD", "wal_set", "x", "y"]);
    send(&mut c, &["ZADD", "wal_zset", "1", "m1", "2", "m2"]);
    drop(c);

    srv.restart();
    let mut c = srv.conn();

    let resp = send(&mut c, &["GET", "wal_str"]);
    assert!(resp.contains("survives"), "WAL string recovery: {resp}");

    let resp = send(&mut c, &["HGETALL", "wal_hash"]);
    assert!(resp.contains("f1"), "WAL hash recovery f1: {resp}");
    assert!(resp.contains("v2"), "WAL hash recovery v2: {resp}");

    let resp = send(&mut c, &["LRANGE", "wal_list", "0", "-1"]);
    assert!(resp.contains("a"), "WAL list recovery: {resp}");

    let resp = send(&mut c, &["SMEMBERS", "wal_set"]);
    assert!(resp.contains("x"), "WAL set recovery: {resp}");

    let resp = send(&mut c, &["ZRANGE", "wal_zset", "0", "-1"]);
    assert!(resp.contains("m1"), "WAL zset recovery: {resp}");
}

#[test]
fn tiered_wal_overwrite_ordering() {
    let mut srv = TestServer::start(17114);
    let mut c = srv.conn();
    send(&mut c, &["SET", "ow", "first"]);
    send(&mut c, &["SET", "ow", "second"]);
    send(&mut c, &["SET", "ow", "third"]);
    drop(c);

    srv.restart();
    let mut c = srv.conn();
    let resp = send(&mut c, &["GET", "ow"]);
    assert!(
        resp.contains("third"),
        "overwrite should be 'third': {resp}"
    );
}

#[test]
fn tiered_wal_set_then_del() {
    let mut srv = TestServer::start(17115);
    let mut c = srv.conn();
    send(&mut c, &["SET", "delwal", "exists"]);
    send(&mut c, &["DEL", "delwal"]);
    drop(c);

    srv.restart();
    let mut c = srv.conn();
    let resp = send(&mut c, &["EXISTS", "delwal"]);
    assert!(
        resp.contains(":0"),
        "DEL'd key should stay deleted after WAL replay: {resp}"
    );
}

#[test]
fn tiered_snapshot_includes_cold() {
    let mut srv = TestServer::start(17116);
    let mut c = srv.conn();
    send(&mut c, &["SET", "snapcold", "value"]);
    fill_memory(&mut c, 20);
    let exists = send(&mut c, &["EXISTS", "snapcold"]);
    assert!(exists.contains(":1"), "key should exist (cold): {exists}");
    send(&mut c, &["SAVE"]);
    drop(c);

    srv.restart();
    let mut c = srv.conn();
    let resp = send(&mut c, &["GET", "snapcold"]);
    assert!(
        resp.contains("value"),
        "cold key should survive snapshot+restart: {resp}"
    );
}

#[test]
fn tiered_flushdb_clears_disk() {
    let srv = TestServer::start(17117);
    let mut c = srv.conn();
    send(&mut c, &["SET", "fkey", "fval"]);
    fill_memory(&mut c, 20);
    send(&mut c, &["FLUSHDB"]);
    let resp = send(&mut c, &["DBSIZE"]);
    assert!(
        resp.contains(":0"),
        "FLUSHDB should clear everything: {resp}"
    );
    let resp = send(&mut c, &["EXISTS", "fkey"]);
    assert!(
        resp.contains(":0"),
        "flushed cold key should not exist: {resp}"
    );
}
