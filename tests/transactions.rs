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

fn send_and_read(stream: &mut TcpStream, args: &[&str]) -> String {
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

fn start_lux(port: u16) -> LuxServer {
    let bin = find_lux_binary().expect("no lux binary found - run `cargo build` first");
    let tmpdir =
        std::env::temp_dir().join(format!("lux_tx_test_{}_{}", std::process::id(), port));
    std::fs::create_dir_all(&tmpdir).unwrap();
    let child = std::process::Command::new(&bin)
        .env("LUX_PORT", port.to_string())
        .env("LUX_SHARDS", "4")
        .env("LUX_SAVE_INTERVAL", "0")
        .env("LUX_DATA_DIR", tmpdir.to_str().unwrap())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("failed to start lux");

    let server = LuxServer { child, tmpdir };

    for _ in 0..40 {
        if TcpStream::connect(format!("127.0.0.1:{port}")).is_ok() {
            return server;
        }
        thread::sleep(Duration::from_millis(50));
    }
    panic!("lux did not start within 2 seconds on port {port}");
}

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    stream
}

#[test]
fn multi_set_get_exec_returns_array() {
    let port: u16 = 16480;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let resp = send_and_read(&mut conn, &["MULTI"]);
    assert!(resp.contains("+OK"), "MULTI: {resp}");

    let resp = send_and_read(&mut conn, &["SET", "txkey", "txval"]);
    assert!(resp.contains("+QUEUED"), "SET queued: {resp}");

    let resp = send_and_read(&mut conn, &["GET", "txkey"]);
    assert!(resp.contains("+QUEUED"), "GET queued: {resp}");

    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("*2"), "EXEC array: {resp}");
    assert!(resp.contains("+OK"), "EXEC contains SET OK: {resp}");
    assert!(resp.contains("txval"), "EXEC contains GET result: {resp}");
}

#[test]
fn multi_discard_clears_queue() {
    let port: u16 = 16481;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["SET", "dkey", "dval"]);
    let resp = send_and_read(&mut conn, &["DISCARD"]);
    assert!(resp.contains("+OK"), "DISCARD: {resp}");

    let resp = send_and_read(&mut conn, &["GET", "dkey"]);
    assert!(resp.contains("$-1"), "key should not exist: {resp}");
}

#[test]
fn watch_no_conflict_succeeds() {
    let port: u16 = 16482;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "wkey", "orig"]);
    send_and_read(&mut conn, &["WATCH", "wkey"]);
    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["SET", "wkey", "updated"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("*1"), "EXEC should succeed: {resp}");

    let resp = send_and_read(&mut conn, &["GET", "wkey"]);
    assert!(resp.contains("updated"), "value should be updated: {resp}");
}

#[test]
fn watch_conflict_aborts_exec() {
    let port: u16 = 16483;
    let _server = start_lux(port);
    let mut conn1 = connect(port);
    let mut conn2 = connect(port);

    send_and_read(&mut conn1, &["SET", "ckey", "orig"]);
    send_and_read(&mut conn1, &["WATCH", "ckey"]);
    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["SET", "ckey", "from_tx"]);

    send_and_read(&mut conn2, &["SET", "ckey", "from_other"]);

    let resp = send_and_read(&mut conn1, &["EXEC"]);
    assert!(resp.contains("*-1"), "EXEC should return nil array: {resp}");

    let resp = send_and_read(&mut conn1, &["GET", "ckey"]);
    assert!(
        resp.contains("from_other"),
        "value should be from other client: {resp}"
    );
}

#[test]
fn nested_multi_returns_error() {
    let port: u16 = 16484;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    let resp = send_and_read(&mut conn, &["MULTI"]);
    assert!(
        resp.contains("ERR Command 'multi' not allowed inside a transaction"),
        "nested MULTI: {resp}"
    );
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("EXECABORT"), "EXEC after error: {resp}");
}

#[test]
fn subscribe_inside_multi_returns_error() {
    let port: u16 = 16485;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    let resp = send_and_read(&mut conn, &["SUBSCRIBE", "chan"]);
    assert!(
        resp.contains("ERR Command 'subscribe' not allowed inside a transaction"),
        "SUBSCRIBE in MULTI should error: {resp}"
    );
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("EXECABORT"), "EXEC after SUBSCRIBE error: {resp}");
}

#[test]
fn empty_multi_exec_returns_empty_array() {
    let port: u16 = 16486;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("*0"), "empty EXEC: {resp}");
}

#[test]
fn watch_unwatch_exec_succeeds_despite_conflict() {
    let port: u16 = 16487;
    let _server = start_lux(port);
    let mut conn1 = connect(port);
    let mut conn2 = connect(port);

    send_and_read(&mut conn1, &["SET", "ukey", "orig"]);
    send_and_read(&mut conn1, &["WATCH", "ukey"]);
    send_and_read(&mut conn1, &["UNWATCH"]);

    send_and_read(&mut conn2, &["SET", "ukey", "changed"]);

    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["SET", "ukey", "from_tx"]);
    let resp = send_and_read(&mut conn1, &["EXEC"]);
    assert!(
        resp.contains("*1"),
        "EXEC should succeed after UNWATCH: {resp}"
    );

    let resp = send_and_read(&mut conn1, &["GET", "ukey"]);
    assert!(resp.contains("from_tx"), "value should be from tx: {resp}");
}

#[test]
fn exec_without_multi_returns_error() {
    let port: u16 = 16488;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("ERR EXEC without MULTI"), "EXEC error: {resp}");
}

#[test]
fn discard_without_multi_returns_error() {
    let port: u16 = 16489;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let resp = send_and_read(&mut conn, &["DISCARD"]);
    assert!(
        resp.contains("ERR DISCARD without MULTI"),
        "DISCARD error: {resp}"
    );
}

#[test]
fn watch_inside_multi_returns_error() {
    let port: u16 = 16490;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    let resp = send_and_read(&mut conn, &["WATCH", "somekey"]);
    assert!(
        resp.contains("ERR Command 'watch' not allowed inside a transaction"),
        "WATCH in MULTI: {resp}"
    );
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("EXECABORT"), "EXEC after WATCH error: {resp}");
}

#[test]
fn multi_incr_exec_returns_results() {
    let port: u16 = 16491;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "counter", "10"]);
    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["INCR", "counter"]);
    send_and_read(&mut conn, &["INCR", "counter"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("*2"), "EXEC array of 2: {resp}");
    assert!(resp.contains(":11"), "first INCR result: {resp}");
    assert!(resp.contains(":12"), "second INCR result: {resp}");
}

#[test]
fn bad_args_in_multi_not_queued() {
    let port: u16 = 16492;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    let resp = send_and_read(&mut conn, &["SET", "only_key"]);
    assert!(
        resp.contains("ERR wrong number"),
        "bad args should error: {resp}"
    );

    send_and_read(&mut conn, &["SET", "ok_key", "ok_val"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("EXECABORT"), "EXEC should abort after bad args: {resp}");
}

#[test]
fn watch_multi_keys_one_modified_aborts() {
    let port: u16 = 16493;
    let _server = start_lux(port);
    let mut conn1 = connect(port);
    let mut conn2 = connect(port);

    send_and_read(&mut conn1, &["SET", "x", "30"]);
    send_and_read(&mut conn1, &["WATCH", "a", "b", "x", "k", "z"]);

    send_and_read(&mut conn2, &["SET", "x", "40"]);

    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["PING"]);
    let resp = send_and_read(&mut conn1, &["EXEC"]);
    assert!(
        resp.contains("*-1"),
        "modifying 1 of 5 watched keys should abort: {resp}"
    );
}

#[test]
fn execabort_clears_multi_state() {
    let port: u16 = 16494;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["SET", "foo", "bar"]);
    send_and_read(&mut conn, &["NOTACOMMAND"]);
    send_and_read(&mut conn, &["SET", "foo2", "bar2"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("EXECABORT"), "should abort: {resp}");

    let resp = send_and_read(&mut conn, &["PING"]);
    assert!(resp.contains("PONG"), "should be back to normal: {resp}");
}

#[test]
fn after_successful_exec_watch_is_cleared() {
    let port: u16 = 16495;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "x", "30"]);
    send_and_read(&mut conn, &["WATCH", "x"]);
    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["PING"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("PONG"), "first EXEC: {resp}");

    send_and_read(&mut conn, &["SET", "x", "40"]);
    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["PING"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(
        resp.contains("PONG"),
        "second EXEC should succeed, watch was cleared: {resp}"
    );
}

#[test]
fn after_failed_exec_watch_is_cleared() {
    let port: u16 = 16496;
    let _server = start_lux(port);
    let mut conn1 = connect(port);
    let mut conn2 = connect(port);

    send_and_read(&mut conn1, &["SET", "x", "30"]);
    send_and_read(&mut conn1, &["WATCH", "x"]);
    send_and_read(&mut conn2, &["SET", "x", "40"]);
    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["PING"]);
    let resp = send_and_read(&mut conn1, &["EXEC"]);
    assert!(resp.contains("*-1"), "first EXEC aborted: {resp}");

    send_and_read(&mut conn2, &["SET", "x", "50"]);
    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["PING"]);
    let resp = send_and_read(&mut conn1, &["EXEC"]);
    assert!(
        resp.contains("PONG"),
        "second EXEC should succeed, watch cleared after abort: {resp}"
    );
}

#[test]
fn unwatch_with_nothing_watched() {
    let port: u16 = 16497;
    let _server = start_lux(port);
    let mut conn = connect(port);

    let resp = send_and_read(&mut conn, &["UNWATCH"]);
    assert!(resp.contains("+OK"), "UNWATCH on fresh connection: {resp}");
}

#[test]
fn flushall_invalidates_watch_on_existing_key() {
    let port: u16 = 16498;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "x", "30"]);
    send_and_read(&mut conn, &["WATCH", "x"]);
    send_and_read(&mut conn, &["FLUSHALL"]);
    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["PING"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(
        resp.contains("*-1"),
        "FLUSHALL should invalidate watch: {resp}"
    );
}

#[test]
fn flushdb_invalidates_watch_on_existing_key() {
    let port: u16 = 16499;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "x", "30"]);
    send_and_read(&mut conn, &["WATCH", "x"]);
    send_and_read(&mut conn, &["FLUSHDB"]);
    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["PING"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(
        resp.contains("*-1"),
        "FLUSHDB should invalidate watch: {resp}"
    );
}

#[test]
fn expire_touches_watched_key() {
    let port: u16 = 16500;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["SET", "x", "foo"]);
    send_and_read(&mut conn, &["WATCH", "x"]);
    send_and_read(&mut conn, &["EXPIRE", "x", "10"]);
    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["PING"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(
        resp.contains("*-1"),
        "EXPIRE should touch watched key: {resp}"
    );
}

#[test]
fn discard_clears_watch_dirty_flag() {
    let port: u16 = 16501;
    let _server = start_lux(port);
    let mut conn1 = connect(port);
    let mut conn2 = connect(port);

    send_and_read(&mut conn1, &["SET", "x", "10"]);
    send_and_read(&mut conn1, &["WATCH", "x"]);
    send_and_read(&mut conn2, &["SET", "x", "10"]);
    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["DISCARD"]);

    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["INCR", "x"]);
    let resp = send_and_read(&mut conn1, &["EXEC"]);
    assert!(
        resp.contains(":11"),
        "DISCARD should clear dirty flag, INCR should work: {resp}"
    );
}

#[test]
fn discard_fully_unwatches_keys() {
    let port: u16 = 16502;
    let _server = start_lux(port);
    let mut conn1 = connect(port);
    let mut conn2 = connect(port);

    send_and_read(&mut conn1, &["SET", "x", "10"]);
    send_and_read(&mut conn1, &["WATCH", "x"]);
    send_and_read(&mut conn2, &["SET", "x", "10"]);
    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["DISCARD"]);

    send_and_read(&mut conn2, &["SET", "x", "10"]);

    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["INCR", "x"]);
    let resp = send_and_read(&mut conn1, &["EXEC"]);
    assert!(
        resp.contains(":11"),
        "DISCARD should unwatch, second write should not conflict: {resp}"
    );
}

#[test]
fn flushall_watch_multi_keys_stability() {
    let port: u16 = 16503;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MSET", "a", "a", "b", "b"]);
    send_and_read(&mut conn, &["WATCH", "b", "a"]);
    send_and_read(&mut conn, &["FLUSHALL"]);
    let resp = send_and_read(&mut conn, &["PING"]);
    assert!(resp.contains("PONG"), "should not crash: {resp}");
    let resp = send_and_read(&mut conn, &["UNWATCH"]);
    assert!(resp.contains("+OK"), "UNWATCH after FLUSHALL: {resp}");
}

#[test]
fn unknown_command_in_multi_causes_execabort() {
    let port: u16 = 16504;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["SET", "foo1", "bar1"]);
    let resp = send_and_read(&mut conn, &["TOTALLYNOTACOMMAND"]);
    assert!(resp.contains("ERR unknown command"), "unknown cmd: {resp}");
    send_and_read(&mut conn, &["SET", "foo2", "bar2"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("EXECABORT"), "EXEC should abort: {resp}");

    let resp = send_and_read(&mut conn, &["EXISTS", "foo1"]);
    assert!(resp.contains(":0"), "foo1 should not exist: {resp}");
    let resp = send_and_read(&mut conn, &["EXISTS", "foo2"]);
    assert!(resp.contains(":0"), "foo2 should not exist: {resp}");
}

#[test]
fn watch_multiple_calls_accumulate() {
    let port: u16 = 16505;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["WATCH", "x", "y", "z"]);
    send_and_read(&mut conn, &["WATCH", "k"]);
    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["PING"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(
        resp.contains("PONG"),
        "multiple WATCH calls, no conflict: {resp}"
    );
}

#[test]
fn multi_with_mixed_data_types() {
    let port: u16 = 16506;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["SET", "str", "hello"]);
    send_and_read(&mut conn, &["LPUSH", "list", "a", "b"]);
    send_and_read(&mut conn, &["SADD", "set", "x", "y"]);
    send_and_read(&mut conn, &["HSET", "hash", "f1", "v1"]);
    send_and_read(&mut conn, &["ZADD", "zset", "1", "m1"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("*5"), "5 commands in array: {resp}");
    assert!(resp.contains("+OK"), "SET result: {resp}");

    let resp = send_and_read(&mut conn, &["GET", "str"]);
    assert!(resp.contains("hello"), "string was set: {resp}");
    let resp = send_and_read(&mut conn, &["LLEN", "list"]);
    assert!(resp.contains(":2"), "list has 2 items: {resp}");
    let resp = send_and_read(&mut conn, &["SCARD", "set"]);
    assert!(resp.contains(":2"), "set has 2 members: {resp}");
}

#[test]
fn publish_inside_multi_exec() {
    let port: u16 = 16507;
    let _server = start_lux(port);
    let mut conn = connect(port);

    send_and_read(&mut conn, &["MULTI"]);
    send_and_read(&mut conn, &["PUBLISH", "chan", "msg"]);
    let resp = send_and_read(&mut conn, &["EXEC"]);
    assert!(resp.contains("*1"), "EXEC array: {resp}");
    assert!(resp.contains(":0"), "PUBLISH returns 0 subscribers: {resp}");
}

#[test]
fn watch_on_nonexistent_key_then_create_aborts() {
    let port: u16 = 16508;
    let _server = start_lux(port);
    let mut conn1 = connect(port);
    let mut conn2 = connect(port);

    send_and_read(&mut conn1, &["DEL", "newkey"]);
    send_and_read(&mut conn1, &["WATCH", "newkey"]);

    send_and_read(&mut conn2, &["SET", "newkey", "created"]);

    send_and_read(&mut conn1, &["MULTI"]);
    send_and_read(&mut conn1, &["PING"]);
    let resp = send_and_read(&mut conn1, &["EXEC"]);
    assert!(
        resp.contains("*-1"),
        "creating a watched nonexistent key should abort: {resp}"
    );
}
