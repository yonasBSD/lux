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
    let bin = find_lux_binary().expect("no lux binary found");
    let tmpdir =
        std::env::temp_dir().join(format!("lux_streams_test_{}_{}", std::process::id(), port));
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
    panic!("lux did not start on port {port}");
}

fn connect(port: u16) -> TcpStream {
    let stream = TcpStream::connect(format!("127.0.0.1:{port}")).unwrap();
    stream.set_nodelay(true).unwrap();
    stream
        .set_read_timeout(Some(Duration::from_millis(2000)))
        .unwrap();
    stream
}

#[test]
fn xadd_and_xlen() {
    let port: u16 = 17200;
    let _server = start_lux(port);
    let mut conn = connect(port);
    let resp = send_and_read(&mut conn, &["XADD", "mystream", "*", "name", "alice"]);
    assert!(resp.contains("-"), "stream id contains dash: {resp}");
    send_and_read(&mut conn, &["XADD", "mystream", "*", "name", "bob"]);
    let resp = send_and_read(&mut conn, &["XLEN", "mystream"]);
    assert!(resp.contains(":2"), "xlen is 2: {resp}");
}

#[test]
fn xrange_returns_entries() {
    let port: u16 = 17201;
    let _server = start_lux(port);
    let mut conn = connect(port);
    send_and_read(&mut conn, &["XADD", "s", "*", "k", "v1"]);
    send_and_read(&mut conn, &["XADD", "s", "*", "k", "v2"]);
    let resp = send_and_read(&mut conn, &["XRANGE", "s", "-", "+"]);
    assert!(resp.contains("v1"), "contains v1: {resp}");
    assert!(resp.contains("v2"), "contains v2: {resp}");
}

#[test]
fn xread_returns_new_entries() {
    let port: u16 = 17202;
    let _server = start_lux(port);
    let mut conn = connect(port);
    send_and_read(&mut conn, &["XADD", "s", "*", "f", "a"]);
    send_and_read(&mut conn, &["XADD", "s", "*", "f", "b"]);
    let resp = send_and_read(&mut conn, &["XREAD", "STREAMS", "s", "0-0"]);
    assert!(resp.contains("a"), "contains a: {resp}");
    assert!(resp.contains("b"), "contains b: {resp}");
}

#[test]
fn xgroup_create_and_xreadgroup() {
    let port: u16 = 17203;
    let _server = start_lux(port);
    let mut conn = connect(port);
    send_and_read(&mut conn, &["XADD", "s", "*", "f", "v1"]);
    send_and_read(&mut conn, &["XADD", "s", "*", "f", "v2"]);
    let resp = send_and_read(&mut conn, &["XGROUP", "CREATE", "s", "grp1", "0"]);
    assert!(resp.contains("OK"), "group created: {resp}");
    let resp = send_and_read(
        &mut conn,
        &[
            "XREADGROUP",
            "GROUP",
            "grp1",
            "consumer1",
            "STREAMS",
            "s",
            ">",
        ],
    );
    assert!(resp.contains("v1"), "readgroup gets v1: {resp}");
    assert!(resp.contains("v2"), "readgroup gets v2: {resp}");
}

#[test]
fn xack_removes_pending() {
    let port: u16 = 17204;
    let _server = start_lux(port);
    let mut conn = connect(port);
    let id_resp = send_and_read(&mut conn, &["XADD", "s", "1-1", "f", "v"]);
    send_and_read(&mut conn, &["XGROUP", "CREATE", "s", "g", "0"]);
    send_and_read(
        &mut conn,
        &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
    );
    let resp = send_and_read(&mut conn, &["XACK", "s", "g", "1-1"]);
    assert!(resp.contains(":1"), "acked 1: {resp}");
    let _ = id_resp;
}

#[test]
fn xdel_removes_entry() {
    let port: u16 = 17205;
    let _server = start_lux(port);
    let mut conn = connect(port);
    send_and_read(&mut conn, &["XADD", "s", "1-1", "f", "v1"]);
    send_and_read(&mut conn, &["XADD", "s", "2-1", "f", "v2"]);
    let resp = send_and_read(&mut conn, &["XDEL", "s", "1-1"]);
    assert!(resp.contains(":1"), "deleted 1: {resp}");
    let resp = send_and_read(&mut conn, &["XLEN", "s"]);
    assert!(resp.contains(":1"), "len is 1: {resp}");
}

#[test]
fn xgroup_mkstream() {
    let port: u16 = 17206;
    let _server = start_lux(port);
    let mut conn = connect(port);
    let resp = send_and_read(
        &mut conn,
        &["XGROUP", "CREATE", "newstream", "grp", "$", "MKSTREAM"],
    );
    assert!(resp.contains("OK"), "mkstream group created: {resp}");
    let resp = send_and_read(&mut conn, &["XLEN", "newstream"]);
    assert!(resp.contains(":0"), "empty stream: {resp}");
}

#[test]
fn xread_block_woken() {
    let port: u16 = 17207;
    let _server = start_lux(port);
    let mut blocker = connect(port);
    blocker
        .set_read_timeout(Some(Duration::from_millis(5000)))
        .unwrap();

    send_and_read(
        &mut blocker,
        &["XGROUP", "CREATE", "bs", "g", "$", "MKSTREAM"],
    );

    blocker
        .write_all(&resp_cmd(&["XREAD", "BLOCK", "5000", "STREAMS", "bs", "$"]))
        .unwrap();
    thread::sleep(Duration::from_millis(200));

    let mut pusher = connect(port);
    send_and_read(&mut pusher, &["XADD", "bs", "*", "msg", "hello"]);

    thread::sleep(Duration::from_millis(300));
    let resp = read_all(&mut blocker);
    assert!(resp.contains("hello"), "block read got message: {resp}");
}

#[test]
fn xpending_summary() {
    let port: u16 = 17208;
    let _server = start_lux(port);
    let mut conn = connect(port);
    send_and_read(&mut conn, &["XADD", "s", "1-1", "f", "v"]);
    send_and_read(&mut conn, &["XGROUP", "CREATE", "s", "g", "0"]);
    send_and_read(
        &mut conn,
        &["XREADGROUP", "GROUP", "g", "c", "STREAMS", "s", ">"],
    );
    let resp = send_and_read(&mut conn, &["XPENDING", "s", "g"]);
    assert!(resp.contains(":1"), "1 pending: {resp}");
}

#[test]
fn xtrim_limits_length() {
    let port: u16 = 17209;
    let _server = start_lux(port);
    let mut conn = connect(port);
    for i in 0..10 {
        send_and_read(&mut conn, &["XADD", "s", "*", "i", &i.to_string()]);
    }
    let resp = send_and_read(&mut conn, &["XTRIM", "s", "MAXLEN", "5"]);
    assert!(resp.contains(":5"), "trimmed 5: {resp}");
    let resp = send_and_read(&mut conn, &["XLEN", "s"]);
    assert!(resp.contains(":5"), "len is 5: {resp}");
}
