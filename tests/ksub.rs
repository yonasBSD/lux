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

fn send(stream: &mut TcpStream, args: &[&str]) {
    stream.write_all(&resp_cmd(args)).unwrap();
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
        std::env::temp_dir().join(format!("lux_ksub_test_{}_{}", std::process::id(), port));
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
        .set_read_timeout(Some(Duration::from_millis(500)))
        .unwrap();
    stream
}

#[test]
fn ksub_basic_event_delivery() {
    let port: u16 = 16800;
    let _server = start_lux(port);
    let mut sub_conn = connect(port);
    let mut writer = connect(port);

    let resp = send_and_read(&mut sub_conn, &["KSUB", "user:*"]);
    assert!(resp.contains("ksub"), "ksub confirmation: {resp}");
    assert!(resp.contains("user:*"), "pattern in response: {resp}");

    send_and_read(&mut writer, &["SET", "user:1", "alice"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(resp.contains("kmessage"), "kmessage type: {resp}");
    assert!(resp.contains("user:*"), "pattern: {resp}");
    assert!(resp.contains("user:1"), "key: {resp}");
    assert!(resp.contains("set"), "operation: {resp}");
}

#[test]
fn ksub_pattern_filtering() {
    let port: u16 = 16801;
    let _server = start_lux(port);
    let mut sub_conn = connect(port);
    let mut writer = connect(port);

    send(&mut sub_conn, &["KSUB", "user:*"]);
    thread::sleep(Duration::from_millis(100));
    read_all(&mut sub_conn);

    send_and_read(&mut writer, &["SET", "orders:1", "foo"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(
        resp.is_empty(),
        "should not receive non-matching key: {resp}"
    );

    send_and_read(&mut writer, &["SET", "user:2", "bob"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(
        resp.contains("kmessage"),
        "should receive matching key: {resp}"
    );
    assert!(resp.contains("user:2"), "key in event: {resp}");
}

#[test]
fn ksub_multiple_patterns() {
    let port: u16 = 16802;
    let _server = start_lux(port);
    let mut sub_conn = connect(port);
    let mut writer = connect(port);

    send(&mut sub_conn, &["KSUB", "user:*", "order:*"]);
    thread::sleep(Duration::from_millis(100));
    read_all(&mut sub_conn);

    send_and_read(&mut writer, &["SET", "user:1", "alice"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(resp.contains("user:1"), "user key event: {resp}");

    send_and_read(&mut writer, &["SET", "order:1", "pizza"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(resp.contains("order:1"), "order key event: {resp}");
}

#[test]
fn kunsub_stops_events() {
    let port: u16 = 16803;
    let _server = start_lux(port);
    let mut sub_conn = connect(port);
    let mut writer = connect(port);

    send(&mut sub_conn, &["KSUB", "key:*"]);
    thread::sleep(Duration::from_millis(100));
    read_all(&mut sub_conn);

    send_and_read(&mut writer, &["SET", "key:1", "v1"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(
        resp.contains("kmessage"),
        "should receive before unsub: {resp}"
    );

    send(&mut sub_conn, &["KUNSUB", "key:*"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(resp.contains("kunsub"), "kunsub confirmation: {resp}");

    send_and_read(&mut writer, &["SET", "key:2", "v2"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(
        !resp.contains("kmessage"),
        "should not receive after unsub: {resp}"
    );
}

#[test]
fn ksub_hset_event() {
    let port: u16 = 16804;
    let _server = start_lux(port);
    let mut sub_conn = connect(port);
    let mut writer = connect(port);

    send(&mut sub_conn, &["KSUB", "user:*"]);
    thread::sleep(Duration::from_millis(100));
    read_all(&mut sub_conn);

    send_and_read(&mut writer, &["HSET", "user:2", "name", "bob"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(resp.contains("kmessage"), "hset event: {resp}");
    assert!(resp.contains("user:2"), "key: {resp}");
    assert!(resp.contains("hset"), "operation: {resp}");
}

#[test]
fn ksub_del_event() {
    let port: u16 = 16805;
    let _server = start_lux(port);
    let mut sub_conn = connect(port);
    let mut writer = connect(port);

    send_and_read(&mut writer, &["SET", "user:1", "alice"]);

    send(&mut sub_conn, &["KSUB", "user:*"]);
    thread::sleep(Duration::from_millis(100));
    read_all(&mut sub_conn);

    send_and_read(&mut writer, &["DEL", "user:1"]);
    thread::sleep(Duration::from_millis(100));
    let resp = read_all(&mut sub_conn);
    assert!(resp.contains("kmessage"), "del event: {resp}");
    assert!(resp.contains("user:1"), "key: {resp}");
    assert!(resp.contains("del"), "operation: {resp}");
}
