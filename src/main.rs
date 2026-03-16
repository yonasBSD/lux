mod cmd;
mod pubsub;
mod resp;
mod snapshot;
mod store;

use bytes::BytesMut;
use cmd::CmdResult;
use pubsub::Broker;
use resp::Parser;
use std::collections::HashMap;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::sync::OnceLock;
use std::time::Instant;
use store::Store;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::broadcast;

pub static CONNECTED_CLIENTS: AtomicUsize = AtomicUsize::new(0);
pub static TOTAL_COMMANDS: AtomicUsize = AtomicUsize::new(0);
pub static START_TIME: OnceLock<Instant> = OnceLock::new();

#[tokio::main]
async fn main() -> std::io::Result<()> {
    START_TIME.set(Instant::now()).ok();

    let port: u16 = std::env::var("LUX_PORT")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(6379);
    let addr = format!("0.0.0.0:{port}");
    let listener = TcpListener::bind(&addr).await?;
    let store = Arc::new(Store::new());
    let broker = Broker::new();

    let require_auth = std::env::var("LUX_PASSWORD").is_ok_and(|p| !p.is_empty());

    match snapshot::load(&store) {
        Ok(0) => println!("no snapshot found"),
        Ok(n) => println!("loaded {n} keys from snapshot"),
        Err(e) => eprintln!("snapshot load error: {e}"),
    }

    tokio::spawn(snapshot::background_save_loop(store.clone()));

    {
        let store = store.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                store.expire_sweep(Instant::now());
            }
        });
    }

    println!("lux v{} ready on {addr}", env!("CARGO_PKG_VERSION"));

    loop {
        let (socket, peer) = listener.accept().await?;
        let store = store.clone();
        let broker = broker.clone();
        socket.set_nodelay(true).ok();

        tokio::spawn(async move {
            CONNECTED_CLIENTS.fetch_add(1, Ordering::Relaxed);
            let result = handle_connection(socket, peer, store, broker, require_auth).await;
            CONNECTED_CLIENTS.fetch_sub(1, Ordering::Relaxed);
            if let Err(e) = result {
                if e.kind() != std::io::ErrorKind::ConnectionReset {
                    eprintln!("connection error {peer}: {e}");
                }
            }
        });
    }
}

#[inline(always)]
fn cmd_eq_fast(input: &[u8], expected: &[u8]) -> bool {
    input.len() == expected.len()
        && input
            .iter()
            .zip(expected)
            .all(|(a, b)| a.to_ascii_uppercase() == *b)
}

#[inline(always)]
fn is_tx_cmd(cmd: &[u8]) -> bool {
    cmd_eq_fast(cmd, b"MULTI")
        || cmd_eq_fast(cmd, b"EXEC")
        || cmd_eq_fast(cmd, b"DISCARD")
        || cmd_eq_fast(cmd, b"WATCH")
        || cmd_eq_fast(cmd, b"UNWATCH")
}

async fn handle_connection(
    mut socket: tokio::net::TcpStream,
    _peer: std::net::SocketAddr,
    store: Arc<Store>,
    broker: Broker,
    require_auth: bool,
) -> std::io::Result<()> {
    let mut read_buf = vec![0u8; 65536];
    let mut write_buf = BytesMut::with_capacity(65536);
    let mut pending = BytesMut::new();
    let mut subscriptions: HashMap<String, broadcast::Receiver<pubsub::Message>> = HashMap::new();
    let mut sub_mode = false;
    let mut authenticated = !require_auth;
    let mut in_multi = false;
    let mut tx_queue: Vec<Vec<Vec<u8>>> = Vec::new();
    let mut watched: Vec<(String, usize, u64)> = Vec::new();
    let mut tx_error = false;

    loop {
        if sub_mode {
            tokio::select! {
                result = socket.read(&mut read_buf) => {
                    let n = match result {
                        Ok(0) => return Ok(()),
                        Ok(n) => n,
                        Err(e) => return Err(e),
                    };
                    pending.extend_from_slice(&read_buf[..n]);
                    let now = Instant::now();
                    let mut parser = Parser::new(&pending);
                    while let Ok(Some(args)) = parser.parse_command() {
                        if args.is_empty() { continue; }
                        if cmd_eq_fast(args[0], b"SUBSCRIBE") {
                            for ch_bytes in &args[1..] {
                                let ch = std::str::from_utf8(ch_bytes).unwrap_or("").to_string();
                                if !subscriptions.contains_key(&ch) {
                                    let rx = broker.subscribe(&ch).await;
                                    subscriptions.insert(ch.clone(), rx);
                                }
                                resp::write_array_header(&mut write_buf, 3);
                                resp::write_bulk(&mut write_buf, "subscribe");
                                resp::write_bulk(&mut write_buf, &ch);
                                resp::write_integer(&mut write_buf, subscriptions.len() as i64);
                            }
                        } else if cmd_eq_fast(args[0], b"UNSUBSCRIBE") {
                            let channels: Vec<String> = if args.len() > 1 {
                                args[1..].iter().map(|a| std::str::from_utf8(a).unwrap_or("").to_string()).collect()
                            } else {
                                subscriptions.keys().cloned().collect()
                            };
                            for ch in &channels {
                                subscriptions.remove(ch);
                                resp::write_array_header(&mut write_buf, 3);
                                resp::write_bulk(&mut write_buf, "unsubscribe");
                                resp::write_bulk(&mut write_buf, ch);
                                resp::write_integer(&mut write_buf, subscriptions.len() as i64);
                            }
                            if subscriptions.is_empty() {
                                sub_mode = false;
                            }
                        } else if cmd_eq_fast(args[0], b"PING") {
                            if args.len() > 1 {
                                resp::write_bulk_raw(&mut write_buf, args[1]);
                            } else {
                                resp::write_pong(&mut write_buf);
                            }
                        } else {
                            resp::write_error(&mut write_buf, "ERR only SUBSCRIBE, UNSUBSCRIBE, and PING are allowed in subscribe mode");
                        }
                        let _ = now;
                    }
                    let consumed = parser.pos();
                    let _ = pending.split_to(consumed);
                    if !write_buf.is_empty() {
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                }
                msg = async {
                    for (_ch, rx) in subscriptions.iter_mut() {
                        if let Ok(msg) = rx.try_recv() {
                            return Some(msg);
                        }
                    }
                    tokio::time::sleep(std::time::Duration::from_millis(1)).await;
                    for (_ch, rx) in subscriptions.iter_mut() {
                        if let Ok(msg) = rx.try_recv() {
                            return Some(msg);
                        }
                    }
                    None
                } => {
                    if let Some(msg) = msg {
                        resp::write_array_header(&mut write_buf, 3);
                        resp::write_bulk(&mut write_buf, "message");
                        resp::write_bulk(&mut write_buf, &msg.channel);
                        resp::write_bulk(&mut write_buf, &msg.payload);
                        socket.write_all(&write_buf).await?;
                        write_buf.clear();
                    }
                }
            }
        } else {
            let n = match socket.read(&mut read_buf).await {
                Ok(0) => return Ok(()),
                Ok(n) => n,
                Err(e) => return Err(e),
            };

            pending.extend_from_slice(&read_buf[..n]);
            let now = Instant::now();
            let mut parser = Parser::new(&pending);

            let mut commands: Vec<Vec<&[u8]>> = Vec::new();
            while let Ok(Some(args)) = parser.parse_command() {
                if args.is_empty() {
                    continue;
                }
                commands.push(args);
            }
            let consumed = parser.pos();

            if commands.len() <= 1 {
                for args in &commands {
                    if !authenticated
                        && !cmd_eq_fast(args[0], b"AUTH")
                        && !cmd_eq_fast(args[0], b"PING")
                        && !cmd_eq_fast(args[0], b"QUIT")
                    {
                        resp::write_error(&mut write_buf, "NOAUTH Authentication required");
                        continue;
                    }
                    TOTAL_COMMANDS.fetch_add(1, Ordering::Relaxed);

                    if cmd_eq_fast(args[0], b"MULTI") {
                        if in_multi {
                            let cmd_name = std::str::from_utf8(args[0]).unwrap_or("multi").to_lowercase();
                            resp::write_error(&mut write_buf, &format!("ERR Command '{}' not allowed inside a transaction", cmd_name));
                            tx_error = true;
                        } else {
                            in_multi = true;
                            tx_error = false;
                            resp::write_ok(&mut write_buf);
                        }
                        continue;
                    } else if cmd_eq_fast(args[0], b"EXEC") {
                        if !in_multi {
                            resp::write_error(&mut write_buf, "ERR EXEC without MULTI");
                        } else if tx_error {
                            resp::write_error(&mut write_buf, "EXECABORT Transaction discarded because of previous errors.");
                        } else {
                            let mut aborted = false;
                            for (_key, shard_idx, version) in &watched {
                                let current = store.shard_version(*shard_idx);
                                if current != *version {
                                    aborted = true;
                                    break;
                                }
                            }
                            if aborted {
                                resp::write_null_array(&mut write_buf);
                            } else {
                                let queue = std::mem::take(&mut tx_queue);
                                resp::write_array_header(&mut write_buf, queue.len());
                                for owned_args in &queue {
                                    let refs: Vec<&[u8]> = owned_args.iter().map(|v| v.as_slice()).collect();
                                    match cmd::execute(&store, &broker, &refs, &mut write_buf, now) {
                                        CmdResult::Written => {}
                                        CmdResult::Authenticated => { authenticated = true; }
                                        CmdResult::Subscribe { .. } => {
                                            resp::write_error(&mut write_buf, "ERR Command 'subscribe' not allowed inside a transaction");
                                        }
                                        CmdResult::Publish { channel, message } => {
                                            let count = broker.publish(&channel, message).await;
                                            resp::write_integer(&mut write_buf, count);
                                        }
                                    }
                                }
                            }
                        }
                        in_multi = false;
                        tx_error = false;
                        tx_queue.clear();
                        watched.clear();
                        continue;
                    } else if cmd_eq_fast(args[0], b"DISCARD") {
                        if !in_multi {
                            resp::write_error(&mut write_buf, "ERR DISCARD without MULTI");
                        } else {
                            in_multi = false;
                            tx_error = false;
                            tx_queue.clear();
                            watched.clear();
                            resp::write_ok(&mut write_buf);
                        }
                        continue;
                    } else if cmd_eq_fast(args[0], b"WATCH") {
                        if in_multi {
                            resp::write_error(&mut write_buf, "ERR Command 'watch' not allowed inside a transaction");
                            tx_error = true;
                        } else if args.len() < 2 {
                            resp::write_error(&mut write_buf, "ERR wrong number of arguments for 'watch' command");
                        } else {
                            for key_bytes in &args[1..] {
                                let key = std::str::from_utf8(key_bytes).unwrap_or("").to_string();
                                let shard_idx = store.shard_for_key(key_bytes);
                                let version = store.shard_version(shard_idx);
                                watched.push((key, shard_idx, version));
                            }
                            resp::write_ok(&mut write_buf);
                        }
                        continue;
                    } else if cmd_eq_fast(args[0], b"UNWATCH") {
                        watched.clear();
                        resp::write_ok(&mut write_buf);
                        continue;
                    }

                    if in_multi {
                        if cmd_eq_fast(args[0], b"SUBSCRIBE") || cmd_eq_fast(args[0], b"PSUBSCRIBE") {
                            resp::write_error(&mut write_buf, &format!(
                                "ERR Command '{}' not allowed inside a transaction",
                                std::str::from_utf8(args[0]).unwrap_or("subscribe").to_lowercase()
                            ));
                            tx_error = true;
                        } else if !cmd::is_known_command(args[0]) {
                            let cmd_name = std::str::from_utf8(args[0]).unwrap_or("unknown").to_lowercase();
                            resp::write_error(&mut write_buf, &format!("ERR unknown command '{cmd_name}'"));
                            tx_error = true;
                        } else {
                            match cmd::validate_args(args) {
                                Ok(()) => {
                                    let owned: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
                                    tx_queue.push(owned);
                                    resp::write_queued(&mut write_buf);
                                }
                                Err(e) => {
                                    resp::write_error(&mut write_buf, &e);
                                    tx_error = true;
                                }
                            }
                        }
                        continue;
                    }

                    match cmd::execute(&store, &broker, args, &mut write_buf, now) {
                        CmdResult::Written => {}
                        CmdResult::Authenticated => {
                            authenticated = true;
                        }
                        CmdResult::Subscribe { channels } => {
                            for ch in &channels {
                                let rx = broker.subscribe(ch).await;
                                subscriptions.insert(ch.clone(), rx);
                                resp::write_array_header(&mut write_buf, 3);
                                resp::write_bulk(&mut write_buf, "subscribe");
                                resp::write_bulk(&mut write_buf, ch);
                                resp::write_integer(&mut write_buf, subscriptions.len() as i64);
                            }
                            sub_mode = true;
                            break;
                        }
                        CmdResult::Publish { channel, message } => {
                            let count = broker.publish(&channel, message).await;
                            resp::write_integer(&mut write_buf, count);
                        }
                    }
                }
            } else {
                let cmd_count = commands.len();
                TOTAL_COMMANDS.fetch_add(cmd_count, Ordering::Relaxed);

                let mut has_special = in_multi;
                let mut all_single_key_rw = true;
                for args in &commands {
                    if !authenticated
                        && !cmd_eq_fast(args[0], b"AUTH")
                        && !cmd_eq_fast(args[0], b"PING")
                        && !cmd_eq_fast(args[0], b"QUIT")
                    {
                        has_special = true;
                        break;
                    }
                    if cmd_eq_fast(args[0], b"SUBSCRIBE")
                        || cmd_eq_fast(args[0], b"PUBLISH")
                        || cmd_eq_fast(args[0], b"AUTH")
                        || is_tx_cmd(args[0])
                    {
                        has_special = true;
                        break;
                    }
                    if args.len() < 2
                        || cmd_eq_fast(args[0], b"MGET")
                        || cmd_eq_fast(args[0], b"MSET")
                        || cmd_eq_fast(args[0], b"DEL")
                        || cmd_eq_fast(args[0], b"EXISTS")
                        || cmd_eq_fast(args[0], b"KEYS")
                        || cmd_eq_fast(args[0], b"SCAN")
                        || cmd_eq_fast(args[0], b"FLUSHDB")
                        || cmd_eq_fast(args[0], b"FLUSHALL")
                        || cmd_eq_fast(args[0], b"DBSIZE")
                        || cmd_eq_fast(args[0], b"SAVE")
                        || cmd_eq_fast(args[0], b"INFO")
                        || cmd_eq_fast(args[0], b"RENAME")
                        || cmd_eq_fast(args[0], b"SUNION")
                        || cmd_eq_fast(args[0], b"SINTER")
                        || cmd_eq_fast(args[0], b"SDIFF")
                        || cmd_eq_fast(args[0], b"ZUNIONSTORE")
                        || cmd_eq_fast(args[0], b"ZINTERSTORE")
                        || cmd_eq_fast(args[0], b"ZDIFFSTORE")
                    {
                        all_single_key_rw = false;
                    }
                }

                if has_special || !all_single_key_rw {
                    for args in &commands {
                        if !authenticated
                            && !cmd_eq_fast(args[0], b"AUTH")
                            && !cmd_eq_fast(args[0], b"PING")
                            && !cmd_eq_fast(args[0], b"QUIT")
                        {
                            resp::write_error(&mut write_buf, "NOAUTH Authentication required");
                            continue;
                        }

                        if cmd_eq_fast(args[0], b"MULTI") {
                            if in_multi {
                                let cmd_name = std::str::from_utf8(args[0]).unwrap_or("multi").to_lowercase();
                                resp::write_error(&mut write_buf, &format!("ERR Command '{}' not allowed inside a transaction", cmd_name));
                                tx_error = true;
                            } else {
                                in_multi = true;
                                tx_error = false;
                                resp::write_ok(&mut write_buf);
                            }
                            continue;
                        } else if cmd_eq_fast(args[0], b"EXEC") {
                            if !in_multi {
                                resp::write_error(&mut write_buf, "ERR EXEC without MULTI");
                            } else if tx_error {
                                resp::write_error(&mut write_buf, "EXECABORT Transaction discarded because of previous errors.");
                            } else {
                                let mut aborted = false;
                                for (_, shard_idx, version) in &watched {
                                    if store.shard_version(*shard_idx) != *version {
                                        aborted = true;
                                        break;
                                    }
                                }
                                if aborted {
                                    resp::write_null_array(&mut write_buf);
                                } else {
                                    let queue = std::mem::take(&mut tx_queue);
                                    resp::write_array_header(&mut write_buf, queue.len());
                                    for owned_args in &queue {
                                        let refs: Vec<&[u8]> = owned_args.iter().map(|v| v.as_slice()).collect();
                                        match cmd::execute(&store, &broker, &refs, &mut write_buf, now) {
                                            CmdResult::Written => {}
                                            CmdResult::Authenticated => { authenticated = true; }
                                            CmdResult::Subscribe { .. } => {
                                                resp::write_error(&mut write_buf, "ERR Command 'subscribe' not allowed inside a transaction");
                                            }
                                            CmdResult::Publish { channel, message } => {
                                                let count = broker.publish(&channel, message).await;
                                                resp::write_integer(&mut write_buf, count);
                                            }
                                        }
                                    }
                                }
                            }
                            in_multi = false;
                            tx_error = false;
                            tx_queue.clear();
                            watched.clear();
                            continue;
                        } else if cmd_eq_fast(args[0], b"DISCARD") {
                            if !in_multi {
                                resp::write_error(&mut write_buf, "ERR DISCARD without MULTI");
                            } else {
                                in_multi = false;
                                tx_error = false;
                                tx_queue.clear();
                                watched.clear();
                                resp::write_ok(&mut write_buf);
                            }
                            continue;
                        } else if cmd_eq_fast(args[0], b"WATCH") {
                            if in_multi {
                                resp::write_error(&mut write_buf, "ERR Command 'watch' not allowed inside a transaction");
                                tx_error = true;
                            } else if args.len() < 2 {
                                resp::write_error(&mut write_buf, "ERR wrong number of arguments for 'watch' command");
                            } else {
                                for key_bytes in &args[1..] {
                                    let key = std::str::from_utf8(key_bytes).unwrap_or("").to_string();
                                    let shard_idx = store.shard_for_key(key_bytes);
                                    let version = store.shard_version(shard_idx);
                                    watched.push((key, shard_idx, version));
                                }
                                resp::write_ok(&mut write_buf);
                            }
                            continue;
                        } else if cmd_eq_fast(args[0], b"UNWATCH") {
                            watched.clear();
                            resp::write_ok(&mut write_buf);
                            continue;
                        }

                        if in_multi {
                            if cmd_eq_fast(args[0], b"SUBSCRIBE") || cmd_eq_fast(args[0], b"PSUBSCRIBE") {
                                resp::write_error(&mut write_buf, &format!(
                                    "ERR Command '{}' not allowed inside a transaction",
                                    std::str::from_utf8(args[0]).unwrap_or("subscribe").to_lowercase()
                                ));
                                tx_error = true;
                            } else {
                                match cmd::validate_args(args) {
                                    Ok(()) => {
                                        let owned: Vec<Vec<u8>> = args.iter().map(|a| a.to_vec()).collect();
                                        tx_queue.push(owned);
                                        resp::write_queued(&mut write_buf);
                                    }
                                    Err(e) => {
                                        resp::write_error(&mut write_buf, &e);
                                        tx_error = true;
                                    }
                                }
                            }
                            continue;
                        }

                        match cmd::execute(&store, &broker, args, &mut write_buf, now) {
                            CmdResult::Written => {}
                            CmdResult::Authenticated => {
                                authenticated = true;
                            }
                            CmdResult::Subscribe { channels } => {
                                for ch in &channels {
                                    let rx = broker.subscribe(ch).await;
                                    subscriptions.insert(ch.clone(), rx);
                                    resp::write_array_header(&mut write_buf, 3);
                                    resp::write_bulk(&mut write_buf, "subscribe");
                                    resp::write_bulk(&mut write_buf, ch);
                                    resp::write_integer(&mut write_buf, subscriptions.len() as i64);
                                }
                                sub_mode = true;
                                break;
                            }
                            CmdResult::Publish { channel, message } => {
                                let count = broker.publish(&channel, message).await;
                                resp::write_integer(&mut write_buf, count);
                            }
                        }
                    }
                } else {
                    const IS_SIMPLE_SET: u8 = 1;
                    const IS_SIMPLE_GET: u8 = 2;

                    let mut shards: Vec<u32> = Vec::with_capacity(cmd_count);
                    let mut flags: Vec<u8> = Vec::with_capacity(cmd_count);
                    for args in &commands {
                        shards.push(store.shard_for_key(args[1]) as u32);
                        flags.push(if cmd_eq_fast(args[0], b"SET") && args.len() == 3 {
                            IS_SIMPLE_SET
                        } else if cmd_eq_fast(args[0], b"GET") && args.len() == 2 {
                            IS_SIMPLE_GET
                        } else {
                            0
                        });
                    }

                    let mut i = 0usize;
                    while i < cmd_count {
                        let shard_idx = shards[i];
                        let mut batch_end = i + 1;
                        while batch_end < cmd_count && shards[batch_end] == shard_idx {
                            batch_end += 1;
                        }

                        if batch_end == i + 1 {
                            let f = flags[i];
                            if f == IS_SIMPLE_SET {
                                let mut shard = store.lock_write_shard(shard_idx as usize);
                                shard.version += 1;
                                Store::set_on_shard(
                                    &mut shard.data,
                                    commands[i][1],
                                    commands[i][2],
                                    None,
                                    now,
                                );
                                write_buf.extend_from_slice(resp::OK);
                            } else if f == IS_SIMPLE_GET {
                                let shard = store.lock_read_shard(shard_idx as usize);
                                Store::get_and_write(
                                    &shard.data,
                                    commands[i][1],
                                    now,
                                    &mut write_buf,
                                );
                            } else {
                                cmd::execute(&store, &broker, &commands[i], &mut write_buf, now);
                            }
                        } else {
                            let all_simple = flags[i..batch_end].iter().all(|&f| f != 0);
                            if all_simple {
                                let has_writes = flags[i..batch_end].contains(&IS_SIMPLE_SET);
                                if has_writes {
                                    let mut shard = store.lock_write_shard(shard_idx as usize);
                                    shard.version += 1;
                                    for (&f, args) in
                                        flags[i..batch_end].iter().zip(&commands[i..batch_end])
                                    {
                                        if f == IS_SIMPLE_SET {
                                            Store::set_on_shard(
                                                &mut shard.data,
                                                args[1],
                                                args[2],
                                                None,
                                                now,
                                            );
                                            write_buf.extend_from_slice(resp::OK);
                                        } else {
                                            Store::get_and_write(
                                                &shard.data,
                                                args[1],
                                                now,
                                                &mut write_buf,
                                            );
                                        }
                                    }
                                } else {
                                    let shard = store.lock_read_shard(shard_idx as usize);
                                    for args in &commands[i..batch_end] {
                                        Store::get_and_write(
                                            &shard.data,
                                            args[1],
                                            now,
                                            &mut write_buf,
                                        );
                                    }
                                }
                            } else {
                                for args in &commands[i..batch_end] {
                                    cmd::execute(&store, &broker, args, &mut write_buf, now);
                                }
                            }
                        }

                        i = batch_end;
                    }
                }
            }

            let _ = pending.split_to(consumed);

            if !write_buf.is_empty() {
                socket.write_all(&write_buf).await?;
                write_buf.clear();
            }
        }
    }
}
