use bytes::{Bytes, BytesMut};
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use crate::pubsub::Broker;
use crate::resp;
use crate::store::{Entry, Store, StoreValue};
use crate::{CONNECTED_CLIENTS, START_TIME, TOTAL_COMMANDS};

pub enum CmdResult {
    Written,
    Authenticated,
    Subscribe { channels: Vec<String> },
    Publish { channel: String, message: String },
}

fn is_restricted() -> bool {
    std::env::var("LUX_RESTRICTED").is_ok_and(|v| v == "1" || v == "true")
}

#[inline(always)]
fn cmd_eq(input: &[u8], expected: &[u8]) -> bool {
    input.len() == expected.len()
        && input
            .iter()
            .zip(expected)
            .all(|(a, b)| a.to_ascii_uppercase() == *b)
}

#[inline(always)]
fn arg_str(arg: &[u8]) -> &str {
    std::str::from_utf8(arg).unwrap_or("")
}

fn parse_u64(arg: &[u8]) -> Result<u64, ()> {
    arg_str(arg).parse::<u64>().map_err(|_| ())
}

fn parse_i64(arg: &[u8]) -> Result<i64, ()> {
    arg_str(arg).parse::<i64>().map_err(|_| ())
}

pub fn execute(
    store: &Store,
    _broker: &Broker,
    args: &[&[u8]],
    out: &mut BytesMut,
    now: Instant,
) -> CmdResult {
    if args.is_empty() {
        resp::write_error(out, "ERR no command");
        return CmdResult::Written;
    }

    let cmd = args[0];

    if cmd_eq(cmd, b"AUTH") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'auth' command");
            return CmdResult::Written;
        }
        let expected = std::env::var("LUX_PASSWORD").unwrap_or_default();
        if expected.is_empty() {
            resp::write_error(out, "ERR Client sent AUTH, but no password is set");
        } else if arg_str(args[1]) == expected {
            resp::write_ok(out);
            return CmdResult::Authenticated;
        } else {
            resp::write_error(out, "WRONGPASS invalid password");
        }
        return CmdResult::Written;
    }

    if (cmd_eq(cmd, b"KEYS")
        || cmd_eq(cmd, b"FLUSHALL")
        || cmd_eq(cmd, b"FLUSHDB")
        || cmd_eq(cmd, b"DEBUG"))
        && is_restricted()
    {
        resp::write_error(out, "ERR command disabled in restricted mode");
        return CmdResult::Written;
    }

    if cmd_eq(cmd, b"PING") {
        if args.len() > 1 {
            resp::write_bulk_raw(out, args[1]);
        } else {
            resp::write_pong(out);
        }
    } else if cmd_eq(cmd, b"ECHO") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'echo' command");
        } else {
            resp::write_bulk_raw(out, args[1]);
        }
    } else if cmd_eq(cmd, b"SET") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'set' command");
            return CmdResult::Written;
        }
        let mut ttl = None;
        let mut nx = false;
        let mut xx = false;
        let mut i = 3;
        while i < args.len() {
            if cmd_eq(args[i], b"EX") {
                if i + 1 >= args.len() {
                    resp::write_error(out, "ERR syntax error");
                    return CmdResult::Written;
                }
                match parse_u64(args[i + 1]) {
                    Ok(s) => ttl = Some(Duration::from_secs(s)),
                    Err(_) => {
                        resp::write_error(out, "ERR value is not an integer or out of range");
                        return CmdResult::Written;
                    }
                }
                i += 2;
            } else if cmd_eq(args[i], b"PX") {
                if i + 1 >= args.len() {
                    resp::write_error(out, "ERR syntax error");
                    return CmdResult::Written;
                }
                match parse_u64(args[i + 1]) {
                    Ok(ms) => ttl = Some(Duration::from_millis(ms)),
                    Err(_) => {
                        resp::write_error(out, "ERR value is not an integer or out of range");
                        return CmdResult::Written;
                    }
                }
                i += 2;
            } else if cmd_eq(args[i], b"NX") {
                nx = true;
                i += 1;
            } else if cmd_eq(args[i], b"XX") {
                xx = true;
                i += 1;
            } else {
                resp::write_error(out, "ERR syntax error");
                return CmdResult::Written;
            }
        }
        if nx {
            if store.set_nx(args[1], args[2], now) {
                resp::write_ok(out);
            } else {
                resp::write_null(out);
            }
        } else if xx {
            if store.get(args[1], now).is_some() {
                store.set(args[1], args[2], ttl, now);
                resp::write_ok(out);
            } else {
                resp::write_null(out);
            }
        } else {
            store.set(args[1], args[2], ttl, now);
            resp::write_ok(out);
        }
    } else if cmd_eq(cmd, b"SETNX") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'setnx' command");
            return CmdResult::Written;
        }
        resp::write_integer(
            out,
            if store.set_nx(args[1], args[2], now) {
                1
            } else {
                0
            },
        );
    } else if cmd_eq(cmd, b"SETEX") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'setex' command");
            return CmdResult::Written;
        }
        match parse_i64(args[2]) {
            Ok(secs) if secs <= 0 => {
                resp::write_error(out, "ERR invalid expire time in 'setex' command")
            }
            Ok(secs) => {
                store.set(
                    args[1],
                    args[3],
                    Some(Duration::from_secs(secs as u64)),
                    now,
                );
                resp::write_ok(out);
            }
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"PSETEX") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'psetex' command");
            return CmdResult::Written;
        }
        match parse_u64(args[2]) {
            Ok(ms) => {
                store.set(args[1], args[3], Some(Duration::from_millis(ms)), now);
                resp::write_ok(out);
            }
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"GET") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'get' command");
            return CmdResult::Written;
        }
        resp::write_optional_bulk_raw(out, &store.get(args[1], now));
    } else if cmd_eq(cmd, b"GETSET") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'getset' command");
            return CmdResult::Written;
        }
        resp::write_optional_bulk_raw(out, &store.get_set(args[1], args[2], now));
    } else if cmd_eq(cmd, b"MGET") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'mget' command");
            return CmdResult::Written;
        }
        resp::write_array_header(out, args.len() - 1);
        for key in &args[1..] {
            resp::write_optional_bulk_raw(out, &store.get(key, now));
        }
    } else if cmd_eq(cmd, b"MSET") {
        if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
            resp::write_error(out, "ERR wrong number of arguments for 'mset' command");
            return CmdResult::Written;
        }
        let mut i = 1;
        while i < args.len() {
            store.set(args[i], args[i + 1], None, now);
            i += 2;
        }
        resp::write_ok(out);
    } else if cmd_eq(cmd, b"STRLEN") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'strlen' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.strlen(args[1], now));
    } else if cmd_eq(cmd, b"DEL") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'del' command");
            return CmdResult::Written;
        }
        let keys: Vec<&[u8]> = args[1..].to_vec();
        resp::write_integer(out, store.del(&keys));
    } else if cmd_eq(cmd, b"EXISTS") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'exists' command");
            return CmdResult::Written;
        }
        let keys: Vec<&[u8]> = args[1..].to_vec();
        resp::write_integer(out, store.exists(&keys, now));
    } else if cmd_eq(cmd, b"INCR") {
        if args.len() != 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'incr' command");
            return CmdResult::Written;
        }
        match store.incr(args[1], 1, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"DECR") {
        if args.len() != 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'decr' command");
            return CmdResult::Written;
        }
        match store.incr(args[1], -1, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"INCRBY") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'incrby' command");
            return CmdResult::Written;
        }
        match parse_i64(args[2]) {
            Ok(delta) => match store.incr(args[1], delta, now) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            },
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"DECRBY") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'decrby' command");
            return CmdResult::Written;
        }
        match parse_i64(args[2]) {
            Ok(delta) => match store.incr(args[1], -delta, now) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            },
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"APPEND") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'append' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.append(args[1], args[2], now));
    } else if cmd_eq(cmd, b"KEYS") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'keys' command");
            return CmdResult::Written;
        }
        let keys = store.keys(args[1], now);
        resp::write_bulk_array(out, &keys);
    } else if cmd_eq(cmd, b"SCAN") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'scan' command");
            return CmdResult::Written;
        }
        let cursor = parse_u64(args[1]).unwrap_or(0) as usize;
        let mut pattern: &[u8] = b"*";
        let mut count = 10usize;
        let mut type_filter: Option<&str> = None;
        let mut i = 2;
        while i < args.len() {
            if cmd_eq(args[i], b"MATCH") && i + 1 < args.len() {
                pattern = args[i + 1];
                i += 2;
            } else if cmd_eq(args[i], b"COUNT") && i + 1 < args.len() {
                count = parse_u64(args[i + 1]).unwrap_or(10) as usize;
                i += 2;
            } else if cmd_eq(args[i], b"TYPE") && i + 1 < args.len() {
                type_filter = Some(arg_str(args[i + 1]));
                i += 2;
            } else {
                i += 1;
            }
        }
        let (next_cursor, all_keys) = store.scan(cursor, pattern, count, now);
        let keys: Vec<String> = if let Some(tf) = type_filter {
            all_keys
                .into_iter()
                .filter(|k| store.get_entry_type(k.as_bytes(), now) == Some(tf))
                .collect()
        } else {
            all_keys
        };
        resp::write_array_header(out, 2);
        resp::write_bulk(out, &next_cursor.to_string());
        resp::write_bulk_array(out, &keys);
    } else if cmd_eq(cmd, b"TTL") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'ttl' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.ttl(args[1], now));
    } else if cmd_eq(cmd, b"PTTL") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'pttl' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.pttl(args[1], now));
    } else if cmd_eq(cmd, b"EXPIRE") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'expire' command");
            return CmdResult::Written;
        }
        match parse_u64(args[2]) {
            Ok(secs) => resp::write_integer(
                out,
                if store.expire(args[1], secs, now) {
                    1
                } else {
                    0
                },
            ),
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"PEXPIRE") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'pexpire' command");
            return CmdResult::Written;
        }
        match parse_u64(args[2]) {
            Ok(ms) => resp::write_integer(
                out,
                if store.pexpire(args[1], ms, now) {
                    1
                } else {
                    0
                },
            ),
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"PERSIST") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'persist' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, if store.persist(args[1], now) { 1 } else { 0 });
    } else if cmd_eq(cmd, b"TYPE") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'type' command");
            return CmdResult::Written;
        }
        match store.get_entry_type(args[1], now) {
            Some(t) => resp::write_simple(out, t),
            None => resp::write_simple(out, "none"),
        }
    } else if cmd_eq(cmd, b"RENAME") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'rename' command");
            return CmdResult::Written;
        }
        match store.rename(args[1], args[2], now) {
            Ok(()) => resp::write_ok(out),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"DBSIZE") {
        resp::write_integer(out, store.dbsize(now));
    } else if cmd_eq(cmd, b"FLUSHDB") || cmd_eq(cmd, b"FLUSHALL") {
        store.flushdb();
        resp::write_ok(out);
    } else if cmd_eq(cmd, b"LPUSH") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'lpush' command");
            return CmdResult::Written;
        }
        let vals: Vec<&[u8]> = args[2..].to_vec();
        match store.lpush(args[1], &vals, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"RPUSH") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'rpush' command");
            return CmdResult::Written;
        }
        let vals: Vec<&[u8]> = args[2..].to_vec();
        match store.rpush(args[1], &vals, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"LPOP") {
        if args.len() < 2 || args.len() > 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'lpop' command");
            return CmdResult::Written;
        }
        if args.len() == 3 {
            let count = parse_u64(args[2]).unwrap_or(1) as usize;
            let idx = store.shard_for_key(args[1]);
            let mut shard = store.lock_write_shard(idx);
            shard.version += 1;
            let ks = arg_str(args[1]);
            match shard.data.get_mut(ks) {
                Some(entry) if !entry.is_expired_at(now) => {
                    if let StoreValue::List(list) = &mut entry.value {
                        if count == 0 {
                            resp::write_array_header(out, 0);
                        } else {
                            let n = count.min(list.len());
                            let items: Vec<Bytes> =
                                (0..n).filter_map(|_| list.pop_front()).collect();
                            resp::write_array_header(out, items.len());
                            for item in &items {
                                resp::write_bulk_raw(out, item);
                            }
                        }
                    } else {
                        resp::write_null(out);
                    }
                }
                _ => resp::write_null(out),
            }
        } else {
            resp::write_optional_bulk_raw(out, &store.lpop(args[1], now));
        }
    } else if cmd_eq(cmd, b"RPOP") {
        if args.len() < 2 || args.len() > 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'rpop' command");
            return CmdResult::Written;
        }
        if args.len() == 3 {
            let count = parse_u64(args[2]).unwrap_or(1) as usize;
            let idx = store.shard_for_key(args[1]);
            let mut shard = store.lock_write_shard(idx);
            shard.version += 1;
            let ks = arg_str(args[1]);
            match shard.data.get_mut(ks) {
                Some(entry) if !entry.is_expired_at(now) => {
                    if let StoreValue::List(list) = &mut entry.value {
                        if count == 0 {
                            resp::write_array_header(out, 0);
                        } else {
                            let n = count.min(list.len());
                            let items: Vec<Bytes> =
                                (0..n).filter_map(|_| list.pop_back()).collect();
                            resp::write_array_header(out, items.len());
                            for item in &items {
                                resp::write_bulk_raw(out, item);
                            }
                        }
                    } else {
                        resp::write_null(out);
                    }
                }
                _ => resp::write_null(out),
            }
        } else {
            resp::write_optional_bulk_raw(out, &store.rpop(args[1], now));
        }
    } else if cmd_eq(cmd, b"LLEN") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'llen' command");
            return CmdResult::Written;
        }
        match store.llen(args[1], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"LRANGE") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'lrange' command");
            return CmdResult::Written;
        }
        let start = parse_i64(args[2]).unwrap_or(0);
        let stop = parse_i64(args[3]).unwrap_or(-1);
        match store.lrange(args[1], start, stop, now) {
            Ok(items) => resp::write_bulk_array_raw(out, &items),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"LINDEX") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'lindex' command");
            return CmdResult::Written;
        }
        let index = parse_i64(args[2]).unwrap_or(0);
        resp::write_optional_bulk_raw(out, &store.lindex(args[1], index, now));
    } else if cmd_eq(cmd, b"HSET") || cmd_eq(cmd, b"HMSET") {
        if args.len() < 4 || !(args.len() - 2).is_multiple_of(2) {
            let cmd_name = if cmd_eq(cmd, b"HMSET") {
                "hmset"
            } else {
                "hset"
            };
            resp::write_error(
                out,
                &format!("ERR wrong number of arguments for '{}' command", cmd_name),
            );
            return CmdResult::Written;
        }
        let pairs: Vec<(&[u8], &[u8])> = args[2..].chunks(2).map(|c| (c[0], c[1])).collect();
        match store.hset(args[1], &pairs, now) {
            Ok(n) => {
                if cmd_eq(cmd, b"HMSET") {
                    resp::write_ok(out);
                } else {
                    resp::write_integer(out, n);
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HGET") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'hget' command");
            return CmdResult::Written;
        }
        resp::write_optional_bulk_raw(out, &store.hget(args[1], args[2], now));
    } else if cmd_eq(cmd, b"HMGET") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'hmget' command");
            return CmdResult::Written;
        }
        let fields: Vec<&[u8]> = args[2..].to_vec();
        let results = store.hmget(args[1], &fields, now);
        resp::write_array_header(out, results.len());
        for val in &results {
            resp::write_optional_bulk_raw(out, val);
        }
    } else if cmd_eq(cmd, b"HDEL") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'hdel' command");
            return CmdResult::Written;
        }
        let fields: Vec<&[u8]> = args[2..].to_vec();
        match store.hdel(args[1], &fields, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HGETALL") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'hgetall' command");
            return CmdResult::Written;
        }
        match store.hgetall(args[1], now) {
            Ok(pairs) => {
                resp::write_array_header(out, pairs.len() * 2);
                for (k, v) in &pairs {
                    resp::write_bulk(out, k);
                    resp::write_bulk_raw(out, v);
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HKEYS") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'hkeys' command");
            return CmdResult::Written;
        }
        match store.hkeys(args[1], now) {
            Ok(keys) => resp::write_bulk_array(out, &keys),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HVALS") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'hvals' command");
            return CmdResult::Written;
        }
        match store.hvals(args[1], now) {
            Ok(vals) => resp::write_bulk_array_raw(out, &vals),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HLEN") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'hlen' command");
            return CmdResult::Written;
        }
        match store.hlen(args[1], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HEXISTS") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'hexists' command");
            return CmdResult::Written;
        }
        match store.hexists(args[1], args[2], now) {
            Ok(b) => resp::write_integer(out, if b { 1 } else { 0 }),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HINCRBY") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'hincrby' command");
            return CmdResult::Written;
        }
        match parse_i64(args[3]) {
            Ok(delta) => match store.hincrby(args[1], args[2], delta, now) {
                Ok(n) => resp::write_integer(out, n),
                Err(e) => resp::write_error(out, &e),
            },
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"SADD") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'sadd' command");
            return CmdResult::Written;
        }
        let members: Vec<&[u8]> = args[2..].to_vec();
        match store.sadd(args[1], &members, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SREM") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'srem' command");
            return CmdResult::Written;
        }
        let members: Vec<&[u8]> = args[2..].to_vec();
        match store.srem(args[1], &members, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SMEMBERS") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'smembers' command");
            return CmdResult::Written;
        }
        match store.smembers(args[1], now) {
            Ok(members) => resp::write_bulk_array(out, &members),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SISMEMBER") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'sismember' command");
            return CmdResult::Written;
        }
        match store.sismember(args[1], args[2], now) {
            Ok(b) => resp::write_integer(out, if b { 1 } else { 0 }),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SCARD") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'scard' command");
            return CmdResult::Written;
        }
        match store.scard(args[1], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SUNION") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'sunion' command");
            return CmdResult::Written;
        }
        let keys: Vec<&[u8]> = args[1..].to_vec();
        match store.sunion(&keys, now) {
            Ok(members) => resp::write_bulk_array(out, &members),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SINTER") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'sinter' command");
            return CmdResult::Written;
        }
        let keys: Vec<&[u8]> = args[1..].to_vec();
        match store.sinter(&keys, now) {
            Ok(members) => resp::write_bulk_array(out, &members),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SDIFF") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'sdiff' command");
            return CmdResult::Written;
        }
        let keys: Vec<&[u8]> = args[1..].to_vec();
        match store.sdiff(&keys, now) {
            Ok(members) => resp::write_bulk_array(out, &members),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SAVE") {
        match crate::snapshot::save(store) {
            Ok(n) => resp::write_simple(out, &format!("OK ({n} keys saved)")),
            Err(e) => resp::write_error(out, &format!("ERR snapshot failed: {e}")),
        }
    } else if cmd_eq(cmd, b"INFO") {
        let section = if args.len() > 1 {
            arg_str(args[1]).to_lowercase()
        } else {
            "all".to_string()
        };
        let info = build_info(store, &section, now);
        resp::write_bulk(out, &info);
    } else if cmd_eq(cmd, b"CONFIG") {
        if args.len() > 1 && cmd_eq(args[1], b"GET") {
            resp::write_array_header(out, 0);
        } else {
            resp::write_ok(out);
        }
    } else if cmd_eq(cmd, b"CLIENT") || cmd_eq(cmd, b"SELECT") {
        resp::write_ok(out);
    } else if cmd_eq(cmd, b"COMMAND") {
        if args.len() > 1 && cmd_eq(args[1], b"DOCS") {
            resp::write_array_header(out, 0);
        } else {
            resp::write_ok(out);
        }
    } else if cmd_eq(cmd, b"GETDEL") {
        if args.len() != 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'getdel' command");
            return CmdResult::Written;
        }
        resp::write_optional_bulk_raw(out, &store.getdel(args[1], now));
    } else if cmd_eq(cmd, b"GETEX") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'getex' command");
            return CmdResult::Written;
        }
        let mut ttl = None;
        let mut persist = false;
        let mut i = 2;
        while i < args.len() {
            if cmd_eq(args[i], b"EX") && i + 1 < args.len() {
                ttl = Some(Duration::from_secs(parse_u64(args[i + 1]).unwrap_or(0)));
                i += 2;
            } else if cmd_eq(args[i], b"PX") && i + 1 < args.len() {
                ttl = Some(Duration::from_millis(parse_u64(args[i + 1]).unwrap_or(0)));
                i += 2;
            } else if cmd_eq(args[i], b"EXAT") && i + 1 < args.len() {
                let ts = parse_u64(args[i + 1]).unwrap_or(0);
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();
                if ts > now_ts {
                    ttl = Some(Duration::from_secs(ts - now_ts));
                }
                i += 2;
            } else if cmd_eq(args[i], b"PXAT") && i + 1 < args.len() {
                let ts = parse_u64(args[i + 1]).unwrap_or(0);
                let now_ts = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_millis() as u64;
                if ts > now_ts {
                    ttl = Some(Duration::from_millis(ts - now_ts));
                }
                i += 2;
            } else if cmd_eq(args[i], b"PERSIST") {
                persist = true;
                i += 1;
            } else {
                resp::write_error(out, "ERR syntax error");
                return CmdResult::Written;
            }
        }
        resp::write_optional_bulk_raw(out, &store.getex(args[1], ttl, persist, now));
    } else if cmd_eq(cmd, b"GETRANGE") || cmd_eq(cmd, b"SUBSTR") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'getrange' command");
            return CmdResult::Written;
        }
        let val = store.getrange(
            args[1],
            parse_i64(args[2]).unwrap_or(0),
            parse_i64(args[3]).unwrap_or(-1),
            now,
        );
        resp::write_bulk_raw(out, &val);
    } else if cmd_eq(cmd, b"SETRANGE") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'setrange' command");
            return CmdResult::Written;
        }
        resp::write_integer(
            out,
            store.setrange(
                args[1],
                parse_u64(args[2]).unwrap_or(0) as usize,
                args[3],
                now,
            ),
        );
    } else if cmd_eq(cmd, b"MSETNX") {
        if args.len() < 3 || !(args.len() - 1).is_multiple_of(2) {
            resp::write_error(out, "ERR wrong number of arguments for 'msetnx' command");
            return CmdResult::Written;
        }
        let pairs: Vec<(&[u8], &[u8])> = args[1..].chunks(2).map(|c| (c[0], c[1])).collect();
        resp::write_integer(out, if store.msetnx(&pairs, now) { 1 } else { 0 });
    } else if cmd_eq(cmd, b"UNLINK") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'unlink' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.unlink(&args[1..]));
    } else if cmd_eq(cmd, b"EXPIREAT") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'expireat' command");
            return CmdResult::Written;
        }
        match parse_u64(args[2]) {
            Ok(ts) => resp::write_integer(
                out,
                if store.expireat(args[1], ts, now) {
                    1
                } else {
                    0
                },
            ),
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"PEXPIREAT") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'pexpireat' command");
            return CmdResult::Written;
        }
        match parse_u64(args[2]) {
            Ok(ts) => resp::write_integer(
                out,
                if store.pexpireat(args[1], ts, now) {
                    1
                } else {
                    0
                },
            ),
            Err(_) => resp::write_error(out, "ERR value is not an integer or out of range"),
        }
    } else if cmd_eq(cmd, b"EXPIRETIME") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.expiretime(args[1], now));
    } else if cmd_eq(cmd, b"PEXPIRETIME") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.pexpiretime(args[1], now));
    } else if cmd_eq(cmd, b"LSET") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'lset' command");
            return CmdResult::Written;
        }
        match store.lset(args[1], parse_i64(args[2]).unwrap_or(0), args[3], now) {
            Ok(()) => resp::write_ok(out),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"LINSERT") {
        if args.len() < 5 {
            resp::write_error(out, "ERR wrong number of arguments for 'linsert' command");
            return CmdResult::Written;
        }
        match store.linsert(args[1], cmd_eq(args[2], b"BEFORE"), args[3], args[4], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"LREM") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'lrem' command");
            return CmdResult::Written;
        }
        match store.lrem(args[1], parse_i64(args[2]).unwrap_or(0), args[3], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"LTRIM") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'ltrim' command");
            return CmdResult::Written;
        }
        match store.ltrim(
            args[1],
            parse_i64(args[2]).unwrap_or(0),
            parse_i64(args[3]).unwrap_or(-1),
            now,
        ) {
            Ok(()) => resp::write_ok(out),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"LPUSHX") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'lpushx' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.lpushx(args[1], &args[2..], now));
    } else if cmd_eq(cmd, b"RPUSHX") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'rpushx' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.rpushx(args[1], &args[2..], now));
    } else if cmd_eq(cmd, b"LPOS") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'lpos' command");
            return CmdResult::Written;
        }
        let key = args[1];
        let element = args[2];
        let mut rank = 1i64;
        let mut count = None::<usize>;
        let mut maxlen = 0usize;
        let mut i = 3;
        while i < args.len() {
            if cmd_eq(args[i], b"RANK") && i + 1 < args.len() {
                rank = parse_i64(args[i + 1]).unwrap_or(1);
                if rank == 0 {
                    resp::write_error(out, "ERR RANK can't be zero: use 1 to start from the first match, 2 from the second ... or use negative to start from the end of the list");
                    return CmdResult::Written;
                }
                i += 2;
            } else if cmd_eq(args[i], b"COUNT") && i + 1 < args.len() {
                let c = parse_u64(args[i + 1]).unwrap_or(0) as usize;
                count = Some(c);
                i += 2;
            } else if cmd_eq(args[i], b"MAXLEN") && i + 1 < args.len() {
                maxlen = parse_u64(args[i + 1]).unwrap_or(0) as usize;
                i += 2;
            } else {
                i += 1;
            }
        }
        let idx = store.shard_for_key(key);
        let shard = store.lock_read_shard(idx);
        let ks = arg_str(key);
        match shard.data.get(ks) {
            Some(entry) if !entry.is_expired_at(now) => {
                if let StoreValue::List(list) = &entry.value {
                    let len = if maxlen > 0 {
                        maxlen.min(list.len())
                    } else {
                        list.len()
                    };
                    let mut matches = Vec::new();
                    if rank > 0 {
                        let mut found = 0i64;
                        for (j, item) in list.iter().take(len).enumerate() {
                            if item.as_ref() == element {
                                found += 1;
                                if found >= rank {
                                    matches.push(j as i64);
                                    if let Some(c) = count {
                                        if c > 0 && matches.len() >= c {
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                    } else {
                        let mut found = 0i64;
                        for j in (0..len).rev() {
                            if list[j].as_ref() == element {
                                found += 1;
                                if found >= rank.abs() {
                                    matches.push(j as i64);
                                    if let Some(c) = count {
                                        if c > 0 && matches.len() >= c {
                                            break;
                                        }
                                    } else {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if count.is_some() {
                        resp::write_array_header(out, matches.len());
                        for m in &matches {
                            resp::write_integer(out, *m);
                        }
                    } else if matches.is_empty() {
                        resp::write_null(out);
                    } else {
                        resp::write_integer(out, matches[0]);
                    }
                } else {
                    resp::write_error(out, "WRONGTYPE");
                }
            }
            _ => {
                if count.is_some() {
                    resp::write_array_header(out, 0);
                } else {
                    resp::write_null(out);
                }
            }
        }
    } else if cmd_eq(cmd, b"LMOVE") {
        if args.len() < 5 {
            resp::write_error(out, "ERR wrong number of arguments for 'lmove' command");
            return CmdResult::Written;
        }
        resp::write_optional_bulk_raw(
            out,
            &store.lmove(
                args[1],
                args[2],
                cmd_eq(args[3], b"LEFT"),
                cmd_eq(args[4], b"LEFT"),
                now,
            ),
        );
    } else if cmd_eq(cmd, b"RPOPLPUSH") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'rpoplpush' command");
            return CmdResult::Written;
        }
        resp::write_optional_bulk_raw(out, &store.lmove(args[1], args[2], false, true, now));
    } else if cmd_eq(cmd, b"HSETNX") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'hsetnx' command");
            return CmdResult::Written;
        }
        match store.hsetnx(args[1], args[2], args[3], now) {
            Ok(b) => resp::write_integer(out, if b { 1 } else { 0 }),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HINCRBYFLOAT") {
        if args.len() < 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'hincrbyfloat' command",
            );
            return CmdResult::Written;
        }
        let delta: f64 = match arg_str(args[3]).parse() {
            Ok(d) => d,
            Err(_) => {
                resp::write_error(out, "ERR value is not a valid float");
                return CmdResult::Written;
            }
        };
        match store.hincrbyfloat(args[1], args[2], delta, now) {
            Ok(s) => resp::write_bulk(out, &s),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HSTRLEN") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'hstrlen' command");
            return CmdResult::Written;
        }
        resp::write_integer(out, store.hstrlen(args[1], args[2], now));
    } else if cmd_eq(cmd, b"SPOP") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'spop' command");
            return CmdResult::Written;
        }
        let count = if args.len() > 2 {
            parse_u64(args[2]).unwrap_or(1) as usize
        } else {
            1
        };
        match store.spop(args[1], count, now) {
            Ok(members) => {
                if args.len() <= 2 {
                    if members.is_empty() {
                        resp::write_null(out);
                    } else {
                        resp::write_bulk(out, &members[0]);
                    }
                } else {
                    resp::write_array_header(out, members.len());
                    for m in &members {
                        resp::write_bulk(out, m);
                    }
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SRANDMEMBER") {
        if args.len() < 2 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'srandmember' command",
            );
            return CmdResult::Written;
        }
        let count = if args.len() > 2 {
            parse_i64(args[2]).unwrap_or(1)
        } else {
            0
        };
        match store.srandmember(args[1], if count == 0 { 1 } else { count }, now) {
            Ok(members) => {
                if args.len() <= 2 {
                    if members.is_empty() {
                        resp::write_null(out);
                    } else {
                        resp::write_bulk(out, &members[0]);
                    }
                } else {
                    resp::write_array_header(out, members.len());
                    for m in &members {
                        resp::write_bulk(out, m);
                    }
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SMOVE") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'smove' command");
            return CmdResult::Written;
        }
        match store.smove(args[1], args[2], args[3], now) {
            Ok(b) => resp::write_integer(out, if b { 1 } else { 0 }),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SMISMEMBER") {
        if args.len() < 3 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'smismember' command",
            );
            return CmdResult::Written;
        }
        let results = store.smismember(args[1], &args[2..], now);
        resp::write_array_header(out, results.len());
        for r in results {
            resp::write_integer(out, if r { 1 } else { 0 });
        }
    } else if cmd_eq(cmd, b"SDIFFSTORE") {
        if args.len() < 3 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'sdiffstore' command",
            );
            return CmdResult::Written;
        }
        match store.sdiffstore(args[1], &args[2..], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SINTERSTORE") {
        if args.len() < 3 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'sinterstore' command",
            );
            return CmdResult::Written;
        }
        match store.sinterstore(args[1], &args[2..], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SUNIONSTORE") {
        if args.len() < 3 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'sunionstore' command",
            );
            return CmdResult::Written;
        }
        match store.sunionstore(args[1], &args[2..], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"SINTERCARD") {
        if args.len() < 3 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'sintercard' command",
            );
            return CmdResult::Written;
        }
        let numkeys = parse_u64(args[1]).unwrap_or(0) as usize;
        if 2 + numkeys > args.len() {
            resp::write_error(out, "ERR syntax error");
            return CmdResult::Written;
        }
        match store.sinter(&args[2..2 + numkeys], now) {
            Ok(r) => resp::write_integer(out, r.len() as i64),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"HRANDFIELD") {
        if args.len() < 2 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'hrandfield' command",
            );
            return CmdResult::Written;
        }
        let count = if args.len() > 2 {
            parse_i64(args[2]).unwrap_or(1)
        } else {
            0
        };
        let with_values = args.len() > 3 && cmd_eq(args[3], b"WITHVALUES");
        let idx = store.shard_for_key(args[1]);
        let shard = store.lock_read_shard(idx);
        let ks = arg_str(args[1]);
        match shard.data.get(ks) {
            Some(entry) if !entry.is_expired_at(now) => {
                if let StoreValue::Hash(map) = &entry.value {
                    let abs = if count == 0 {
                        1usize
                    } else {
                        count.unsigned_abs() as usize
                    };
                    let all: Vec<_> = map.iter().collect();
                    let seed = now.elapsed().as_nanos() as usize;
                    let start = if all.is_empty() { 0 } else { seed % all.len() };
                    let fields: Vec<_> = all
                        .iter()
                        .cycle()
                        .skip(start)
                        .take(abs.min(all.len()))
                        .collect();
                    if args.len() <= 2 {
                        if fields.is_empty() {
                            resp::write_null(out);
                        } else {
                            resp::write_bulk(out, fields[0].0);
                        }
                    } else if with_values {
                        resp::write_array_header(out, fields.len() * 2);
                        for (k, v) in &fields {
                            resp::write_bulk(out, k);
                            resp::write_bulk_raw(out, v);
                        }
                    } else {
                        resp::write_array_header(out, fields.len());
                        for (k, _) in &fields {
                            resp::write_bulk(out, k);
                        }
                    }
                } else {
                    resp::write_error(out, "WRONGTYPE");
                }
            }
            _ => {
                if args.len() <= 2 {
                    resp::write_null(out);
                } else {
                    resp::write_array_header(out, 0);
                }
            }
        }
    } else if cmd_eq(cmd, b"HSCAN") || cmd_eq(cmd, b"SSCAN") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments");
            return CmdResult::Written;
        }
        let cursor = parse_u64(args[2]).unwrap_or(0) as usize;
        let mut count = 10usize;
        let mut i = 3;
        while i < args.len() {
            if cmd_eq(args[i], b"COUNT") && i + 1 < args.len() {
                count = parse_u64(args[i + 1]).unwrap_or(10) as usize;
                i += 2;
            } else {
                i += 1;
            }
        }
        let idx = store.shard_for_key(args[1]);
        let shard = store.lock_read_shard(idx);
        let ks = arg_str(args[1]);
        match shard.data.get(ks) {
            Some(entry) if !entry.is_expired_at(now) => {
                if cmd_eq(cmd, b"HSCAN") {
                    if let StoreValue::Hash(map) = &entry.value {
                        let all: Vec<_> = map.iter().collect();
                        let s = cursor.min(all.len());
                        let e = (s + count).min(all.len());
                        let next = if e >= all.len() { 0 } else { e };
                        resp::write_array_header(out, 2);
                        resp::write_bulk(out, &next.to_string());
                        resp::write_array_header(out, (e - s) * 2);
                        for (k, v) in &all[s..e] {
                            resp::write_bulk(out, k);
                            resp::write_bulk_raw(out, v);
                        }
                    } else {
                        resp::write_error(out, "WRONGTYPE");
                    }
                } else if let StoreValue::Set(set) = &entry.value {
                    let all: Vec<_> = set.iter().collect();
                    let s = cursor.min(all.len());
                    let e = (s + count).min(all.len());
                    let next = if e >= all.len() { 0 } else { e };
                    resp::write_array_header(out, 2);
                    resp::write_bulk(out, &next.to_string());
                    resp::write_array_header(out, e - s);
                    for m in &all[s..e] {
                        resp::write_bulk(out, m);
                    }
                } else {
                    resp::write_error(out, "WRONGTYPE");
                }
            }
            _ => {
                resp::write_array_header(out, 2);
                resp::write_bulk(out, "0");
                resp::write_array_header(out, 0);
            }
        }
    } else if cmd_eq(cmd, b"INCRBYFLOAT") {
        if args.len() != 3 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'incrbyfloat' command",
            );
            return CmdResult::Written;
        }
        let delta_str = arg_str(args[2]);
        if delta_str.contains(' ') {
            resp::write_error(out, "ERR value is not a valid float");
            return CmdResult::Written;
        }
        let delta: f64 = match delta_str.parse::<f64>() {
            Ok(d) if d.is_nan() || d.is_infinite() => {
                resp::write_error(out, "ERR increment would produce NaN or Infinity");
                return CmdResult::Written;
            }
            Ok(d) => d,
            Err(_) => {
                resp::write_error(out, "ERR value is not a valid float");
                return CmdResult::Written;
            }
        };
        let idx = store.shard_for_key(args[1]);
        let mut shard = store.lock_write_shard(idx);
        let ks = arg_str(args[1]);
        let current: f64 = match shard.data.get(ks) {
            Some(e) if !e.is_expired_at(now) => match &e.value {
                StoreValue::Str(s) => {
                    let ss = std::str::from_utf8(s).unwrap_or("");
                    if ss.contains(' ') {
                        resp::write_error(out, "ERR value is not a valid float");
                        return CmdResult::Written;
                    }
                    match ss.parse::<f64>() {
                        Ok(v) if v.is_nan() || v.is_infinite() => {
                            resp::write_error(out, "ERR value is not a valid float");
                            return CmdResult::Written;
                        }
                        Ok(v) => v,
                        Err(_) => {
                            resp::write_error(out, "ERR value is not a valid float");
                            return CmdResult::Written;
                        }
                    }
                }
                _ => {
                    resp::write_error(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                    return CmdResult::Written;
                }
            },
            _ => 0.0,
        };
        let new_val = current + delta;
        if new_val.is_nan() || new_val.is_infinite() {
            resp::write_error(out, "ERR increment would produce NaN or Infinity");
            return CmdResult::Written;
        }
        let new_str = if new_val.fract() == 0.0 && new_val.abs() < 1e15 {
            format!("{}", new_val as i64)
        } else {
            format!("{}", new_val)
        };
        let expires_at = shard.data.get(ks).and_then(|e| e.expires_at);
        shard.version += 1;
        shard.data.insert(
            ks.to_string(),
            Entry {
                value: StoreValue::Str(Bytes::from(new_str.clone())),
                expires_at,
            },
        );
        resp::write_bulk(out, &new_str);
    } else if cmd_eq(cmd, b"TIME") {
        let now_sys = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default();
        resp::write_array_header(out, 2);
        resp::write_bulk(out, &now_sys.as_secs().to_string());
        resp::write_bulk(out, &(now_sys.subsec_micros()).to_string());
    } else if cmd_eq(cmd, b"RENAMENX") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'renamenx' command");
            return CmdResult::Written;
        }
        if store.get(args[2], now).is_some() {
            resp::write_integer(out, 0);
        } else {
            match store.rename(args[1], args[2], now) {
                Ok(()) => resp::write_integer(out, 1),
                Err(e) => resp::write_error(out, &e),
            }
        }
    } else if cmd_eq(cmd, b"RANDOMKEY") {
        let mut found = None;
        for i in 0..store.shard_count() {
            let shard = store.lock_read_shard(i);
            if let Some((k, entry)) = shard.data.iter().next() {
                if !entry.is_expired_at(now) {
                    found = Some(k.clone());
                    break;
                }
            }
        }
        match found {
            Some(k) => resp::write_bulk(out, &k),
            None => resp::write_null(out),
        }
    } else if cmd_eq(cmd, b"HELLO") {
        resp::write_array_header(out, 14);
        resp::write_bulk(out, "server");
        resp::write_bulk(out, "lux");
        resp::write_bulk(out, "version");
        resp::write_bulk(out, env!("CARGO_PKG_VERSION"));
        resp::write_bulk(out, "proto");
        resp::write_integer(out, 2);
        resp::write_bulk(out, "id");
        resp::write_integer(out, 1);
        resp::write_bulk(out, "mode");
        resp::write_bulk(out, "standalone");
        resp::write_bulk(out, "role");
        resp::write_bulk(out, "master");
        resp::write_bulk(out, "modules");
        resp::write_array_header(out, 0);
    } else if cmd_eq(cmd, b"PSUBSCRIBE") || cmd_eq(cmd, b"PUNSUBSCRIBE") {
        resp::write_ok(out);
    } else if cmd_eq(cmd, b"COPY") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'copy' command");
            return CmdResult::Written;
        }
        let replace = args.iter().any(|a| cmd_eq(a, b"REPLACE"));
        if !replace && store.get(args[2], now).is_some() {
            resp::write_integer(out, 0);
        } else {
            match store.get(args[1], now) {
                Some(val) => {
                    store.set(args[2], &val, None, now);
                    resp::write_integer(out, 1);
                }
                None => resp::write_integer(out, 0),
            }
        }
    } else if cmd_eq(cmd, b"FUNCTION")
        || cmd_eq(cmd, b"DEBUG")
        || cmd_eq(cmd, b"WAIT")
        || cmd_eq(cmd, b"RESET")
        || cmd_eq(cmd, b"LATENCY")
        || cmd_eq(cmd, b"SWAPDB")
    {
        resp::write_ok(out);
    } else if cmd_eq(cmd, b"OBJECT") {
        if args.len() > 2 && cmd_eq(args[1], b"REFCOUNT") {
            resp::write_integer(out, 1);
        } else if args.len() > 2 && cmd_eq(args[1], b"ENCODING") {
            let key = args[2];
            let idx = store.shard_for_key(key);
            let shard = store.lock_read_shard(idx);
            let ks = arg_str(key);
            match shard.data.get(ks) {
                Some(entry) if !entry.is_expired_at(now) => {
                    let enc = match &entry.value {
                        StoreValue::Str(s) => {
                            if let Ok(ss) = std::str::from_utf8(s) {
                                if ss.parse::<i64>().is_ok() {
                                    "int"
                                } else if s.len() <= 44 {
                                    "embstr"
                                } else {
                                    "raw"
                                }
                            } else {
                                "raw"
                            }
                        }
                        StoreValue::List(l) => {
                            if l.len() <= 128 {
                                "listpack"
                            } else {
                                "quicklist"
                            }
                        }
                        StoreValue::Hash(h) => {
                            if h.len() < 128
                                && h.iter().all(|(k, v)| k.len() <= 64 && v.len() <= 64)
                            {
                                "listpack"
                            } else {
                                "hashtable"
                            }
                        }
                        StoreValue::Set(s) => {
                            if s.iter().all(|m| m.parse::<i64>().is_ok()) && s.len() <= 512 {
                                "intset"
                            } else if s.len() < 128 {
                                "listpack"
                            } else {
                                "hashtable"
                            }
                        }
                        StoreValue::SortedSet(_, scores) => {
                            if scores.len() < 128 {
                                "listpack"
                            } else {
                                "skiplist"
                            }
                        }
                    };
                    resp::write_bulk(out, enc);
                }
                _ => resp::write_error(out, "ERR no such key"),
            }
        } else if args.len() > 2 && cmd_eq(args[1], b"IDLETIME") {
            resp::write_integer(out, 0);
        } else {
            resp::write_ok(out);
        }
    } else if cmd_eq(cmd, b"MEMORY") {
        if args.len() > 2 && cmd_eq(args[1], b"USAGE") {
            let key = args[2];
            let idx = store.shard_for_key(key);
            let shard = store.lock_read_shard(idx);
            let ks = arg_str(key);
            match shard.data.get(ks) {
                Some(entry) if !entry.is_expired_at(now) => {
                    let size = ks.len()
                        + 64
                        + match &entry.value {
                            StoreValue::Str(s) => s.len() + 16,
                            StoreValue::List(l) => l.iter().map(|b| b.len() + 16).sum::<usize>(),
                            StoreValue::Hash(h) => {
                                h.iter().map(|(k, v)| k.len() + v.len() + 32).sum::<usize>()
                            }
                            StoreValue::Set(s) => s.iter().map(|m| m.len() + 16).sum::<usize>(),
                            StoreValue::SortedSet(_, scores) => {
                                scores.iter().map(|(m, _)| m.len() + 48).sum::<usize>()
                            }
                        };
                    resp::write_integer(out, size as i64);
                }
                _ => resp::write_null(out),
            }
        } else {
            resp::write_ok(out);
        }
    } else if cmd_eq(cmd, b"UNSUBSCRIBE") {
        resp::write_ok(out);
    } else if cmd_eq(cmd, b"BGSAVE") {
        match crate::snapshot::save(store) {
            Ok(_) => resp::write_simple(out, "Background saving started"),
            Err(e) => resp::write_error(out, &format!("ERR snapshot failed: {e}")),
        }
    } else if cmd_eq(cmd, b"LASTSAVE") {
        resp::write_integer(
            out,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_secs() as i64,
        );
    } else if cmd_eq(cmd, b"PUBLISH") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'publish' command");
            return CmdResult::Written;
        }
        return CmdResult::Publish {
            channel: arg_str(args[1]).to_string(),
            message: arg_str(args[2]).to_string(),
        };
    } else if cmd_eq(cmd, b"SUBSCRIBE") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'subscribe' command");
            return CmdResult::Written;
        }
        return CmdResult::Subscribe {
            channels: args[1..].iter().map(|a| arg_str(a).to_string()).collect(),
        };
    } else if cmd_eq(cmd, b"ZADD") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'zadd' command");
            return CmdResult::Written;
        }
        let mut nx = false;
        let mut xx = false;
        let mut gt = false;
        let mut lt = false;
        let mut ch = false;
        let mut i = 2;
        while i < args.len() {
            if cmd_eq(args[i], b"NX") {
                nx = true;
                i += 1;
            } else if cmd_eq(args[i], b"XX") {
                xx = true;
                i += 1;
            } else if cmd_eq(args[i], b"GT") {
                gt = true;
                i += 1;
            } else if cmd_eq(args[i], b"LT") {
                lt = true;
                i += 1;
            } else if cmd_eq(args[i], b"CH") {
                ch = true;
                i += 1;
            } else {
                break;
            }
        }
        if !(args.len() - i).is_multiple_of(2) || i >= args.len() {
            resp::write_error(out, "ERR syntax error");
            return CmdResult::Written;
        }
        let mut members = Vec::new();
        while i + 1 < args.len() {
            let score: f64 = match arg_str(args[i]).parse() {
                Ok(s) => s,
                Err(_) => {
                    resp::write_error(out, "ERR value is not a valid float");
                    return CmdResult::Written;
                }
            };
            members.push((args[i + 1], score));
            i += 2;
        }
        match store.zadd(args[1], &members, nx, xx, gt, lt, ch, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZSCORE") {
        if args.len() != 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'zscore' command");
            return CmdResult::Written;
        }
        match store.zscore(args[1], args[2], now) {
            Ok(Some(s)) => {
                let ss = format_float(s);
                resp::write_bulk(out, &ss);
            }
            Ok(None) => resp::write_null(out),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZRANK") {
        if args.len() != 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'zrank' command");
            return CmdResult::Written;
        }
        match store.zrank(args[1], args[2], false, now) {
            Ok(Some(r)) => resp::write_integer(out, r),
            Ok(None) => resp::write_null(out),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZREVRANK") {
        if args.len() != 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'zrevrank' command");
            return CmdResult::Written;
        }
        match store.zrank(args[1], args[2], true, now) {
            Ok(Some(r)) => resp::write_integer(out, r),
            Ok(None) => resp::write_null(out),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZREM") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'zrem' command");
            return CmdResult::Written;
        }
        let members: Vec<&[u8]> = args[2..].to_vec();
        match store.zrem(args[1], &members, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZCARD") {
        if args.len() != 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'zcard' command");
            return CmdResult::Written;
        }
        match store.zcard(args[1], now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZRANGE") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'zrange' command");
            return CmdResult::Written;
        }
        let mut reverse = false;
        let mut with_scores = false;
        let mut byscore = false;
        let mut bylex = false;
        let mut offset: Option<usize> = None;
        let mut count: Option<usize> = None;
        let mut i = 4;
        while i < args.len() {
            if cmd_eq(args[i], b"REV") {
                reverse = true;
                i += 1;
            } else if cmd_eq(args[i], b"WITHSCORES") {
                with_scores = true;
                i += 1;
            } else if cmd_eq(args[i], b"BYSCORE") {
                byscore = true;
                i += 1;
            } else if cmd_eq(args[i], b"BYLEX") {
                bylex = true;
                i += 1;
            } else if cmd_eq(args[i], b"LIMIT") && i + 2 < args.len() {
                offset = Some(parse_u64(args[i + 1]).unwrap_or(0) as usize);
                count = Some(parse_u64(args[i + 2]).unwrap_or(0) as usize);
                i += 3;
            } else {
                i += 1;
            }
        }
        if byscore {
            let (min, min_ex) = parse_score_bound(arg_str(args[2]), false);
            let (max, max_ex) = parse_score_bound(arg_str(args[3]), true);
            match store.zrangebyscore(
                args[1],
                min,
                max,
                min_ex,
                max_ex,
                reverse,
                offset,
                count,
                with_scores,
                now,
            ) {
                Ok(items) => write_zset_result(out, &items, with_scores),
                Err(e) => resp::write_error(out, &e),
            }
        } else if bylex {
            match store.zrangebylex(
                args[1],
                arg_str(args[2]),
                arg_str(args[3]),
                offset,
                count,
                reverse,
                now,
            ) {
                Ok(items) => {
                    resp::write_array_header(out, items.len());
                    for m in &items {
                        resp::write_bulk(out, m);
                    }
                }
                Err(e) => resp::write_error(out, &e),
            }
        } else {
            let start = parse_i64(args[2]).unwrap_or(0);
            let stop = parse_i64(args[3]).unwrap_or(-1);
            match store.zrange(args[1], start, stop, reverse, with_scores, now) {
                Ok(items) => write_zset_result(out, &items, with_scores),
                Err(e) => resp::write_error(out, &e),
            }
        }
    } else if cmd_eq(cmd, b"ZREVRANGE") {
        if args.len() < 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'zrevrange' command");
            return CmdResult::Written;
        }
        let with_scores = args.len() > 4 && cmd_eq(args[4], b"WITHSCORES");
        let start = parse_i64(args[2]).unwrap_or(0);
        let stop = parse_i64(args[3]).unwrap_or(-1);
        match store.zrange(args[1], start, stop, true, with_scores, now) {
            Ok(items) => write_zset_result(out, &items, with_scores),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZRANGEBYSCORE") {
        if args.len() < 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zrangebyscore' command",
            );
            return CmdResult::Written;
        }
        let (min, min_ex) = parse_score_bound(arg_str(args[2]), false);
        let (max, max_ex) = parse_score_bound(arg_str(args[3]), true);
        let mut with_scores = false;
        let mut offset: Option<usize> = None;
        let mut count: Option<usize> = None;
        let mut i = 4;
        while i < args.len() {
            if cmd_eq(args[i], b"WITHSCORES") {
                with_scores = true;
                i += 1;
            } else if cmd_eq(args[i], b"LIMIT") && i + 2 < args.len() {
                offset = Some(parse_u64(args[i + 1]).unwrap_or(0) as usize);
                count = Some(parse_u64(args[i + 2]).unwrap_or(0) as usize);
                i += 3;
            } else {
                i += 1;
            }
        }
        match store.zrangebyscore(
            args[1],
            min,
            max,
            min_ex,
            max_ex,
            false,
            offset,
            count,
            with_scores,
            now,
        ) {
            Ok(items) => write_zset_result(out, &items, with_scores),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZREVRANGEBYSCORE") {
        if args.len() < 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zrevrangebyscore' command",
            );
            return CmdResult::Written;
        }
        let (max, max_ex) = parse_score_bound(arg_str(args[2]), true);
        let (min, min_ex) = parse_score_bound(arg_str(args[3]), false);
        let mut with_scores = false;
        let mut offset: Option<usize> = None;
        let mut count: Option<usize> = None;
        let mut i = 4;
        while i < args.len() {
            if cmd_eq(args[i], b"WITHSCORES") {
                with_scores = true;
                i += 1;
            } else if cmd_eq(args[i], b"LIMIT") && i + 2 < args.len() {
                offset = Some(parse_u64(args[i + 1]).unwrap_or(0) as usize);
                count = Some(parse_u64(args[i + 2]).unwrap_or(0) as usize);
                i += 3;
            } else {
                i += 1;
            }
        }
        match store.zrangebyscore(
            args[1],
            min,
            max,
            min_ex,
            max_ex,
            true,
            offset,
            count,
            with_scores,
            now,
        ) {
            Ok(items) => write_zset_result(out, &items, with_scores),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZRANGEBYLEX") {
        if args.len() < 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zrangebylex' command",
            );
            return CmdResult::Written;
        }
        let mut offset: Option<usize> = None;
        let mut count: Option<usize> = None;
        let mut i = 4;
        while i < args.len() {
            if cmd_eq(args[i], b"LIMIT") && i + 2 < args.len() {
                offset = Some(parse_u64(args[i + 1]).unwrap_or(0) as usize);
                count = Some(parse_u64(args[i + 2]).unwrap_or(0) as usize);
                i += 3;
            } else {
                i += 1;
            }
        }
        match store.zrangebylex(
            args[1],
            arg_str(args[2]),
            arg_str(args[3]),
            offset,
            count,
            false,
            now,
        ) {
            Ok(items) => {
                resp::write_array_header(out, items.len());
                for m in &items {
                    resp::write_bulk(out, m);
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZREVRANGEBYLEX") {
        if args.len() < 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zrevrangebylex' command",
            );
            return CmdResult::Written;
        }
        let mut offset: Option<usize> = None;
        let mut count: Option<usize> = None;
        let mut i = 4;
        while i < args.len() {
            if cmd_eq(args[i], b"LIMIT") && i + 2 < args.len() {
                offset = Some(parse_u64(args[i + 1]).unwrap_or(0) as usize);
                count = Some(parse_u64(args[i + 2]).unwrap_or(0) as usize);
                i += 3;
            } else {
                i += 1;
            }
        }
        match store.zrangebylex(
            args[1],
            arg_str(args[3]),
            arg_str(args[2]),
            offset,
            count,
            true,
            now,
        ) {
            Ok(items) => {
                resp::write_array_header(out, items.len());
                for m in &items {
                    resp::write_bulk(out, m);
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZINCRBY") {
        if args.len() != 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'zincrby' command");
            return CmdResult::Written;
        }
        let increment: f64 = match arg_str(args[2]).parse() {
            Ok(d) => d,
            Err(_) => {
                resp::write_error(out, "ERR value is not a valid float");
                return CmdResult::Written;
            }
        };
        match store.zincrby(args[1], args[3], increment, now) {
            Ok(s) => {
                let ss = format_float(s);
                resp::write_bulk(out, &ss);
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZCOUNT") {
        if args.len() != 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'zcount' command");
            return CmdResult::Written;
        }
        let (min, min_ex) = parse_score_bound(arg_str(args[2]), false);
        let (max, max_ex) = parse_score_bound(arg_str(args[3]), true);
        match store.zcount(args[1], min, max, min_ex, max_ex, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZPOPMIN") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'zpopmin' command");
            return CmdResult::Written;
        }
        let count = if args.len() > 2 {
            parse_u64(args[2]).unwrap_or(1) as usize
        } else {
            1
        };
        match store.zpopmin(args[1], count, now) {
            Ok(items) => {
                if args.len() <= 2 && items.len() <= 1 {
                    if items.is_empty() {
                        resp::write_array_header(out, 0);
                    } else {
                        resp::write_array_header(out, 2);
                        resp::write_bulk(out, &items[0].0);
                        resp::write_bulk(out, &format_float(items[0].1));
                    }
                } else {
                    resp::write_array_header(out, items.len() * 2);
                    for (m, s) in &items {
                        resp::write_bulk(out, m);
                        resp::write_bulk(out, &format_float(*s));
                    }
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZPOPMAX") {
        if args.len() < 2 {
            resp::write_error(out, "ERR wrong number of arguments for 'zpopmax' command");
            return CmdResult::Written;
        }
        let count = if args.len() > 2 {
            parse_u64(args[2]).unwrap_or(1) as usize
        } else {
            1
        };
        match store.zpopmax(args[1], count, now) {
            Ok(items) => {
                if args.len() <= 2 && items.len() <= 1 {
                    if items.is_empty() {
                        resp::write_array_header(out, 0);
                    } else {
                        resp::write_array_header(out, 2);
                        resp::write_bulk(out, &items[0].0);
                        resp::write_bulk(out, &format_float(items[0].1));
                    }
                } else {
                    resp::write_array_header(out, items.len() * 2);
                    for (m, s) in &items {
                        resp::write_bulk(out, m);
                        resp::write_bulk(out, &format_float(*s));
                    }
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZUNIONSTORE") {
        if args.len() < 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zunionstore' command",
            );
            return CmdResult::Written;
        }
        let numkeys = parse_u64(args[2]).unwrap_or(0) as usize;
        if 3 + numkeys > args.len() {
            resp::write_error(out, "ERR syntax error");
            return CmdResult::Written;
        }
        let keys: Vec<&[u8]> = args[3..3 + numkeys].to_vec();
        let (weights, aggregate) = parse_zstore_options(&args[3 + numkeys..]);
        match store.zunionstore(args[1], &keys, &weights, &aggregate, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZINTERSTORE") {
        if args.len() < 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zinterstore' command",
            );
            return CmdResult::Written;
        }
        let numkeys = parse_u64(args[2]).unwrap_or(0) as usize;
        if 3 + numkeys > args.len() {
            resp::write_error(out, "ERR syntax error");
            return CmdResult::Written;
        }
        let keys: Vec<&[u8]> = args[3..3 + numkeys].to_vec();
        let (weights, aggregate) = parse_zstore_options(&args[3 + numkeys..]);
        match store.zinterstore(args[1], &keys, &weights, &aggregate, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZDIFFSTORE") {
        if args.len() < 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zdiffstore' command",
            );
            return CmdResult::Written;
        }
        let numkeys = parse_u64(args[2]).unwrap_or(0) as usize;
        if 3 + numkeys > args.len() {
            resp::write_error(out, "ERR syntax error");
            return CmdResult::Written;
        }
        let keys: Vec<&[u8]> = args[3..3 + numkeys].to_vec();
        match store.zdiffstore(args[1], &keys, now) {
            Ok(n) => resp::write_integer(out, n),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZSCAN") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'zscan' command");
            return CmdResult::Written;
        }
        let cursor = parse_u64(args[2]).unwrap_or(0) as usize;
        let mut count = 10usize;
        let mut i = 3;
        while i < args.len() {
            if cmd_eq(args[i], b"COUNT") && i + 1 < args.len() {
                count = parse_u64(args[i + 1]).unwrap_or(10) as usize;
                i += 2;
            } else {
                i += 1;
            }
        }
        let idx = store.shard_for_key(args[1]);
        let shard = store.lock_read_shard(idx);
        let ks = arg_str(args[1]);
        match shard.data.get(ks) {
            Some(entry) if !entry.is_expired_at(now) => {
                if let StoreValue::SortedSet(tree, _) = &entry.value {
                    let all: Vec<_> = tree.keys().collect();
                    let s = cursor.min(all.len());
                    let e = (s + count).min(all.len());
                    let next = if e >= all.len() { 0 } else { e };
                    resp::write_array_header(out, 2);
                    resp::write_bulk(out, &next.to_string());
                    resp::write_array_header(out, (e - s) * 2);
                    for (score, member) in &all[s..e] {
                        resp::write_bulk(out, member);
                        resp::write_bulk(out, &format_float(score.0));
                    }
                } else {
                    resp::write_error(
                        out,
                        "WRONGTYPE Operation against a key holding the wrong kind of value",
                    );
                }
            }
            _ => {
                resp::write_array_header(out, 2);
                resp::write_bulk(out, "0");
                resp::write_array_header(out, 0);
            }
        }
    } else if cmd_eq(cmd, b"ZMSCORE") {
        if args.len() < 3 {
            resp::write_error(out, "ERR wrong number of arguments for 'zmscore' command");
            return CmdResult::Written;
        }
        let members: Vec<&[u8]> = args[2..].to_vec();
        match store.zmscore(args[1], &members, now) {
            Ok(scores) => {
                resp::write_array_header(out, scores.len());
                for s in &scores {
                    match s {
                        Some(v) => resp::write_bulk(out, &format_float(*v)),
                        None => resp::write_null(out),
                    }
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZLEXCOUNT") {
        if args.len() != 4 {
            resp::write_error(out, "ERR wrong number of arguments for 'zlexcount' command");
            return CmdResult::Written;
        }
        match store.zrangebylex(
            args[1],
            arg_str(args[2]),
            arg_str(args[3]),
            None,
            None,
            false,
            now,
        ) {
            Ok(items) => resp::write_integer(out, items.len() as i64),
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZREMRANGEBYRANK") {
        if args.len() != 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zremrangebyrank' command",
            );
            return CmdResult::Written;
        }
        let start = parse_i64(args[2]).unwrap_or(0);
        let stop = parse_i64(args[3]).unwrap_or(-1);
        match store.zrange(args[1], start, stop, false, true, now) {
            Ok(items) => {
                let members: Vec<&[u8]> = items.iter().map(|(m, _)| m.as_bytes()).collect();
                match store.zrem(args[1], &members, now) {
                    Ok(n) => resp::write_integer(out, n),
                    Err(e) => resp::write_error(out, &e),
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZREMRANGEBYSCORE") {
        if args.len() != 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zremrangebyscore' command",
            );
            return CmdResult::Written;
        }
        let (min, min_ex) = parse_score_bound(arg_str(args[2]), false);
        let (max, max_ex) = parse_score_bound(arg_str(args[3]), true);
        match store.zrangebyscore(
            args[1], min, max, min_ex, max_ex, false, None, None, true, now,
        ) {
            Ok(items) => {
                let members: Vec<&[u8]> = items.iter().map(|(m, _)| m.as_bytes()).collect();
                match store.zrem(args[1], &members, now) {
                    Ok(n) => resp::write_integer(out, n),
                    Err(e) => resp::write_error(out, &e),
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else if cmd_eq(cmd, b"ZREMRANGEBYLEX") {
        if args.len() != 4 {
            resp::write_error(
                out,
                "ERR wrong number of arguments for 'zremrangebylex' command",
            );
            return CmdResult::Written;
        }
        match store.zrangebylex(
            args[1],
            arg_str(args[2]),
            arg_str(args[3]),
            None,
            None,
            false,
            now,
        ) {
            Ok(items) => {
                let members: Vec<&[u8]> = items.iter().map(|m| m.as_bytes()).collect();
                match store.zrem(args[1], &members, now) {
                    Ok(n) => resp::write_integer(out, n),
                    Err(e) => resp::write_error(out, &e),
                }
            }
            Err(e) => resp::write_error(out, &e),
        }
    } else {
        resp::write_error(out, &format!("ERR unknown command '{}'", arg_str(cmd)));
    }
    CmdResult::Written
}

pub fn is_known_command(cmd: &[u8]) -> bool {
    cmd_eq(cmd, b"SET") || cmd_eq(cmd, b"GET") || cmd_eq(cmd, b"DEL")
        || cmd_eq(cmd, b"PING") || cmd_eq(cmd, b"ECHO") || cmd_eq(cmd, b"QUIT")
        || cmd_eq(cmd, b"SETNX") || cmd_eq(cmd, b"SETEX") || cmd_eq(cmd, b"PSETEX")
        || cmd_eq(cmd, b"GETSET") || cmd_eq(cmd, b"MGET") || cmd_eq(cmd, b"MSET")
        || cmd_eq(cmd, b"STRLEN") || cmd_eq(cmd, b"EXISTS") || cmd_eq(cmd, b"INCR")
        || cmd_eq(cmd, b"DECR") || cmd_eq(cmd, b"INCRBY") || cmd_eq(cmd, b"DECRBY")
        || cmd_eq(cmd, b"APPEND") || cmd_eq(cmd, b"KEYS") || cmd_eq(cmd, b"SCAN")
        || cmd_eq(cmd, b"TTL") || cmd_eq(cmd, b"PTTL") || cmd_eq(cmd, b"EXPIRE")
        || cmd_eq(cmd, b"PEXPIRE") || cmd_eq(cmd, b"PERSIST") || cmd_eq(cmd, b"TYPE")
        || cmd_eq(cmd, b"RENAME") || cmd_eq(cmd, b"DBSIZE") || cmd_eq(cmd, b"FLUSHDB")
        || cmd_eq(cmd, b"FLUSHALL") || cmd_eq(cmd, b"LPUSH") || cmd_eq(cmd, b"RPUSH")
        || cmd_eq(cmd, b"LPOP") || cmd_eq(cmd, b"RPOP") || cmd_eq(cmd, b"LLEN")
        || cmd_eq(cmd, b"LRANGE") || cmd_eq(cmd, b"LINDEX") || cmd_eq(cmd, b"HSET")
        || cmd_eq(cmd, b"HMSET") || cmd_eq(cmd, b"HGET") || cmd_eq(cmd, b"HMGET")
        || cmd_eq(cmd, b"HDEL") || cmd_eq(cmd, b"HGETALL") || cmd_eq(cmd, b"HKEYS")
        || cmd_eq(cmd, b"HVALS") || cmd_eq(cmd, b"HLEN") || cmd_eq(cmd, b"HEXISTS")
        || cmd_eq(cmd, b"HINCRBY") || cmd_eq(cmd, b"SADD") || cmd_eq(cmd, b"SREM")
        || cmd_eq(cmd, b"SMEMBERS") || cmd_eq(cmd, b"SISMEMBER") || cmd_eq(cmd, b"SCARD")
        || cmd_eq(cmd, b"SUNION") || cmd_eq(cmd, b"SINTER") || cmd_eq(cmd, b"SDIFF")
        || cmd_eq(cmd, b"SAVE") || cmd_eq(cmd, b"INFO") || cmd_eq(cmd, b"CONFIG")
        || cmd_eq(cmd, b"CLIENT") || cmd_eq(cmd, b"SELECT") || cmd_eq(cmd, b"COMMAND")
        || cmd_eq(cmd, b"GETDEL") || cmd_eq(cmd, b"GETEX") || cmd_eq(cmd, b"GETRANGE")
        || cmd_eq(cmd, b"SUBSTR") || cmd_eq(cmd, b"SETRANGE") || cmd_eq(cmd, b"MSETNX")
        || cmd_eq(cmd, b"UNLINK") || cmd_eq(cmd, b"EXPIREAT") || cmd_eq(cmd, b"PEXPIREAT")
        || cmd_eq(cmd, b"EXPIRETIME") || cmd_eq(cmd, b"PEXPIRETIME")
        || cmd_eq(cmd, b"LSET") || cmd_eq(cmd, b"LINSERT") || cmd_eq(cmd, b"LREM")
        || cmd_eq(cmd, b"LTRIM") || cmd_eq(cmd, b"LPUSHX") || cmd_eq(cmd, b"RPUSHX")
        || cmd_eq(cmd, b"LPOS") || cmd_eq(cmd, b"LMOVE") || cmd_eq(cmd, b"RPOPLPUSH")
        || cmd_eq(cmd, b"HSETNX") || cmd_eq(cmd, b"HINCRBYFLOAT") || cmd_eq(cmd, b"HSTRLEN")
        || cmd_eq(cmd, b"SPOP") || cmd_eq(cmd, b"SRANDMEMBER") || cmd_eq(cmd, b"SMOVE")
        || cmd_eq(cmd, b"SMISMEMBER") || cmd_eq(cmd, b"SDIFFSTORE")
        || cmd_eq(cmd, b"SINTERSTORE") || cmd_eq(cmd, b"SUNIONSTORE")
        || cmd_eq(cmd, b"SINTERCARD") || cmd_eq(cmd, b"HRANDFIELD")
        || cmd_eq(cmd, b"HSCAN") || cmd_eq(cmd, b"SSCAN") || cmd_eq(cmd, b"INCRBYFLOAT")
        || cmd_eq(cmd, b"TIME") || cmd_eq(cmd, b"RENAMENX") || cmd_eq(cmd, b"RANDOMKEY")
        || cmd_eq(cmd, b"HELLO") || cmd_eq(cmd, b"PSUBSCRIBE") || cmd_eq(cmd, b"PUNSUBSCRIBE")
        || cmd_eq(cmd, b"COPY") || cmd_eq(cmd, b"FUNCTION") || cmd_eq(cmd, b"DEBUG")
        || cmd_eq(cmd, b"WAIT") || cmd_eq(cmd, b"RESET") || cmd_eq(cmd, b"LATENCY")
        || cmd_eq(cmd, b"SWAPDB") || cmd_eq(cmd, b"OBJECT") || cmd_eq(cmd, b"MEMORY")
        || cmd_eq(cmd, b"BGSAVE") || cmd_eq(cmd, b"LASTSAVE") || cmd_eq(cmd, b"PUBLISH")
        || cmd_eq(cmd, b"SUBSCRIBE") || cmd_eq(cmd, b"ZADD") || cmd_eq(cmd, b"ZSCORE")
        || cmd_eq(cmd, b"ZRANK") || cmd_eq(cmd, b"ZREVRANK") || cmd_eq(cmd, b"ZREM")
        || cmd_eq(cmd, b"ZCARD") || cmd_eq(cmd, b"ZRANGE") || cmd_eq(cmd, b"ZINCRBY")
        || cmd_eq(cmd, b"ZCOUNT") || cmd_eq(cmd, b"ZPOPMIN") || cmd_eq(cmd, b"ZPOPMAX")
        || cmd_eq(cmd, b"ZUNIONSTORE") || cmd_eq(cmd, b"ZINTERSTORE")
        || cmd_eq(cmd, b"ZDIFFSTORE") || cmd_eq(cmd, b"ZSCAN") || cmd_eq(cmd, b"ZMSCORE")
        || cmd_eq(cmd, b"ZLEXCOUNT") || cmd_eq(cmd, b"ZRANGEBYSCORE")
        || cmd_eq(cmd, b"ZREVRANGEBYSCORE") || cmd_eq(cmd, b"ZRANGEBYLEX")
        || cmd_eq(cmd, b"ZREVRANGEBYLEX") || cmd_eq(cmd, b"ZREMRANGEBYRANK")
        || cmd_eq(cmd, b"ZREMRANGEBYSCORE") || cmd_eq(cmd, b"ZREMRANGEBYLEX")
        || cmd_eq(cmd, b"AUTH")
}

pub fn validate_args(args: &[&[u8]]) -> Result<(), String> {
    if args.is_empty() {
        return Err("ERR no command".to_string());
    }
    let cmd = args[0];
    let min = if cmd_eq(cmd, b"SET") || cmd_eq(cmd, b"GETSET") || cmd_eq(cmd, b"SETNX")
        || cmd_eq(cmd, b"APPEND") || cmd_eq(cmd, b"EXPIRE") || cmd_eq(cmd, b"PEXPIRE")
        || cmd_eq(cmd, b"HGET") || cmd_eq(cmd, b"HEXISTS") || cmd_eq(cmd, b"SISMEMBER")
        || cmd_eq(cmd, b"LINDEX") || cmd_eq(cmd, b"GETRANGE") || cmd_eq(cmd, b"SUBSTR")
    {
        3
    } else if cmd_eq(cmd, b"GET") || cmd_eq(cmd, b"DEL") || cmd_eq(cmd, b"EXISTS")
        || cmd_eq(cmd, b"INCR") || cmd_eq(cmd, b"DECR") || cmd_eq(cmd, b"STRLEN")
        || cmd_eq(cmd, b"TTL") || cmd_eq(cmd, b"PTTL") || cmd_eq(cmd, b"TYPE")
        || cmd_eq(cmd, b"PERSIST") || cmd_eq(cmd, b"KEYS") || cmd_eq(cmd, b"LLEN")
        || cmd_eq(cmd, b"LPOP") || cmd_eq(cmd, b"RPOP") || cmd_eq(cmd, b"HGETALL")
        || cmd_eq(cmd, b"HKEYS") || cmd_eq(cmd, b"HVALS") || cmd_eq(cmd, b"HLEN")
        || cmd_eq(cmd, b"SMEMBERS") || cmd_eq(cmd, b"SCARD") || cmd_eq(cmd, b"ZCARD")
        || cmd_eq(cmd, b"SCAN") || cmd_eq(cmd, b"ECHO") || cmd_eq(cmd, b"GETDEL")
        || cmd_eq(cmd, b"GETEX")
    {
        2
    } else if cmd_eq(cmd, b"LPUSH") || cmd_eq(cmd, b"RPUSH") || cmd_eq(cmd, b"SADD")
        || cmd_eq(cmd, b"SREM") || cmd_eq(cmd, b"INCRBY") || cmd_eq(cmd, b"DECRBY")
        || cmd_eq(cmd, b"MSET") || cmd_eq(cmd, b"MGET") || cmd_eq(cmd, b"RENAME")
        || cmd_eq(cmd, b"INCRBYFLOAT") || cmd_eq(cmd, b"HDEL") || cmd_eq(cmd, b"HMGET")
        || cmd_eq(cmd, b"COPY") || cmd_eq(cmd, b"LRANGE") || cmd_eq(cmd, b"SUNION")
        || cmd_eq(cmd, b"SINTER") || cmd_eq(cmd, b"SDIFF")
    {
        3
    } else if cmd_eq(cmd, b"SETEX") || cmd_eq(cmd, b"PSETEX") || cmd_eq(cmd, b"HSET")
        || cmd_eq(cmd, b"HMSET") || cmd_eq(cmd, b"HINCRBY") || cmd_eq(cmd, b"HSETNX")
        || cmd_eq(cmd, b"HINCRBYFLOAT") || cmd_eq(cmd, b"ZADD") || cmd_eq(cmd, b"LSET")
        || cmd_eq(cmd, b"SETRANGE") || cmd_eq(cmd, b"LINSERT") || cmd_eq(cmd, b"LREM")
        || cmd_eq(cmd, b"LTRIM") || cmd_eq(cmd, b"ZUNIONSTORE") || cmd_eq(cmd, b"ZINTERSTORE")
        || cmd_eq(cmd, b"ZDIFFSTORE") || cmd_eq(cmd, b"ZSCORE") || cmd_eq(cmd, b"ZRANK")
        || cmd_eq(cmd, b"ZREVRANK") || cmd_eq(cmd, b"ZREM") || cmd_eq(cmd, b"ZINCRBY")
        || cmd_eq(cmd, b"SMOVE") || cmd_eq(cmd, b"HSTRLEN")
    {
        4
    } else if cmd_eq(cmd, b"PING") || cmd_eq(cmd, b"DBSIZE") || cmd_eq(cmd, b"FLUSHDB")
        || cmd_eq(cmd, b"FLUSHALL") || cmd_eq(cmd, b"SAVE") || cmd_eq(cmd, b"INFO")
        || cmd_eq(cmd, b"TIME") || cmd_eq(cmd, b"RANDOMKEY") || cmd_eq(cmd, b"BGSAVE")
        || cmd_eq(cmd, b"LASTSAVE") || cmd_eq(cmd, b"HELLO") || cmd_eq(cmd, b"QUIT")
        || cmd_eq(cmd, b"COMMAND") || cmd_eq(cmd, b"CONFIG") || cmd_eq(cmd, b"CLIENT")
        || cmd_eq(cmd, b"SELECT")
    {
        1
    } else {
        return Ok(());
    };
    if args.len() < min {
        let cmd_name = std::str::from_utf8(cmd).unwrap_or("unknown").to_lowercase();
        return Err(format!(
            "ERR wrong number of arguments for '{}' command",
            cmd_name
        ));
    }
    Ok(())
}

fn format_float(v: f64) -> String {
    if v.fract() == 0.0 && v.abs() < 1e15 {
        format!("{}", v as i64)
    } else {
        format!("{}", v)
    }
}

fn parse_score_bound(s: &str, is_max: bool) -> (f64, bool) {
    if s == "-inf" || s == "-" {
        (f64::NEG_INFINITY, false)
    } else if s == "+inf" || s == "+" {
        (f64::INFINITY, false)
    } else if let Some(rest) = s.strip_prefix('(') {
        (
            rest.parse::<f64>().unwrap_or(if is_max {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            }),
            true,
        )
    } else {
        (
            s.parse::<f64>().unwrap_or(if is_max {
                f64::INFINITY
            } else {
                f64::NEG_INFINITY
            }),
            false,
        )
    }
}

fn parse_zstore_options(args: &[&[u8]]) -> (Vec<f64>, String) {
    let mut weights = Vec::new();
    let mut aggregate = "SUM".to_string();
    let mut i = 0;
    while i < args.len() {
        if cmd_eq(args[i], b"WEIGHTS") {
            i += 1;
            while i < args.len() && !cmd_eq(args[i], b"AGGREGATE") {
                weights.push(arg_str(args[i]).parse::<f64>().unwrap_or(1.0));
                i += 1;
            }
        } else if cmd_eq(args[i], b"AGGREGATE") && i + 1 < args.len() {
            aggregate = arg_str(args[i + 1]).to_uppercase();
            i += 2;
        } else {
            i += 1;
        }
    }
    (weights, aggregate)
}

fn write_zset_result(out: &mut BytesMut, items: &[(String, f64)], with_scores: bool) {
    if with_scores {
        resp::write_array_header(out, items.len() * 2);
        for (m, s) in items {
            resp::write_bulk(out, m);
            resp::write_bulk(out, &format_float(*s));
        }
    } else {
        resp::write_array_header(out, items.len());
        for (m, _) in items {
            resp::write_bulk(out, m);
        }
    }
}

fn build_info(store: &Store, _section: &str, now: Instant) -> String {
    let uptime = START_TIME.get().map(|t| t.elapsed().as_secs()).unwrap_or(0);
    let restricted = is_restricted();
    let powered_by = if restricted {
        "\r\npowered_by:LuxDB Cloud (luxdb.dev)"
    } else {
        ""
    };
    format!(
        "# Server\r\n\
         lux_version:{}\r\n\
         shards:{}\r\n\
         uptime_in_seconds:{}\r\n\
         {powered_by}\
         \r\n\
         # Clients\r\n\
         connected_clients:{}\r\n\
         \r\n\
         # Stats\r\n\
         total_commands_processed:{}\r\n\
         \r\n\
         # Memory\r\n\
         used_memory_bytes:{}\r\n\
         \r\n\
         # Keyspace\r\n\
         keys:{}\r\n",
        env!("CARGO_PKG_VERSION"),
        store.shard_count(),
        uptime,
        CONNECTED_CLIENTS.load(Ordering::Relaxed),
        TOTAL_COMMANDS.load(Ordering::Relaxed),
        store.approximate_memory(),
        store.dbsize(now)
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::pubsub::Broker;
    use crate::store::Store;
    use std::time::Instant;

    fn exec(store: &Store, args: &[&[u8]]) -> BytesMut {
        let broker = Broker::new();
        let mut out = BytesMut::new();
        let now = Instant::now();
        execute(store, &broker, args, &mut out, now);
        out
    }

    fn exec_str(store: &Store, args: &[&[u8]]) -> String {
        String::from_utf8_lossy(&exec(store, args)).to_string()
    }

    #[test]
    fn set_wrong_arg_count() {
        let store = Store::new();
        let out = exec_str(&store, &[b"SET", b"key"]);
        assert!(out.contains("ERR wrong number of arguments"));
    }

    #[test]
    fn setex_negative_time() {
        let store = Store::new();
        let out = exec_str(&store, &[b"SETEX", b"key", b"-1", b"val"]);
        assert!(out.contains("ERR invalid expire time"));
    }

    #[test]
    fn incrbyfloat_nan_error() {
        let store = Store::new();
        let out = exec_str(&store, &[b"INCRBYFLOAT", b"key", b"nan"]);
        assert!(out.contains("NaN or Infinity") || out.contains("not a valid float"));
    }

    #[test]
    fn incrbyfloat_with_spaces() {
        let store = Store::new();
        let out = exec_str(&store, &[b"INCRBYFLOAT", b"key", b"1 2"]);
        assert!(out.contains("not a valid float"));
    }

    #[test]
    fn unknown_command_returns_error() {
        let store = Store::new();
        let out = exec_str(&store, &[b"NOTACMD"]);
        assert!(out.contains("ERR unknown command"));
    }

    #[test]
    fn auth_wrong_password() {
        std::env::set_var("LUX_PASSWORD", "secret123");
        let store = Store::new();
        let out = exec_str(&store, &[b"AUTH", b"wrong"]);
        assert!(out.contains("WRONGPASS"));
        std::env::remove_var("LUX_PASSWORD");
    }

    #[test]
    fn getex_syntax_error() {
        let store = Store::new();
        let out = exec_str(&store, &[b"GETEX", b"key", b"BADOPT"]);
        assert!(out.contains("ERR syntax error"));
    }

    #[test]
    fn ping_returns_pong() {
        let store = Store::new();
        let out = exec(&store, &[b"PING"]);
        assert_eq!(&out[..], b"+PONG\r\n");
    }

    #[test]
    fn ping_with_message() {
        let store = Store::new();
        let out = exec_str(&store, &[b"PING", b"hello"]);
        assert!(out.contains("hello"));
    }

    #[test]
    fn echo_returns_argument() {
        let store = Store::new();
        let out = exec_str(&store, &[b"ECHO", b"test"]);
        assert!(out.contains("test"));
    }

    #[test]
    fn set_then_get() {
        let store = Store::new();
        exec(&store, &[b"SET", b"mykey", b"myval"]);
        let out = exec_str(&store, &[b"GET", b"mykey"]);
        assert!(out.contains("myval"));
    }

    #[test]
    fn del_returns_count() {
        let store = Store::new();
        exec(&store, &[b"SET", b"a", b"1"]);
        exec(&store, &[b"SET", b"b", b"2"]);
        let out = exec(&store, &[b"DEL", b"a", b"b", b"c"]);
        assert!(out.starts_with(b":2\r\n"));
    }

    #[test]
    fn zadd_and_zscore() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"zs", b"1.5", b"alice"]);
        let out = exec_str(&store, &[b"ZSCORE", b"zs", b"alice"]);
        assert!(out.contains("1") || out.contains("1.5"));
    }

    #[test]
    fn zadd_nx_flag() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"zs", b"1", b"a"]);
        exec(&store, &[b"ZADD", b"zs", b"NX", b"2", b"a"]);
        let score = store.zscore(b"zs", b"a", Instant::now()).unwrap().unwrap();
        assert!((score - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn type_returns_correct_type() {
        let store = Store::new();
        exec(&store, &[b"SET", b"s", b"val"]);
        exec(&store, &[b"LPUSH", b"l", b"val"]);
        exec(&store, &[b"SADD", b"set", b"val"]);
        exec(&store, &[b"ZADD", b"zs", b"1", b"val"]);
        assert!(exec_str(&store, &[b"TYPE", b"s"]).contains("string"));
        assert!(exec_str(&store, &[b"TYPE", b"l"]).contains("list"));
        assert!(exec_str(&store, &[b"TYPE", b"set"]).contains("set"));
        assert!(exec_str(&store, &[b"TYPE", b"zs"]).contains("zset"));
        assert!(exec_str(&store, &[b"TYPE", b"missing"]).contains("none"));
    }

    #[test]
    fn exec_without_multi_returns_error() {
        let store = Store::new();
        let out = exec_str(&store, &[b"EXEC"]);
        assert!(out.contains("ERR unknown command"));
    }

    #[test]
    fn discard_without_multi_returns_error() {
        let store = Store::new();
        let out = exec_str(&store, &[b"DISCARD"]);
        assert!(out.contains("ERR unknown command"));
    }

    #[test]
    fn validate_args_rejects_missing_args() {
        assert!(validate_args(&[b"SET" as &[u8], b"key"]).is_err());
        assert!(validate_args(&[b"GET" as &[u8]]).is_err());
        assert!(validate_args(&[b"HSET" as &[u8], b"k", b"f"]).is_err());
    }

    #[test]
    fn validate_args_accepts_valid_commands() {
        assert!(validate_args(&[b"SET" as &[u8], b"key", b"val"]).is_ok());
        assert!(validate_args(&[b"GET" as &[u8], b"key"]).is_ok());
        assert!(validate_args(&[b"PING" as &[u8]]).is_ok());
        assert!(validate_args(&[b"DEL" as &[u8], b"key"]).is_ok());
    }

    #[test]
    fn validate_args_passes_unknown_commands() {
        assert!(validate_args(&[b"FOOBAR" as &[u8]]).is_ok());
    }

    #[test]
    fn set_xx_only_if_exists() {
        let store = Store::new();
        let out = exec_str(&store, &[b"SET", b"k", b"v", b"XX"]);
        assert!(out.contains("$-1"), "XX on missing key returns null: {out}");
        exec(&store, &[b"SET", b"k", b"orig"]);
        let out = exec_str(&store, &[b"SET", b"k", b"new", b"XX"]);
        assert!(out.contains("+OK"), "XX on existing key succeeds: {out}");
        let out = exec_str(&store, &[b"GET", b"k"]);
        assert!(out.contains("new"));
    }

    #[test]
    fn set_px_millisecond_ttl() {
        let store = Store::new();
        exec(&store, &[b"SET", b"k", b"v", b"PX", b"100000"]);
        let ttl = store.pttl(b"k", Instant::now());
        assert!(ttl > 0 && ttl <= 100000, "PX TTL: {ttl}");
    }

    #[test]
    fn psetex_sets_with_ms_ttl() {
        let store = Store::new();
        exec(&store, &[b"PSETEX", b"k", b"50000", b"val"]);
        let out = exec_str(&store, &[b"GET", b"k"]);
        assert!(out.contains("val"));
        let ttl = store.pttl(b"k", Instant::now());
        assert!(ttl > 0 && ttl <= 50000, "PSETEX TTL: {ttl}");
    }

    #[test]
    fn copy_basic_and_replace() {
        let store = Store::new();
        exec(&store, &[b"SET", b"src", b"hello"]);
        let out = exec_str(&store, &[b"COPY", b"src", b"dst"]);
        assert!(out.contains(":1"));
        let out = exec_str(&store, &[b"GET", b"dst"]);
        assert!(out.contains("hello"));

        exec(&store, &[b"SET", b"dst", b"existing"]);
        let out = exec_str(&store, &[b"COPY", b"src", b"dst"]);
        assert!(out.contains(":0"), "no REPLACE, dest exists: {out}");

        let out = exec_str(&store, &[b"COPY", b"src", b"dst", b"REPLACE"]);
        assert!(out.contains(":1"), "with REPLACE: {out}");
        let out = exec_str(&store, &[b"GET", b"dst"]);
        assert!(out.contains("hello"));
    }

    #[test]
    fn copy_nonexistent_source() {
        let store = Store::new();
        let out = exec_str(&store, &[b"COPY", b"nosrc", b"dst"]);
        assert!(out.contains(":0"));
    }

    #[test]
    fn renamenx_only_if_dest_missing() {
        let store = Store::new();
        exec(&store, &[b"SET", b"a", b"1"]);
        exec(&store, &[b"SET", b"b", b"2"]);
        let out = exec_str(&store, &[b"RENAMENX", b"a", b"b"]);
        assert!(out.contains(":0"), "dest exists: {out}");
        let out = exec_str(&store, &[b"RENAMENX", b"a", b"c"]);
        assert!(out.contains(":1"), "dest missing: {out}");
        let out = exec_str(&store, &[b"GET", b"c"]);
        assert!(out.contains("1"));
    }

    #[test]
    fn time_returns_two_element_array() {
        let store = Store::new();
        let out = exec_str(&store, &[b"TIME"]);
        assert!(out.starts_with("*2\r\n"), "TIME array: {out}");
    }

    #[test]
    fn object_encoding_types() {
        let store = Store::new();
        exec(&store, &[b"SET", b"num", b"42"]);
        let out = exec_str(&store, &[b"OBJECT", b"ENCODING", b"num"]);
        assert!(out.contains("int"), "integer encoding: {out}");

        exec(&store, &[b"SET", b"str", b"hello"]);
        let out = exec_str(&store, &[b"OBJECT", b"ENCODING", b"str"]);
        assert!(out.contains("embstr"), "short string encoding: {out}");

        exec(&store, &[b"LPUSH", b"list", b"a"]);
        let out = exec_str(&store, &[b"OBJECT", b"ENCODING", b"list"]);
        assert!(out.contains("listpack"), "list encoding: {out}");
    }

    #[test]
    fn object_encoding_missing_key() {
        let store = Store::new();
        let out = exec_str(&store, &[b"OBJECT", b"ENCODING", b"nope"]);
        assert!(out.contains("ERR no such key"));
    }

    #[test]
    fn memory_usage_returns_integer() {
        let store = Store::new();
        exec(&store, &[b"SET", b"k", b"hello"]);
        let out = exec_str(&store, &[b"MEMORY", b"USAGE", b"k"]);
        assert!(out.starts_with(":"), "should be integer: {out}");
        let n: i64 = out.trim().trim_start_matches(':').trim_end_matches("\r\n").parse().unwrap_or(0);
        assert!(n > 0, "should be positive: {n}");
    }

    #[test]
    fn memory_usage_missing_key() {
        let store = Store::new();
        let out = exec_str(&store, &[b"MEMORY", b"USAGE", b"nope"]);
        assert!(out.contains("$-1"), "null for missing key: {out}");
    }

    #[test]
    fn lpos_basic() {
        let store = Store::new();
        exec(&store, &[b"RPUSH", b"list", b"a", b"b", b"c", b"b", b"d"]);
        let out = exec_str(&store, &[b"LPOS", b"list", b"b"]);
        assert!(out.contains(":1"), "first occurrence at index 1: {out}");
    }

    #[test]
    fn lpos_count() {
        let store = Store::new();
        exec(&store, &[b"RPUSH", b"list", b"a", b"b", b"c", b"b", b"d"]);
        let out = exec_str(&store, &[b"LPOS", b"list", b"b", b"COUNT", b"0"]);
        assert!(out.contains("*2"), "two occurrences: {out}");
        assert!(out.contains(":1"));
        assert!(out.contains(":3"));
    }

    #[test]
    fn lpos_rank_negative() {
        let store = Store::new();
        exec(&store, &[b"RPUSH", b"list", b"a", b"b", b"c", b"b", b"d"]);
        let out = exec_str(&store, &[b"LPOS", b"list", b"b", b"RANK", b"-1"]);
        assert!(out.contains(":3"), "last occurrence from end: {out}");
    }

    #[test]
    fn lpos_not_found() {
        let store = Store::new();
        exec(&store, &[b"RPUSH", b"list", b"a", b"b"]);
        let out = exec_str(&store, &[b"LPOS", b"list", b"z"]);
        assert!(out.contains("$-1"), "not found returns null: {out}");
    }

    #[test]
    fn hincrbyfloat_basic() {
        let store = Store::new();
        exec(&store, &[b"HSET", b"h", b"f", b"10.5"]);
        let out = exec_str(&store, &[b"HINCRBYFLOAT", b"h", b"f", b"0.1"]);
        assert!(out.contains("10.6"), "float increment: {out}");
    }

    #[test]
    fn hincrbyfloat_creates_field() {
        let store = Store::new();
        let out = exec_str(&store, &[b"HINCRBYFLOAT", b"h", b"newf", b"3.14"]);
        assert!(out.contains("3.14"), "creates field: {out}");
    }

    #[test]
    fn hstrlen_returns_length() {
        let store = Store::new();
        exec(&store, &[b"HSET", b"h", b"f", b"hello"]);
        let out = exec_str(&store, &[b"HSTRLEN", b"h", b"f"]);
        assert!(out.contains(":5"), "length of 'hello': {out}");
        let out = exec_str(&store, &[b"HSTRLEN", b"h", b"missing"]);
        assert!(out.contains(":0"), "missing field: {out}");
    }

    #[test]
    fn hscan_basic() {
        let store = Store::new();
        exec(&store, &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]);
        let out = exec_str(&store, &[b"HSCAN", b"h", b"0"]);
        assert!(out.contains("f1"), "contains field: {out}");
        assert!(out.contains("v1"), "contains value: {out}");
    }

    #[test]
    fn sscan_basic() {
        let store = Store::new();
        exec(&store, &[b"SADD", b"s", b"a", b"b", b"c"]);
        let out = exec_str(&store, &[b"SSCAN", b"s", b"0"]);
        assert!(out.starts_with("*2"), "two-element array (cursor + items): {out}");
    }

    #[test]
    fn zscan_basic() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"z", b"1", b"a", b"2", b"b"]);
        let out = exec_str(&store, &[b"ZSCAN", b"z", b"0"]);
        assert!(out.starts_with("*2"), "two-element array: {out}");
        assert!(out.contains("a"));
    }

    #[test]
    fn spop_single_and_count() {
        let store = Store::new();
        exec(&store, &[b"SADD", b"s", b"a", b"b", b"c"]);
        let out = exec_str(&store, &[b"SPOP", b"s"]);
        assert!(out.contains("$1"), "single element: {out}");

        let out = exec_str(&store, &[b"SPOP", b"s", b"10"]);
        assert!(out.starts_with("*"), "array for count variant: {out}");
    }

    #[test]
    fn srandmember_does_not_remove() {
        let store = Store::new();
        exec(&store, &[b"SADD", b"s", b"a", b"b", b"c"]);
        exec_str(&store, &[b"SRANDMEMBER", b"s"]);
        let out = exec_str(&store, &[b"SCARD", b"s"]);
        assert!(out.contains(":3"), "no removal: {out}");
    }

    #[test]
    fn srandmember_count() {
        let store = Store::new();
        exec(&store, &[b"SADD", b"s", b"a", b"b", b"c"]);
        let out = exec_str(&store, &[b"SRANDMEMBER", b"s", b"2"]);
        assert!(out.starts_with("*"), "array response: {out}");
    }

    #[test]
    fn sintercard_basic() {
        let store = Store::new();
        exec(&store, &[b"SADD", b"s1", b"a", b"b", b"c"]);
        exec(&store, &[b"SADD", b"s2", b"b", b"c", b"d"]);
        let out = exec_str(&store, &[b"SINTERCARD", b"2", b"s1", b"s2"]);
        assert!(out.contains(":2"), "intersection cardinality: {out}");
    }

    #[test]
    fn hrandfield_basic() {
        let store = Store::new();
        exec(&store, &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]);
        let out = exec_str(&store, &[b"HRANDFIELD", b"h"]);
        assert!(out.contains("f1") || out.contains("f2"), "returns a field: {out}");
    }

    #[test]
    fn hrandfield_count_withvalues() {
        let store = Store::new();
        exec(&store, &[b"HSET", b"h", b"f1", b"v1", b"f2", b"v2"]);
        let out = exec_str(&store, &[b"HRANDFIELD", b"h", b"2", b"WITHVALUES"]);
        assert!(out.starts_with("*4"), "2 fields * 2 = 4 elements: {out}");
    }

    #[test]
    fn zremrangebyrank_basic() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        let out = exec_str(&store, &[b"ZREMRANGEBYRANK", b"z", b"0", b"1"]);
        assert!(out.contains(":2"), "removed 2: {out}");
        let out = exec_str(&store, &[b"ZCARD", b"z"]);
        assert!(out.contains(":1"), "1 remaining: {out}");
    }

    #[test]
    fn zremrangebyscore_basic() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        let out = exec_str(&store, &[b"ZREMRANGEBYSCORE", b"z", b"-inf", b"2"]);
        assert!(out.contains(":2"), "removed 2: {out}");
        let out = exec_str(&store, &[b"ZCARD", b"z"]);
        assert!(out.contains(":1"), "1 remaining: {out}");
    }

    #[test]
    fn zremrangebylex_basic() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c", b"0", b"d"]);
        let out = exec_str(&store, &[b"ZREMRANGEBYLEX", b"z", b"[a", b"[c"]);
        assert!(out.contains(":3"), "removed a,b,c: {out}");
        let out = exec_str(&store, &[b"ZCARD", b"z"]);
        assert!(out.contains(":1"), "d remaining: {out}");
    }

    #[test]
    fn zlexcount_basic() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"z", b"0", b"a", b"0", b"b", b"0", b"c"]);
        let out = exec_str(&store, &[b"ZLEXCOUNT", b"z", b"-", b"+"]);
        assert!(out.contains(":3"), "all members: {out}");
        let out = exec_str(&store, &[b"ZLEXCOUNT", b"z", b"[a", b"[b"]);
        assert!(out.contains(":2"), "a and b: {out}");
    }

    #[test]
    fn zrevrange_basic() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        let out = exec_str(&store, &[b"ZRANGE", b"z", b"0", b"-1", b"REV"]);
        let a_pos = out.find("a").unwrap_or(0);
        let c_pos = out.find("c").unwrap_or(usize::MAX);
        assert!(c_pos < a_pos, "c before a in reverse: {out}");
    }

    #[test]
    fn zrevrangebyscore_basic() {
        let store = Store::new();
        exec(&store, &[b"ZADD", b"z", b"1", b"a", b"2", b"b", b"3", b"c"]);
        let out = exec_str(&store, &[b"ZREVRANGEBYSCORE", b"z", b"3", b"1"]);
        assert!(out.contains("a") && out.contains("c"), "contains both: {out}");
    }

    #[test]
    fn unlink_same_as_del() {
        let store = Store::new();
        exec(&store, &[b"SET", b"a", b"1"]);
        exec(&store, &[b"SET", b"b", b"2"]);
        let out = exec_str(&store, &[b"UNLINK", b"a", b"b"]);
        assert!(out.contains(":2"), "removed 2: {out}");
        assert!(store.get(b"a", Instant::now()).is_none());
    }

    #[test]
    fn randomkey_returns_key_or_null() {
        let store = Store::new();
        let out = exec_str(&store, &[b"RANDOMKEY"]);
        assert!(out.contains("$-1"), "empty db returns null: {out}");

        exec(&store, &[b"SET", b"mykey", b"val"]);
        let out = exec_str(&store, &[b"RANDOMKEY"]);
        assert!(out.contains("mykey"), "returns existing key: {out}");
    }

    #[test]
    fn hello_returns_server_info() {
        let store = Store::new();
        let out = exec_str(&store, &[b"HELLO"]);
        assert!(out.contains("lux"), "contains server name: {out}");
        assert!(out.contains("proto"), "contains proto: {out}");
    }

    #[test]
    fn info_returns_bulk_string() {
        let store = Store::new();
        let out = exec_str(&store, &[b"INFO"]);
        assert!(out.contains("lux_version"), "contains version: {out}");
        assert!(out.contains("connected_clients"), "contains clients: {out}");
    }

    #[test]
    fn config_get_returns_empty_array() {
        let store = Store::new();
        let out = exec_str(&store, &[b"CONFIG", b"GET", b"maxmemory"]);
        assert!(out.contains("*0"), "empty array: {out}");
    }

    #[test]
    fn select_returns_ok() {
        let store = Store::new();
        let out = exec_str(&store, &[b"SELECT", b"0"]);
        assert!(out.contains("+OK"));
    }

    #[test]
    fn substr_alias_for_getrange() {
        let store = Store::new();
        exec(&store, &[b"SET", b"k", b"Hello World"]);
        let out = exec_str(&store, &[b"SUBSTR", b"k", b"0", b"4"]);
        assert!(out.contains("Hello"), "substr works like getrange: {out}");
    }

    #[test]
    fn multi_exec_discard_watch_unwatch_not_handled_by_cmd() {
        let store = Store::new();
        for cmd in &[
            vec![b"MULTI" as &[u8]],
            vec![b"WATCH", b"key"],
            vec![b"UNWATCH"],
        ] {
            let out = exec_str(&store, &cmd);
            assert!(
                out.contains("ERR unknown command"),
                "cmd {:?} should be unknown: {out}",
                std::str::from_utf8(cmd[0])
            );
        }
    }
}
