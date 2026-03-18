use bytes::BytesMut;
use std::time::Instant;

use crate::resp;
use crate::store::Store;

use super::{parse_i64, parse_u64, CmdResult};

pub fn cmd_sadd(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 3 {
        resp::write_error(out, "ERR wrong number of arguments for 'sadd' command");
        return CmdResult::Written;
    }
    let members: Vec<&[u8]> = args[2..].to_vec();
    match store.sadd(args[1], &members, now) {
        Ok(n) => resp::write_integer(out, n),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_srem(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 3 {
        resp::write_error(out, "ERR wrong number of arguments for 'srem' command");
        return CmdResult::Written;
    }
    let members: Vec<&[u8]> = args[2..].to_vec();
    match store.srem(args[1], &members, now) {
        Ok(n) => resp::write_integer(out, n),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_smembers(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 2 {
        resp::write_error(out, "ERR wrong number of arguments for 'smembers' command");
        return CmdResult::Written;
    }
    match store.smembers(args[1], now) {
        Ok(members) => resp::write_bulk_array(out, &members),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_sismember(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 3 {
        resp::write_error(out, "ERR wrong number of arguments for 'sismember' command");
        return CmdResult::Written;
    }
    match store.sismember(args[1], args[2], now) {
        Ok(b) => resp::write_integer(out, if b { 1 } else { 0 }),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_smismember(
    args: &[&[u8]],
    store: &Store,
    out: &mut BytesMut,
    now: Instant,
) -> CmdResult {
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
    CmdResult::Written
}

pub fn cmd_scard(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 2 {
        resp::write_error(out, "ERR wrong number of arguments for 'scard' command");
        return CmdResult::Written;
    }
    match store.scard(args[1], now) {
        Ok(n) => resp::write_integer(out, n),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_spop(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
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
    CmdResult::Written
}

pub fn cmd_srandmember(
    args: &[&[u8]],
    store: &Store,
    out: &mut BytesMut,
    now: Instant,
) -> CmdResult {
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
    CmdResult::Written
}

pub fn cmd_smove(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 4 {
        resp::write_error(out, "ERR wrong number of arguments for 'smove' command");
        return CmdResult::Written;
    }
    match store.smove(args[1], args[2], args[3], now) {
        Ok(b) => resp::write_integer(out, if b { 1 } else { 0 }),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_sunion(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 2 {
        resp::write_error(out, "ERR wrong number of arguments for 'sunion' command");
        return CmdResult::Written;
    }
    let keys: Vec<&[u8]> = args[1..].to_vec();
    match store.sunion(&keys, now) {
        Ok(members) => resp::write_bulk_array(out, &members),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_sinter(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 2 {
        resp::write_error(out, "ERR wrong number of arguments for 'sinter' command");
        return CmdResult::Written;
    }
    let keys: Vec<&[u8]> = args[1..].to_vec();
    match store.sinter(&keys, now) {
        Ok(members) => resp::write_bulk_array(out, &members),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_sdiff(args: &[&[u8]], store: &Store, out: &mut BytesMut, now: Instant) -> CmdResult {
    if args.len() < 2 {
        resp::write_error(out, "ERR wrong number of arguments for 'sdiff' command");
        return CmdResult::Written;
    }
    let keys: Vec<&[u8]> = args[1..].to_vec();
    match store.sdiff(&keys, now) {
        Ok(members) => resp::write_bulk_array(out, &members),
        Err(e) => resp::write_error(out, &e),
    }
    CmdResult::Written
}

pub fn cmd_sunionstore(
    args: &[&[u8]],
    store: &Store,
    out: &mut BytesMut,
    now: Instant,
) -> CmdResult {
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
    CmdResult::Written
}

pub fn cmd_sinterstore(
    args: &[&[u8]],
    store: &Store,
    out: &mut BytesMut,
    now: Instant,
) -> CmdResult {
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
    CmdResult::Written
}

pub fn cmd_sdiffstore(
    args: &[&[u8]],
    store: &Store,
    out: &mut BytesMut,
    now: Instant,
) -> CmdResult {
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
    CmdResult::Written
}

pub fn cmd_sintercard(
    args: &[&[u8]],
    store: &Store,
    out: &mut BytesMut,
    now: Instant,
) -> CmdResult {
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
    CmdResult::Written
}
