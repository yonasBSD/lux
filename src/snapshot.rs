use crate::store::{DumpValue, Store};
use std::fs;
use std::io::{self, BufRead, Write};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};

fn snapshot_path() -> String {
    let dir = std::env::var("LUX_DATA_DIR").unwrap_or_else(|_| ".".to_string());
    format!("{}/lux.dat", dir.trim_end_matches('/'))
}

fn snapshot_interval() -> Duration {
    let secs: u64 = std::env::var("LUX_SAVE_INTERVAL")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(60);
    Duration::from_secs(secs)
}

pub fn save(store: &Store) -> io::Result<usize> {
    let path = snapshot_path();
    if let Some(parent) = Path::new(&path).parent() {
        fs::create_dir_all(parent)?;
    }
    let now = Instant::now();
    let entries = store.dump_all(now);
    let tmp = format!("{path}.{}.tmp", std::process::id());
    let mut file = fs::File::create(&tmp)?;
    for entry in &entries {
        let type_char = match &entry.value {
            DumpValue::Str(_) => 'S',
            DumpValue::List(_) => 'L',
            DumpValue::Hash(_) => 'H',
            DumpValue::Set(_) => 'T',
            DumpValue::SortedSet(_) => 'Z',
            DumpValue::Stream(..) => 'X',
        };
        let encoded_value = match &entry.value {
            DumpValue::Str(s) => s.clone(),
            DumpValue::List(items) => items.join("\x1f"),
            DumpValue::Hash(pairs) => pairs
                .iter()
                .map(|(k, v)| format!("{}\x1e{}", k, v))
                .collect::<Vec<_>>()
                .join("\x1f"),
            DumpValue::Set(members) => members.join("\x1f"),
            DumpValue::SortedSet(members) => members
                .iter()
                .map(|(m, s)| format!("{}\x1e{}", m, s))
                .collect::<Vec<_>>()
                .join("\x1f"),
            DumpValue::Stream(entries, last_id) => {
                let entries_str: Vec<String> = entries
                    .iter()
                    .map(|(id, fields)| {
                        let flds: Vec<String> = fields
                            .iter()
                            .map(|(k, v)| format!("{}\x1d{}", k, v))
                            .collect();
                        format!("{}\x1d{}", id, flds.join("\x1d"))
                    })
                    .collect();
                format!("{}\x1c{}", last_id, entries_str.join("\x1f"))
            }
        };
        writeln!(
            file,
            "{}\t{}\t{}\t{}",
            type_char, entry.key, encoded_value, entry.ttl_ms
        )?;
    }
    file.sync_all()?;
    fs::rename(&tmp, &path)?;
    Ok(entries.len())
}

pub fn load(store: &Store) -> io::Result<usize> {
    let path_str = snapshot_path();
    let path = Path::new(&path_str);
    if !path.exists() {
        return Ok(0);
    }
    let file = fs::File::open(path)?;
    let reader = io::BufReader::new(file);
    let mut count = 0;
    for line in reader.lines() {
        let line = line?;
        if line.is_empty() {
            continue;
        }

        if !line.contains('\t')
            || line.chars().next().is_none_or(|c| !"SLHTZX".contains(c))
            || line.chars().nth(1) != Some('\t')
        {
            let parts: Vec<&str> = line.splitn(3, '\t').collect();
            if parts.len() == 3 {
                let key = parts[0].to_string();
                let value = parts[1].to_string();
                let ttl_ms: i64 = parts[2].parse().unwrap_or(0);
                let ttl = if ttl_ms > 0 {
                    Some(Duration::from_millis(ttl_ms as u64))
                } else {
                    None
                };
                store.load_entry(key, DumpValue::Str(value), ttl);
                count += 1;
            }
            continue;
        }

        let parts: Vec<&str> = line.splitn(4, '\t').collect();
        if parts.len() != 4 {
            continue;
        }
        let type_char = parts[0];
        let key = parts[1].to_string();
        let raw_value = parts[2];
        let ttl_ms: i64 = parts[3].parse().unwrap_or(0);
        let ttl = if ttl_ms > 0 {
            Some(Duration::from_millis(ttl_ms as u64))
        } else {
            None
        };

        let value = match type_char {
            "S" => DumpValue::Str(raw_value.to_string()),
            "L" => {
                let items: Vec<String> = if raw_value.is_empty() {
                    vec![]
                } else {
                    raw_value.split('\x1f').map(|s| s.to_string()).collect()
                };
                DumpValue::List(items)
            }
            "H" => {
                let pairs: Vec<(String, String)> = if raw_value.is_empty() {
                    vec![]
                } else {
                    raw_value
                        .split('\x1f')
                        .filter_map(|pair| {
                            let kv: Vec<&str> = pair.splitn(2, '\x1e').collect();
                            if kv.len() == 2 {
                                Some((kv[0].to_string(), kv[1].to_string()))
                            } else {
                                None
                            }
                        })
                        .collect()
                };
                DumpValue::Hash(pairs)
            }
            "T" => {
                let members: Vec<String> = if raw_value.is_empty() {
                    vec![]
                } else {
                    raw_value.split('\x1f').map(|s| s.to_string()).collect()
                };
                DumpValue::Set(members)
            }
            "Z" => {
                let members: Vec<(String, f64)> = if raw_value.is_empty() {
                    vec![]
                } else {
                    raw_value
                        .split('\x1f')
                        .filter_map(|pair| {
                            let kv: Vec<&str> = pair.splitn(2, '\x1e').collect();
                            if kv.len() == 2 {
                                Some((kv[0].to_string(), kv[1].parse::<f64>().unwrap_or(0.0)))
                            } else {
                                None
                            }
                        })
                        .collect()
                };
                DumpValue::SortedSet(members)
            }
            "X" => {
                let parts_x: Vec<&str> = raw_value.splitn(2, '\x1c').collect();
                let last_id_str = if !parts_x.is_empty() {
                    parts_x[0].to_string()
                } else {
                    "0-0".to_string()
                };
                let entries_raw = if parts_x.len() >= 2 { parts_x[1] } else { "" };
                let mut entries = Vec::new();
                if !entries_raw.is_empty() {
                    for entry_str in entries_raw.split('\x1f') {
                        let parts_e: Vec<&str> = entry_str.split('\x1d').collect();
                        if !parts_e.is_empty() {
                            let id = parts_e[0].to_string();
                            let mut fields = Vec::new();
                            let mut fi = 1;
                            while fi + 1 < parts_e.len() {
                                fields.push((parts_e[fi].to_string(), parts_e[fi + 1].to_string()));
                                fi += 2;
                            }
                            entries.push((id, fields));
                        }
                    }
                }
                DumpValue::Stream(entries, last_id_str)
            }
            _ => continue,
        };

        store.load_entry(key, value, ttl);
        count += 1;
    }
    Ok(count)
}

pub async fn background_save_loop(store: Arc<Store>) {
    let interval = snapshot_interval();
    if interval.is_zero() {
        return;
    }
    loop {
        tokio::time::sleep(interval).await;
        match save(&store) {
            Ok(n) => println!("snapshot: saved {n} keys"),
            Err(e) => eprintln!("snapshot error: {e} (path: {})", snapshot_path()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::Store;
    use std::sync::atomic::{AtomicU32, Ordering};
    static TEST_ID: AtomicU32 = AtomicU32::new(0);

    fn save_to(store: &Store, path: &str) -> io::Result<usize> {
        if let Some(parent) = Path::new(path).parent() {
            fs::create_dir_all(parent)?;
        }
        let now = Instant::now();
        let entries = store.dump_all(now);
        let tmp = format!("{path}.tmp");
        let mut file = fs::File::create(&tmp)?;
        for entry in &entries {
            let type_char = match &entry.value {
                DumpValue::Str(_) => 'S',
                DumpValue::List(_) => 'L',
                DumpValue::Hash(_) => 'H',
                DumpValue::Set(_) => 'T',
                DumpValue::SortedSet(_) => 'Z',
                DumpValue::Stream(..) => 'X',
            };
            let encoded_value = match &entry.value {
                DumpValue::Str(s) => s.clone(),
                DumpValue::List(items) => items.join("\x1f"),
                DumpValue::Hash(pairs) => pairs
                    .iter()
                    .map(|(k, v)| format!("{}\x1e{}", k, v))
                    .collect::<Vec<_>>()
                    .join("\x1f"),
                DumpValue::Set(members) => members.join("\x1f"),
                DumpValue::SortedSet(members) => members
                    .iter()
                    .map(|(m, s)| format!("{}\x1e{}", m, s))
                    .collect::<Vec<_>>()
                    .join("\x1f"),
                DumpValue::Stream(stream_entries, last_id) => {
                    let entries_str: Vec<String> = stream_entries
                        .iter()
                        .map(|(id, fields)| {
                            let flds: Vec<String> = fields
                                .iter()
                                .map(|(k, v)| format!("{}\x1d{}", k, v))
                                .collect();
                            format!("{}\x1d{}", id, flds.join("\x1d"))
                        })
                        .collect();
                    format!("{}\x1c{}", last_id, entries_str.join("\x1f"))
                }
            };
            writeln!(
                file,
                "{}\t{}\t{}\t{}",
                type_char, entry.key, encoded_value, entry.ttl_ms
            )?;
        }
        file.sync_all()?;
        fs::rename(&tmp, path)?;
        Ok(entries.len())
    }

    fn load_from(store: &Store, path: &str) -> io::Result<usize> {
        let path = Path::new(path);
        if !path.exists() {
            return Ok(0);
        }
        let file = fs::File::open(path)?;
        let reader = io::BufReader::new(file);
        let mut count = 0;
        for line in reader.lines() {
            let line = line?;
            if line.is_empty() {
                continue;
            }
            let parts: Vec<&str> = line.splitn(4, '\t').collect();
            if parts.len() != 4 {
                continue;
            }
            let key = parts[1].to_string();
            let raw_value = parts[2];
            let ttl_ms: i64 = parts[3].parse().unwrap_or(0);
            let ttl = if ttl_ms > 0 {
                Some(Duration::from_millis(ttl_ms as u64))
            } else {
                None
            };
            let value = match parts[0] {
                "S" => DumpValue::Str(raw_value.to_string()),
                "L" => DumpValue::List(if raw_value.is_empty() {
                    vec![]
                } else {
                    raw_value.split('\x1f').map(|s| s.to_string()).collect()
                }),
                "H" => DumpValue::Hash(
                    raw_value
                        .split('\x1f')
                        .filter_map(|p| {
                            let kv: Vec<&str> = p.splitn(2, '\x1e').collect();
                            if kv.len() == 2 {
                                Some((kv[0].to_string(), kv[1].to_string()))
                            } else {
                                None
                            }
                        })
                        .collect(),
                ),
                "T" => DumpValue::Set(if raw_value.is_empty() {
                    vec![]
                } else {
                    raw_value.split('\x1f').map(|s| s.to_string()).collect()
                }),
                "Z" => DumpValue::SortedSet(
                    raw_value
                        .split('\x1f')
                        .filter_map(|p| {
                            let kv: Vec<&str> = p.splitn(2, '\x1e').collect();
                            if kv.len() == 2 {
                                Some((kv[0].to_string(), kv[1].parse::<f64>().unwrap_or(0.0)))
                            } else {
                                None
                            }
                        })
                        .collect(),
                ),
                "X" => {
                    let parts_x: Vec<&str> = raw_value.splitn(2, '\x1c').collect();
                    let last_id_str = if !parts_x.is_empty() {
                        parts_x[0].to_string()
                    } else {
                        "0-0".to_string()
                    };
                    let entries_raw = if parts_x.len() >= 2 { parts_x[1] } else { "" };
                    let mut entries = Vec::new();
                    if !entries_raw.is_empty() {
                        for entry_str in entries_raw.split('\x1f') {
                            let parts_e: Vec<&str> = entry_str.split('\x1d').collect();
                            if !parts_e.is_empty() {
                                let id = parts_e[0].to_string();
                                let mut fields = Vec::new();
                                let mut fi = 1;
                                while fi + 1 < parts_e.len() {
                                    fields.push((
                                        parts_e[fi].to_string(),
                                        parts_e[fi + 1].to_string(),
                                    ));
                                    fi += 2;
                                }
                                entries.push((id, fields));
                            }
                        }
                    }
                    DumpValue::Stream(entries, last_id_str)
                }
                _ => continue,
            };
            store.load_entry(key, value, ttl);
            count += 1;
        }
        Ok(count)
    }

    fn test_path() -> (String, impl Drop) {
        let id = TEST_ID.fetch_add(1, Ordering::Relaxed);
        let dir = std::env::temp_dir().join(format!("lux_snap_test_{}_{}", std::process::id(), id));
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("lux.dat").to_str().unwrap().to_string();
        struct Cleanup(std::path::PathBuf);
        impl Drop for Cleanup {
            fn drop(&mut self) {
                let _ = fs::remove_dir_all(&self.0);
            }
        }
        (path, Cleanup(dir))
    }

    #[test]
    fn roundtrip_strings() {
        let (path, _g) = test_path();
        let store = Store::new();
        let now = Instant::now();
        store.set(b"hello", b"world", None, now);
        store.set(b"num", b"42", None, now);
        assert_eq!(save_to(&store, &path).unwrap(), 2);
        let store2 = Store::new();
        assert_eq!(load_from(&store2, &path).unwrap(), 2);
        assert_eq!(store2.get(b"hello", Instant::now()).unwrap(), &b"world"[..]);
        assert_eq!(store2.get(b"num", Instant::now()).unwrap(), &b"42"[..]);
    }

    #[test]
    fn roundtrip_lists() {
        let (path, _g) = test_path();
        let store = Store::new();
        let now = Instant::now();
        store.rpush(b"mylist", &[b"a", b"b", b"c"], now).unwrap();
        save_to(&store, &path).unwrap();
        let store2 = Store::new();
        load_from(&store2, &path).unwrap();
        let n = Instant::now();
        assert_eq!(store2.llen(b"mylist", n).unwrap(), 3);
        let range = store2.lrange(b"mylist", 0, -1, n).unwrap();
        assert_eq!(range[0], &b"a"[..]);
        assert_eq!(range[2], &b"c"[..]);
    }

    #[test]
    fn roundtrip_hashes() {
        let (path, _g) = test_path();
        let store = Store::new();
        let now = Instant::now();
        store
            .hset(
                b"myhash",
                &[(b"f1" as &[u8], b"v1" as &[u8]), (b"f2", b"v2")],
                now,
            )
            .unwrap();
        save_to(&store, &path).unwrap();
        let store2 = Store::new();
        load_from(&store2, &path).unwrap();
        let n = Instant::now();
        assert_eq!(store2.hget(b"myhash", b"f1", n).unwrap(), &b"v1"[..]);
        assert_eq!(store2.hlen(b"myhash", n).unwrap(), 2);
    }

    #[test]
    fn roundtrip_sets() {
        let (path, _g) = test_path();
        let store = Store::new();
        let now = Instant::now();
        store.sadd(b"myset", &[b"a", b"b", b"c"], now).unwrap();
        save_to(&store, &path).unwrap();
        let store2 = Store::new();
        load_from(&store2, &path).unwrap();
        let n = Instant::now();
        assert_eq!(store2.scard(b"myset", n).unwrap(), 3);
        assert!(store2.sismember(b"myset", b"a", n).unwrap());
    }

    #[test]
    fn roundtrip_sorted_sets() {
        let (path, _g) = test_path();
        let store = Store::new();
        let now = Instant::now();
        store
            .zadd(
                b"myzset",
                &[(b"alice" as &[u8], 1.5), (b"bob", 2.5)],
                false,
                false,
                false,
                false,
                false,
                now,
            )
            .unwrap();
        save_to(&store, &path).unwrap();
        let store2 = Store::new();
        load_from(&store2, &path).unwrap();
        let n = Instant::now();
        assert_eq!(store2.zcard(b"myzset", n).unwrap(), 2);
        assert_eq!(store2.zscore(b"myzset", b"alice", n).unwrap(), Some(1.5));
        assert_eq!(store2.zscore(b"myzset", b"bob", n).unwrap(), Some(2.5));
    }

    #[test]
    fn roundtrip_with_ttl() {
        let (path, _g) = test_path();
        let store = Store::new();
        let now = Instant::now();
        store.set(b"expiring", b"val", Some(Duration::from_secs(3600)), now);
        store.set(b"permanent", b"val", None, now);
        save_to(&store, &path).unwrap();
        let store2 = Store::new();
        load_from(&store2, &path).unwrap();
        let n = Instant::now();
        assert!(store2.get(b"expiring", n).is_some());
        assert!(store2.ttl(b"expiring", n) > 0);
        assert_eq!(store2.ttl(b"permanent", n), -1);
    }

    #[test]
    fn roundtrip_all_types_together() {
        let (path, _g) = test_path();
        let store = Store::new();
        let now = Instant::now();
        store.set(b"str", b"val", None, now);
        store.rpush(b"list", &[b"a", b"b"], now).unwrap();
        store
            .hset(b"hash", &[(b"f" as &[u8], b"v" as &[u8])], now)
            .unwrap();
        store.sadd(b"set", &[b"x", b"y"], now).unwrap();
        store
            .zadd(
                b"zset",
                &[(b"m" as &[u8], 1.0)],
                false,
                false,
                false,
                false,
                false,
                now,
            )
            .unwrap();
        assert_eq!(save_to(&store, &path).unwrap(), 5);
        let store2 = Store::new();
        assert_eq!(load_from(&store2, &path).unwrap(), 5);
        let n = Instant::now();
        assert_eq!(store2.get(b"str", n).unwrap(), &b"val"[..]);
        assert_eq!(store2.llen(b"list", n).unwrap(), 2);
        assert_eq!(store2.hlen(b"hash", n).unwrap(), 1);
        assert_eq!(store2.scard(b"set", n).unwrap(), 2);
        assert_eq!(store2.zcard(b"zset", n).unwrap(), 1);
    }

    #[test]
    fn load_nonexistent_returns_zero() {
        let store = Store::new();
        assert_eq!(
            load_from(&store, "/tmp/lux_nonexistent_file_test.dat").unwrap(),
            0
        );
    }
}
