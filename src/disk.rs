//! Tiered storage: per-shard disk backing and write-ahead log.
//!
//! When LUX_STORAGE_MODE=tiered, each shard gets a DiskShard (append-only
//! data file + in-memory index) and a Wal (command log for crash recovery).
//! Evicted entries are written to the DiskShard instead of being deleted.
//! On read miss, entries are transparently promoted back to memory.

use crate::store::{DumpEntry, DumpValue};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::OnceLock;
use std::time::{Duration, Instant};

#[derive(Clone, Copy, PartialEq, Eq)]
pub enum StorageMode {
    /// All data in memory. Eviction deletes permanently. Zero disk overhead.
    Memory,
    /// Hot data in memory, cold data on disk. Automatic promotion on access.
    Tiered,
}

pub struct StorageConfig {
    pub mode: StorageMode,
    pub dir: String,
}

static STORAGE_CONFIG: OnceLock<StorageConfig> = OnceLock::new();

pub fn storage_config() -> &'static StorageConfig {
    STORAGE_CONFIG.get_or_init(|| {
        let mode = match std::env::var("LUX_STORAGE_MODE")
            .unwrap_or_default()
            .to_lowercase()
            .as_str()
        {
            "tiered" => StorageMode::Tiered,
            _ => StorageMode::Memory,
        };
        let data_dir = std::env::var("LUX_DATA_DIR").unwrap_or_else(|_| ".".to_string());
        let dir = std::env::var("LUX_STORAGE_DIR")
            .unwrap_or_else(|_| format!("{}/storage", data_dir.trim_end_matches('/')));
        StorageConfig { mode, dir }
    })
}

/// Write-ahead log for crash recovery.
///
/// Stores raw command bytes in a length-prefixed binary format. Every write
/// command is appended here before the in-memory mutation. On crash, the WAL
/// is replayed by re-executing each command. Truncated after each snapshot
/// since the snapshot contains all data.
///
/// Frame format: [4B frame_len][4B argc][for each arg: 4B len + bytes]
pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(dir: &Path, shard_id: usize) -> io::Result<Self> {
        let shard_dir = dir.join(format!("shard_{shard_id}"));
        fs::create_dir_all(&shard_dir)?;
        let path = shard_dir.join("wal.lux");
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;
        Ok(Wal { file })
    }

    /// Append a command to the WAL. Flushed to OS buffer immediately;
    /// fsync'd to disk by a background task every 1 second.
    pub fn append_command(&mut self, args: &[&[u8]]) -> io::Result<()> {
        let mut buf = Vec::new();
        let argc = args.len() as u32;
        buf.extend_from_slice(&argc.to_le_bytes());
        for arg in args {
            let len = arg.len() as u32;
            buf.extend_from_slice(&len.to_le_bytes());
            buf.extend_from_slice(arg);
        }
        let frame_len = buf.len() as u32;
        self.file.write_all(&frame_len.to_le_bytes())?;
        self.file.write_all(&buf)?;
        self.file.flush()
    }

    pub fn fsync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Read all commands from the WAL for replay. Partial/corrupt frames
    /// (from a crash mid-write) are safely skipped due to length-prefixed framing.
    pub fn replay(&mut self) -> io::Result<Vec<Vec<Vec<u8>>>> {
        let file_len = self.file.seek(SeekFrom::End(0))?;
        if file_len == 0 {
            return Ok(Vec::new());
        }
        self.file.seek(SeekFrom::Start(0))?;
        let mut commands = Vec::new();

        loop {
            let frame_len = match read_u32(&mut self.file) {
                Ok(l) => l as usize,
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(_) => break,
            };

            let mut buf = vec![0u8; frame_len];
            match self.file.read_exact(&mut buf) {
                Ok(()) => {}
                Err(_) => break,
            }

            let mut cursor = &buf[..];
            let argc = match read_u32(&mut cursor) {
                Ok(n) => n as usize,
                Err(_) => continue,
            };

            let mut args = Vec::with_capacity(argc);
            let mut valid = true;
            for _ in 0..argc {
                match read_bytes(&mut cursor) {
                    Ok(arg) => args.push(arg),
                    Err(_) => {
                        valid = false;
                        break;
                    }
                }
            }
            if valid && !args.is_empty() {
                commands.push(args);
            }
        }
        self.file.seek(SeekFrom::End(0))?;
        Ok(commands)
    }

    pub fn truncate(&mut self) -> io::Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0)).map(|_| ())
    }
}

/// In-memory metadata for a cold entry on disk. The actual data lives in the
/// data file at `offset`. We track `created_at` so TTL can be correctly
/// decremented while the entry sits on disk.
struct DiskEntry {
    offset: u64,
    length: u32,
    ttl_ms: i64,
    created_at: Instant,
}

impl DiskEntry {
    fn is_expired(&self, now: Instant) -> bool {
        if self.ttl_ms <= 0 {
            return false;
        }
        let elapsed = now.duration_since(self.created_at).as_millis() as i64;
        elapsed >= self.ttl_ms
    }

    fn remaining_ttl_ms(&self, now: Instant) -> i64 {
        if self.ttl_ms <= 0 {
            return -1;
        }
        let elapsed = now.duration_since(self.created_at).as_millis() as i64;
        let remaining = self.ttl_ms - elapsed;
        if remaining <= 0 {
            0
        } else {
            remaining
        }
    }
}

/// Per-shard cold storage. Uses a Bitcask-style design:
/// - Append-only data file: serialized entries appended on eviction
/// - In-memory index: HashMap<key, file_offset> for O(1) lookups without scanning
/// - Compaction: periodic rewrite drops dead bytes from overwritten/deleted entries
///
/// Protected by a Mutex in Store. Accessed only on eviction (write) and
/// cache miss (read), both cold paths. Never blocks the in-memory shard RwLock.
pub struct DiskShard {
    /// Maps key -> position in data file. Small footprint since it only
    /// stores offsets, not values.
    index: HashMap<String, DiskEntry>,
    data_file: File,
    path: PathBuf,
    /// Bytes in the data file that are no longer referenced (overwritten entries).
    /// When this exceeds 30% of total_bytes, compaction triggers.
    dead_bytes: usize,
    total_bytes: usize,
}

impl DiskShard {
    /// Opens or creates a disk shard. On startup, rebuilds the in-memory
    /// index by scanning the existing data file.
    pub fn open(dir: &Path, shard_id: usize) -> io::Result<Self> {
        let shard_dir = dir.join(format!("shard_{shard_id}"));
        fs::create_dir_all(&shard_dir)?;
        let path = shard_dir.join("data.lux");
        let data_file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(&path)?;

        let mut ds = DiskShard {
            index: HashMap::new(),
            data_file,
            path,
            dead_bytes: 0,
            total_bytes: 0,
        };
        ds.rebuild_index()?;
        Ok(ds)
    }

    /// Serialize and append an entry to the data file. If the key already
    /// exists on disk (re-eviction), the old bytes become dead and the index
    /// points to the new copy.
    pub fn put(&mut self, key: &str, dump: &DumpEntry) -> io::Result<()> {
        let offset = self.total_bytes as u64;
        let mut buf = Vec::new();
        write_single_entry(&mut buf, dump)?;
        let length = buf.len() as u32;

        self.data_file.write_all(&buf)?;
        self.data_file.flush()?;

        if let Some(old) = self.index.insert(
            key.to_string(),
            DiskEntry {
                offset,
                length,
                ttl_ms: if dump.ttl_ms > 0 { dump.ttl_ms } else { -1 },
                created_at: Instant::now(),
            },
        ) {
            self.dead_bytes += old.length as usize;
        }
        self.total_bytes += length as usize;
        Ok(())
    }

    /// Read an entry from disk by seeking to its offset in the data file.
    /// Returns None if the key isn't in the index or has expired.
    pub fn get(
        &mut self,
        key: &str,
        now: Instant,
    ) -> io::Result<Option<(DumpValue, Option<Duration>)>> {
        let de = match self.index.get(key) {
            Some(de) => de,
            None => return Ok(None),
        };
        if de.is_expired(now) {
            let len = de.length as usize;
            self.index.remove(key);
            self.dead_bytes += len;
            return Ok(None);
        }

        let offset = de.offset;
        let length = de.length as usize;
        let remaining = de.remaining_ttl_ms(now);

        let mut buf = vec![0u8; length];
        let mut reader = &self.data_file;
        reader.seek(SeekFrom::Start(offset))?;
        reader.read_exact(&mut buf)?;

        let mut cursor = &buf[..];
        let (_key, value, _ttl_ms) = read_single_entry(&mut cursor)?;
        let ttl = if remaining > 0 {
            Some(Duration::from_millis(remaining as u64))
        } else {
            None
        };
        Ok(Some((value, ttl)))
    }

    pub fn remove(&mut self, key: &str) {
        if let Some(de) = self.index.remove(key) {
            self.dead_bytes += de.length as usize;
        }
    }

    pub fn contains(&self, key: &str) -> bool {
        self.index.contains_key(key)
    }

    pub fn contains_valid(&self, key: &str, now: Instant) -> bool {
        match self.index.get(key) {
            Some(de) => !de.is_expired(now),
            None => false,
        }
    }

    pub fn keys(&self) -> impl Iterator<Item = &String> {
        self.index.keys()
    }

    pub fn len(&self) -> usize {
        self.index.len()
    }

    /// Check if compaction would be worthwhile. Triggers when >30% of the
    /// data file is dead bytes (overwritten/deleted entries), or when dead
    /// bytes exceed 100MB absolute.
    pub fn should_compact(&self) -> bool {
        if self.dead_bytes == 0 {
            return false;
        }
        let ratio = self.dead_bytes as f64 / self.total_bytes.max(1) as f64;
        (self.total_bytes > 64 * 1024 && ratio > 0.3) || self.dead_bytes > 100 * 1024 * 1024
    }

    /// Rewrite the data file keeping only live entries. Creates a new file,
    /// copies live data, then atomic-renames over the old file. Reclaims all
    /// dead bytes from overwritten/deleted entries.
    pub fn compact(&mut self) -> io::Result<()> {
        let tmp_path = self.path.with_extension("compact.tmp");
        let tmp_file = File::create(&tmp_path)?;
        let mut writer = BufWriter::new(tmp_file);

        let mut new_index = HashMap::new();
        let mut new_total: usize = 0;

        let keys: Vec<String> = self.index.keys().cloned().collect();
        for key in &keys {
            let de = &self.index[key];
            let mut buf = vec![0u8; de.length as usize];
            self.data_file.seek(SeekFrom::Start(de.offset))?;
            self.data_file.read_exact(&mut buf)?;

            let new_offset = new_total as u64;
            writer.write_all(&buf)?;
            new_index.insert(
                key.clone(),
                DiskEntry {
                    offset: new_offset,
                    length: de.length,
                    ttl_ms: de.ttl_ms,
                    created_at: de.created_at,
                },
            );
            new_total += de.length as usize;
        }

        writer.flush()?;
        drop(writer);

        fs::rename(&tmp_path, &self.path)?;
        self.data_file = OpenOptions::new()
            .read(true)
            .append(true)
            .open(&self.path)?;
        self.index = new_index;
        self.total_bytes = new_total;
        self.dead_bytes = 0;
        Ok(())
    }

    pub fn dump_all(&mut self, now: Instant) -> io::Result<Vec<DumpEntry>> {
        let mut entries = Vec::new();
        let keys: Vec<String> = self.index.keys().cloned().collect();
        for key in keys {
            if let Some((value, _ttl)) = self.get(&key, now)? {
                let de = &self.index[&key];
                let ttl_ms = de.remaining_ttl_ms(now);
                entries.push(DumpEntry { key, value, ttl_ms });
            }
        }
        Ok(entries)
    }

    /// Scan the data file from start to end, rebuilding the in-memory index.
    /// Called on startup to recover the index from an existing data file.
    /// If a key appears multiple times (from re-evictions), the last occurrence
    /// wins and earlier ones become dead bytes.
    fn rebuild_index(&mut self) -> io::Result<()> {
        let file_len = self.data_file.seek(SeekFrom::End(0))?;
        if file_len == 0 {
            return Ok(());
        }
        self.data_file.seek(SeekFrom::Start(0))?;
        let now = Instant::now();

        loop {
            let start = self.data_file.stream_position()?;
            if start >= file_len {
                break;
            }
            match read_single_entry(&mut self.data_file) {
                Ok((key, _value, ttl_ms)) => {
                    let end_pos = self.data_file.stream_position()?;
                    let length = (end_pos - start) as u32;

                    if let Some(old) = self.index.insert(
                        key,
                        DiskEntry {
                            offset: start,
                            length,
                            ttl_ms,
                            created_at: now,
                        },
                    ) {
                        self.dead_bytes += old.length as usize;
                    }
                    self.total_bytes += length as usize;
                }
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
        }
        self.data_file.seek(SeekFrom::End(0))?;
        Ok(())
    }
}

fn write_u32(w: &mut impl Write, v: u32) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_i64(w: &mut impl Write, v: i64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_f64(w: &mut impl Write, v: f64) -> io::Result<()> {
    w.write_all(&v.to_le_bytes())
}

fn write_bytes(w: &mut impl Write, data: &[u8]) -> io::Result<()> {
    w.write_all(&(data.len() as u32).to_le_bytes())?;
    w.write_all(data)
}

fn read_u32(r: &mut impl Read) -> io::Result<u32> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_i64(r: &mut impl Read) -> io::Result<i64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
}

fn read_f64(r: &mut impl Read) -> io::Result<f64> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(f64::from_le_bytes(buf))
}

fn read_bytes(r: &mut impl Read) -> io::Result<Vec<u8>> {
    let len = read_u32(r)? as usize;
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf)?;
    Ok(buf)
}

fn read_string(r: &mut impl Read) -> io::Result<String> {
    let raw = read_bytes(r)?;
    String::from_utf8(raw).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

pub fn write_single_entry(w: &mut impl Write, entry: &DumpEntry) -> io::Result<()> {
    let type_byte: u8 = match &entry.value {
        DumpValue::Str(_) => b'S',
        DumpValue::List(_) => b'L',
        DumpValue::Hash(_) => b'H',
        DumpValue::Set(_) => b'T',
        DumpValue::SortedSet(_) => b'Z',
        DumpValue::Stream(..) => b'X',
        DumpValue::Vector(..) => b'V',
        DumpValue::HyperLogLog(..) => b'P',
        DumpValue::TimeSeries(..) => b'I',
    };
    w.write_all(&[type_byte])?;
    write_bytes(w, entry.key.as_bytes())?;
    let ttl = if entry.ttl_ms > 0 { entry.ttl_ms } else { -1 };
    write_i64(w, ttl)?;

    match &entry.value {
        DumpValue::Str(v) => write_bytes(w, v)?,
        DumpValue::List(items) => {
            write_u32(w, items.len() as u32)?;
            for item in items {
                write_bytes(w, item)?;
            }
        }
        DumpValue::Hash(pairs) => {
            write_u32(w, pairs.len() as u32)?;
            for (k, v) in pairs {
                write_bytes(w, k.as_bytes())?;
                write_bytes(w, v)?;
            }
        }
        DumpValue::Set(members) => {
            write_u32(w, members.len() as u32)?;
            for m in members {
                write_bytes(w, m.as_bytes())?;
            }
        }
        DumpValue::SortedSet(members) => {
            write_u32(w, members.len() as u32)?;
            for (m, score) in members {
                write_bytes(w, m.as_bytes())?;
                write_f64(w, *score)?;
            }
        }
        DumpValue::Stream(stream_entries, last_id) => {
            write_bytes(w, last_id.as_bytes())?;
            write_u32(w, stream_entries.len() as u32)?;
            for (id, fields) in stream_entries {
                write_bytes(w, id.as_bytes())?;
                write_u32(w, fields.len() as u32)?;
                for (k, v) in fields {
                    write_bytes(w, k.as_bytes())?;
                    write_bytes(w, v)?;
                }
            }
        }
        DumpValue::Vector(data, metadata) => {
            write_u32(w, data.len() as u32)?;
            for f in data {
                w.write_all(&f.to_le_bytes())?;
            }
            match metadata {
                Some(m) => {
                    w.write_all(&[1u8])?;
                    write_bytes(w, m.as_bytes())?;
                }
                None => w.write_all(&[0u8])?,
            }
        }
        DumpValue::HyperLogLog(regs, _) => {
            write_u32(w, regs.len() as u32)?;
            w.write_all(regs)?;
        }
        DumpValue::TimeSeries(samples, retention, labels) => {
            write_u32(w, samples.len() as u32)?;
            for (ts, val) in samples {
                write_i64(w, *ts)?;
                write_f64(w, *val)?;
            }
            write_i64(w, *retention as i64)?;
            write_u32(w, labels.len() as u32)?;
            for (k, v) in labels {
                write_bytes(w, k.as_bytes())?;
                write_bytes(w, v.as_bytes())?;
            }
        }
    }
    Ok(())
}

pub fn read_single_entry(r: &mut impl Read) -> io::Result<(String, DumpValue, i64)> {
    let mut type_buf = [0u8; 1];
    r.read_exact(&mut type_buf)?;

    let key = read_string(r)?;
    let ttl_ms = read_i64(r)?;

    let value = match type_buf[0] {
        b'S' => DumpValue::Str(read_bytes(r)?),
        b'L' => {
            let len = read_u32(r)? as usize;
            let mut items = Vec::with_capacity(len);
            for _ in 0..len {
                items.push(read_bytes(r)?);
            }
            DumpValue::List(items)
        }
        b'H' => {
            let len = read_u32(r)? as usize;
            let mut pairs = Vec::with_capacity(len);
            for _ in 0..len {
                let k = read_string(r)?;
                let v = read_bytes(r)?;
                pairs.push((k, v));
            }
            DumpValue::Hash(pairs)
        }
        b'T' => {
            let len = read_u32(r)? as usize;
            let mut members = Vec::with_capacity(len);
            for _ in 0..len {
                members.push(read_string(r)?);
            }
            DumpValue::Set(members)
        }
        b'Z' => {
            let len = read_u32(r)? as usize;
            let mut members = Vec::with_capacity(len);
            for _ in 0..len {
                let m = read_string(r)?;
                let s = read_f64(r)?;
                members.push((m, s));
            }
            DumpValue::SortedSet(members)
        }
        b'X' => {
            let last_id = read_string(r)?;
            let entry_count = read_u32(r)? as usize;
            let mut entries = Vec::with_capacity(entry_count);
            for _ in 0..entry_count {
                let id = read_string(r)?;
                let field_count = read_u32(r)? as usize;
                let mut fields = Vec::with_capacity(field_count);
                for _ in 0..field_count {
                    let k = read_string(r)?;
                    let v = read_bytes(r)?;
                    fields.push((k, v));
                }
                entries.push((id, fields));
            }
            DumpValue::Stream(entries, last_id)
        }
        b'V' => {
            let dims = read_u32(r)? as usize;
            let mut data = Vec::with_capacity(dims);
            for _ in 0..dims {
                let mut buf = [0u8; 4];
                r.read_exact(&mut buf)?;
                data.push(f32::from_le_bytes(buf));
            }
            let mut flag = [0u8; 1];
            r.read_exact(&mut flag)?;
            let metadata = if flag[0] == 1 {
                Some(read_string(r)?)
            } else {
                None
            };
            DumpValue::Vector(data, metadata)
        }
        b'P' => {
            let len = read_u32(r)? as usize;
            let mut regs = vec![0u8; len];
            r.read_exact(&mut regs)?;
            let cached = crate::hll::hll_count(&regs);
            DumpValue::HyperLogLog(regs, cached)
        }
        b'I' => {
            let sample_count = read_u32(r)? as usize;
            let mut samples = Vec::with_capacity(sample_count);
            for _ in 0..sample_count {
                let ts = read_i64(r)?;
                let val = read_f64(r)?;
                samples.push((ts, val));
            }
            let retention = read_i64(r)? as u64;
            let label_count = read_u32(r)? as usize;
            let mut labels = Vec::with_capacity(label_count);
            for _ in 0..label_count {
                let k = read_string(r)?;
                let v = read_string(r)?;
                labels.push((k, v));
            }
            DumpValue::TimeSeries(samples, retention, labels)
        }
        other => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("unknown type byte: {other}"),
            ))
        }
    };

    Ok((key, value, ttl_ms))
}
