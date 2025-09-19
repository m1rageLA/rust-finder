use std::fmt;
use std::fs::File;
use std::io::Read;
use std::path::Path;

use anyhow::{anyhow, Context, Result};
use blake3::Hasher;
use chrono::{DateTime, NaiveDate, Utc};
use rusqlite::{
    params, params_from_iter,
    types::{Type, Value},
    Connection,
};
use serde::{Deserialize, Serialize};
use walkdir::WalkDir;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FileRecord {
    pub path: String,
    pub name: String,
    pub ext: Option<String>,
    pub size: i64,
    pub modified: DateTime<Utc>,
    pub added_at: DateTime<Utc>,
    pub hash: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DuplicateGroup {
    pub hash: String,
    pub size: i64,
    pub count: i64,
    pub paths: Vec<String>,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, Default, PartialEq, Eq)]
pub enum SortKey {
    #[default]
    Name,
    Size,
    Modified,
}

#[derive(Debug, Clone, Default)]
pub struct SearchQuery {
    pub name_like: Option<String>,
    pub ext: Option<String>,
    pub min_size: Option<i64>,
    pub max_size: Option<i64>,
    pub date_from: Option<NaiveDate>,
    pub date_to: Option<NaiveDate>,
    pub sort_key: Option<SortKey>,
    pub desc: bool,
    pub limit: Option<i64>,
    pub offset: Option<i64>,
}

pub struct FileIndexer {
    conn: Connection,
}

impl FileIndexer {
    pub fn new<P: AsRef<Path>>(path: P) -> Result<Self> {
        let conn = Connection::open(path)?;
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;\n
             CREATE TABLE IF NOT EXISTS files (
                 path TEXT PRIMARY KEY,
                 name TEXT NOT NULL,
                 ext TEXT,
                 size INTEGER NOT NULL,
                 modified INTEGER NOT NULL,
                 added_at INTEGER NOT NULL DEFAULT (strftime('%s','now')),
                 hash TEXT
             );
             CREATE INDEX IF NOT EXISTS idx_files_name ON files(name);
             CREATE INDEX IF NOT EXISTS idx_files_ext ON files(ext);
             CREATE INDEX IF NOT EXISTS idx_files_modified ON files(modified);
             CREATE INDEX IF NOT EXISTS idx_files_hash ON files(hash)
            ",
        )?;
        Ok(Self { conn })
    }

    pub fn index_dir<P: AsRef<Path>>(&self, root: P, hash: bool) -> Result<usize> {
        let mut count = 0usize;
        for entry in WalkDir::new(root) {
            let entry = entry?;
            if !entry.file_type().is_file() {
                continue;
            }
            let record = self.build_record(entry.path(), hash)?;
            self.upsert(&record)?;
            count += 1;
        }
        Ok(count)
    }

    fn build_record(&self, path: &Path, hash: bool) -> Result<FileRecord> {
        let metadata = path
            .metadata()
            .with_context(|| format!("reading metadata for {}", path.display()))?;
        if !metadata.is_file() {
            return Err(anyhow!("{} is not a regular file", path.display()));
        }
        let name = path
            .file_name()
            .and_then(|s| s.to_str())
            .ok_or_else(|| anyhow!("file name is not valid UTF-8: {}", path.display()))?
            .to_string();
        let ext = path
            .extension()
            .and_then(|s| s.to_str())
            .map(|s| s.to_ascii_lowercase());
        let size = i64::try_from(metadata.len())
            .with_context(|| format!("file is larger than 9 exabytes: {}", path.display()))?;
        let modified = metadata
            .modified()
            .with_context(|| format!("missing modified time for {}", path.display()))?;
        let modified = DateTime::<Utc>::from(modified);
        let added_at = Utc::now();
        let hash = if hash {
            Some(compute_hash(path)?)
        } else {
            None
        };
        Ok(FileRecord {
            path: path.to_string_lossy().to_string(),
            name,
            ext,
            size,
            modified,
            added_at,
            hash,
        })
    }

    fn upsert(&self, rec: &FileRecord) -> Result<()> {
        self.conn.execute(
            "INSERT INTO files(path,name,ext,size,modified,added_at,hash)
             VALUES(?,?,?,?,?,?,?)
             ON CONFLICT(path) DO UPDATE SET
                 name=excluded.name,
                 ext=excluded.ext,
                 size=excluded.size,
                 modified=excluded.modified,
                 hash=excluded.hash",
            params![
                rec.path,
                rec.name,
                rec.ext.as_deref(),
                rec.size,
                rec.modified.timestamp(),
                rec.added_at.timestamp(),
                rec.hash.as_deref()
            ],
        )?;
        Ok(())
    }

    pub fn search(&self, q: &SearchQuery) -> Result<Vec<FileRecord>> {
        let mut sql = String::from("SELECT path,name,ext,size,modified,added_at,hash FROM files");
        let mut conds: Vec<String> = Vec::new();
        let mut params_vec: Vec<Value> = Vec::new();

        if let Some(name) = q.name_like.as_ref().filter(|s| !s.is_empty()) {
            conds.push("name LIKE ?".to_string());
            params_vec.push(Value::Text(format!("%{}%", name)));
        }
        if let Some(ext) = q.ext.as_ref().filter(|s| !s.is_empty()) {
            conds.push("ext = ?".to_string());
            params_vec.push(Value::Text(ext.to_ascii_lowercase()));
        }
        if let Some(min_size) = q.min_size {
            conds.push("size >= ?".to_string());
            params_vec.push(Value::Integer(min_size));
        }
        if let Some(max_size) = q.max_size {
            conds.push("size <= ?".to_string());
            params_vec.push(Value::Integer(max_size));
        }
        if let Some(date) = q.date_from {
            let ts = date
                .and_hms_opt(0, 0, 0)
                .ok_or_else(|| anyhow!("invalid from date"))?
                .and_utc()
                .timestamp();
            conds.push("modified >= ?".to_string());
            params_vec.push(Value::Integer(ts));
        }
        if let Some(date) = q.date_to {
            let ts = date
                .and_hms_opt(23, 59, 59)
                .ok_or_else(|| anyhow!("invalid to date"))?
                .and_utc()
                .timestamp();
            conds.push("modified <= ?".to_string());
            params_vec.push(Value::Integer(ts));
        }

        if !conds.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conds.join(" AND "));
        }
        sql.push_str(" ORDER BY ");
        match q.sort_key.unwrap_or_default() {
            SortKey::Name => sql.push_str("name"),
            SortKey::Size => sql.push_str("size"),
            SortKey::Modified => sql.push_str("modified"),
        }
        if q.desc {
            sql.push_str(" DESC");
        }
        if let Some(limit) = q.limit {
            sql.push_str(&format!(" LIMIT {}", limit));
        }
        if let Some(offset) = q.offset {
            sql.push_str(&format!(" OFFSET {}", offset));
        }

        let params = params_from_iter(params_vec.into_iter());
        let mut stmt = self.conn.prepare(&sql)?;
        let rows = stmt.query_map(params, |row| {
            let modified_ts = row.get::<_, i64>(4)?;
            let added_ts = row.get::<_, i64>(5)?;
            Ok(FileRecord {
                path: row.get(0)?,
                name: row.get(1)?,
                ext: row.get(2)?,
                size: row.get(3)?,
                modified: decode_timestamp(modified_ts, "modified", 4)?,
                added_at: decode_timestamp(added_ts, "added_at", 5)?,
                hash: row.get(6)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub fn recently_added(&self, limit: i64) -> Result<Vec<FileRecord>> {
        let mut stmt = self.conn.prepare(
            "SELECT path,name,ext,size,modified,added_at,hash
             FROM files
             ORDER BY added_at DESC
             LIMIT ?",
        )?;
        let rows = stmt.query_map(params![limit], |row| {
            let modified_ts = row.get::<_, i64>(4)?;
            let added_ts = row.get::<_, i64>(5)?;
            Ok(FileRecord {
                path: row.get(0)?,
                name: row.get(1)?,
                ext: row.get(2)?,
                size: row.get(3)?,
                modified: decode_timestamp(modified_ts, "modified", 4)?,
                added_at: decode_timestamp(added_ts, "added_at", 5)?,
                hash: row.get(6)?,
            })
        })?;
        Ok(rows.filter_map(|r| r.ok()).collect())
    }

    pub fn duplicate_groups(&self, limit: i64) -> Result<Vec<DuplicateGroup>> {
        let mut stmt = self.conn.prepare(
            "SELECT hash,size,COUNT(*) as c
             FROM files
             WHERE hash IS NOT NULL
             GROUP BY hash,size
             HAVING c > 1
             ORDER BY c DESC
             LIMIT ?",
        )?;
        let groups = stmt.query_map(params![limit], |row| {
            Ok((
                row.get::<_, String>(0)?,
                row.get::<_, i64>(1)?,
                row.get::<_, i64>(2)?,
            ))
        })?;

        let mut out = Vec::new();
        for group in groups.filter_map(|r| r.ok()) {
            let mut stmt_paths = self
                .conn
                .prepare("SELECT path FROM files WHERE hash = ? ORDER BY name")?;
            let paths = stmt_paths.query_map(params![&group.0], |row| row.get::<_, String>(0))?;
            let mut collected = Vec::new();
            for path in paths.filter_map(|r| r.ok()) {
                collected.push(path);
            }
            out.push(DuplicateGroup {
                hash: group.0,
                size: group.1,
                count: group.2,
                paths: collected,
            });
        }
        Ok(out)
    }
}

fn compute_hash(path: &Path) -> Result<String> {
    let mut file =
        File::open(path).with_context(|| format!("opening {} for hashing", path.display()))?;
    let mut hasher = Hasher::new();
    let mut buf = [0u8; 8192];
    loop {
        let read = file
            .read(&mut buf)
            .with_context(|| format!("reading {} for hashing", path.display()))?;
        if read == 0 {
            break;
        }
        hasher.update(&buf[..read]);
    }
    Ok(hasher.finalize().to_hex().to_string())
}

fn decode_timestamp(
    ts: i64,
    column: &'static str,
    index: usize,
) -> rusqlite::Result<DateTime<Utc>> {
    DateTime::<Utc>::from_timestamp(ts, 0).ok_or_else(|| {
        rusqlite::Error::FromSqlConversionFailure(
            index,
            Type::Integer,
            Box::new(TimestampOutOfRange { column, value: ts }),
        )
    })
}

#[derive(Debug)]
struct TimestampOutOfRange {
    column: &'static str,
    value: i64,
}

impl fmt::Display for TimestampOutOfRange {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{} timestamp out of range: {}", self.column, self.value)
    }
}

impl std::error::Error for TimestampOutOfRange {}
