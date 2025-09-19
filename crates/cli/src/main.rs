use std::path::PathBuf;

use anyhow::Result;
use chrono::NaiveDate;
use clap::{Parser, Subcommand, ValueEnum};
use comfy_table::{presets::UTF8_FULL, Cell, Row, Table};

use fsindex_core::{DuplicateGroup, FileIndexer, FileRecord, SearchQuery, SortKey};

#[derive(Parser)]
#[command(
    name = "fsindex",
    version,
    about = "File system indexing and search utility"
)]
struct Cli {
    #[arg(long, default_value = "index.db", help = "Path to the SQLite database")]
    db: PathBuf,
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Index a directory recursively
    Index {
        #[arg(help = "Root directory to index")]
        path: PathBuf,
        #[arg(long, help = "Compute and store file hashes")]
        hash: bool,
    },
    /// Search files using optional filters
    Search {
        #[arg(long, help = "Filter by name fragment")]
        name: Option<String>,
        #[arg(long, help = "Filter by file extension")]
        ext: Option<String>,
        #[arg(long, help = "Minimum file size in bytes")]
        min_size: Option<i64>,
        #[arg(long, help = "Maximum file size in bytes")]
        max_size: Option<i64>,
        #[arg(long, help = "Earliest modified date (YYYY-MM-DD)")]
        from: Option<String>,
        #[arg(long, help = "Latest modified date (YYYY-MM-DD)")]
        to: Option<String>,
        #[arg(long, value_enum, default_value_t = OrderKey::Name, help = "Sort column")]
        sort: OrderKey,
        #[arg(long, help = "Sort descending instead of ascending")]
        desc: bool,
        #[arg(long, default_value_t = 50, help = "Limit number of rows")]
        limit: i64,
        #[arg(long, default_value_t = 0, help = "Offset for pagination")]
        offset: i64,
    },
    /// Show most recently indexed files
    Recent {
        #[arg(long, default_value_t = 50, help = "Number of rows to fetch")]
        limit: i64,
    },
    /// Display duplicate files grouped by hash
    Duplicates {
        #[arg(
            long,
            default_value_t = 25,
            help = "Maximum number of duplicate groups"
        )]
        limit: i64,
    },
}

#[derive(Clone, Copy, ValueEnum)]
enum OrderKey {
    Name,
    Size,
    Modified,
}

impl From<OrderKey> for SortKey {
    fn from(value: OrderKey) -> Self {
        match value {
            OrderKey::Name => SortKey::Name,
            OrderKey::Size => SortKey::Size,
            OrderKey::Modified => SortKey::Modified,
        }
    }
}

fn main() -> Result<()> {
    let cli = Cli::parse();
    let indexer = FileIndexer::new(&cli.db)?;

    match cli.command {
        Commands::Index { path, hash } => {
            let count = indexer.index_dir(path, hash)?;
            println!("Indexed {} files", count);
        }
        Commands::Search {
            name,
            ext,
            min_size,
            max_size,
            from,
            to,
            sort,
            desc,
            limit,
            offset,
        } => {
            let mut query = SearchQuery::default();
            query.name_like = name;
            query.ext = ext;
            query.min_size = min_size;
            query.max_size = max_size;
            query.date_from = parse_date_opt(from);
            query.date_to = parse_date_opt(to);
            query.sort_key = Some(sort.into());
            query.desc = desc;
            query.limit = Some(limit);
            query.offset = Some(offset);

            let rows = indexer.search(&query)?;
            render_records(rows);
        }
        Commands::Recent { limit } => {
            let rows = indexer.recently_added(limit)?;
            render_records(rows);
        }
        Commands::Duplicates { limit } => {
            let groups = indexer.duplicate_groups(limit)?;
            render_duplicates(groups);
        }
    }

    Ok(())
}

fn parse_date_opt(input: Option<String>) -> Option<NaiveDate> {
    input
        .as_deref()
        .map(|s| NaiveDate::parse_from_str(s, "%Y-%m-%d").ok())
        .flatten()
}

fn render_records(rows: Vec<FileRecord>) {
    let mut table = Table::new();
    table.load_preset(UTF8_FULL);
    table.set_header(Row::from(vec![
        Cell::new("Name"),
        Cell::new("Ext"),
        Cell::new("Size"),
        Cell::new("Modified"),
        Cell::new("Path"),
    ]));

    for record in rows {
        table.add_row(Row::from(vec![
            Cell::new(record.name),
            Cell::new(record.ext.unwrap_or_default()),
            Cell::new(human_bytes(record.size as u64)),
            Cell::new(record.modified.format("%Y-%m-%d %H:%M:%S").to_string()),
            Cell::new(record.path),
        ]));
    }

    println!("{}", table);
}

fn render_duplicates(groups: Vec<DuplicateGroup>) {
    for group in groups {
        println!(
            "hash={} size={} count={}",
            group.hash,
            human_bytes(group.size as u64),
            group.count
        );
        for path in group.paths {
            println!("  {}", path);
        }
    }
}

fn human_bytes(bytes: u64) -> String {
    const UNITS: [&str; 5] = ["B", "KB", "MB", "GB", "TB"];
    let mut unit = 0usize;
    let mut value = bytes as f64;

    while value >= 1024.0 && unit < UNITS.len() - 1 {
        value /= 1024.0;
        unit += 1;
    }

    if unit == 0 {
        format!("{} {}", bytes, UNITS[unit])
    } else {
        format!("{:.2} {}", value, UNITS[unit])
    }
}
