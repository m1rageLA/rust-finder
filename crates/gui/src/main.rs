use std::path::PathBuf;

use anyhow::{anyhow, Result};
use chrono::{DateTime, NaiveDate, Utc};
use eframe::{egui, App as EguiApp, Frame, NativeOptions};
use fsindex_core::{FileIndexer, FileRecord, SearchQuery, SortKey};

fn main() -> Result<()> {
    let options = NativeOptions::default();
    eframe::run_native(
        "File Search Indexer",
        options,
        Box::new(|_cc| Box::new(FsIndexApp::default())),
    )
    .map_err(|err| anyhow!(err.to_string()))?;
    Ok(())
}

struct FsIndexApp {
    db_path: String,
    index_dir: Option<PathBuf>,
    index_hash: bool,
    name_like: String,
    ext: String,
    min_size: String,
    max_size: String,
    from: String,
    to: String,
    sort_idx: usize,
    desc: bool,
    limit: String,
    offset: String,
    results: Vec<FileRecord>,
    status: String,
    tab: usize,
}

impl Default for FsIndexApp {
    fn default() -> Self {
        Self {
            db_path: "index.db".into(),
            index_dir: None,
            index_hash: true,
            name_like: String::new(),
            ext: String::new(),
            min_size: String::new(),
            max_size: String::new(),
            from: String::new(),
            to: String::new(),
            sort_idx: 0,
            desc: false,
            limit: "50".into(),
            offset: "0".into(),
            results: Vec::new(),
            status: String::new(),
            tab: 0,
        }
    }
}

impl EguiApp for FsIndexApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut Frame) {
        egui::TopBottomPanel::top("top").show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.label("DB");
                ui.text_edit_singleline(&mut self.db_path);
                if ui.button("Open").clicked() {
                    if let Some(path) = rfd::FileDialog::new().set_directory(".").pick_file() {
                        self.db_path = path.to_string_lossy().to_string();
                    }
                }
                if ui.button("Pick Folder").clicked() {
                    self.index_dir = rfd::FileDialog::new().pick_folder();
                }
                ui.checkbox(&mut self.index_hash, "Hash");
                if ui.button("Index").clicked() {
                    match FileIndexer::new(&self.db_path).and_then(|idx| {
                        idx.index_dir(
                            self.index_dir.clone().unwrap_or_else(|| PathBuf::from(".")),
                            self.index_hash,
                        )
                    }) {
                        Ok(count) => self.status = format!("Indexed {} files", count),
                        Err(err) => self.status = err.to_string(),
                    }
                }
                ui.label(&self.status);
            });
        });

        egui::TopBottomPanel::bottom("bottom").show(ctx, |ui| {
            ui.label("CLI: fsindex --db index.db <command>");
        });

        egui::CentralPanel::default().show(ctx, |ui| {
            ui.horizontal(|ui| {
                ui.selectable_value(&mut self.tab, 0, "Search");
                ui.selectable_value(&mut self.tab, 1, "Recent");
                ui.selectable_value(&mut self.tab, 2, "Duplicates");
            });

            match self.tab {
                0 => self.ui_search(ui),
                1 => self.ui_recent(ui),
                _ => self.ui_duplicates(ui),
            }
        });
    }
}

impl FsIndexApp {
    fn parse_num(text: &str) -> Option<i64> {
        text.trim().parse::<i64>().ok()
    }

    fn parse_date(text: &str) -> Option<NaiveDate> {
        NaiveDate::parse_from_str(text.trim(), "%Y-%m-%d").ok()
    }

    fn current_indexer(&self) -> Option<FileIndexer> {
        FileIndexer::new(&self.db_path).ok()
    }

    fn ui_search(&mut self, ui: &mut egui::Ui) {
        ui.horizontal(|ui| {
            ui.label("Name");
            ui.text_edit_singleline(&mut self.name_like);
            ui.label("Ext");
            ui.text_edit_singleline(&mut self.ext);
            ui.label("Min");
            ui.text_edit_singleline(&mut self.min_size);
            ui.label("Max");
            ui.text_edit_singleline(&mut self.max_size);
        });

        ui.horizontal(|ui| {
            ui.label("From");
            ui.text_edit_singleline(&mut self.from);
            ui.label("To");
            ui.text_edit_singleline(&mut self.to);
            ui.label("Sort");
            egui::ComboBox::from_id_source("sort")
                .selected_text(["Name", "Size", "Modified"][self.sort_idx])
                .show_ui(ui, |ui| {
                    for (idx, label) in ["Name", "Size", "Modified"].iter().enumerate() {
                        if ui.selectable_label(self.sort_idx == idx, *label).clicked() {
                            self.sort_idx = idx;
                        }
                    }
                });
            ui.checkbox(&mut self.desc, "Desc");
            ui.label("Limit");
            ui.text_edit_singleline(&mut self.limit);
            ui.label("Offset");
            ui.text_edit_singleline(&mut self.offset);
            if ui.button("Search").clicked() {
                if let Some(indexer) = self.current_indexer() {
                    let mut query = SearchQuery::default();
                    if !self.name_like.trim().is_empty() {
                        query.name_like = Some(self.name_like.clone());
                    }
                    if !self.ext.trim().is_empty() {
                        query.ext = Some(self.ext.clone());
                    }
                    query.min_size = Self::parse_num(&self.min_size);
                    query.max_size = Self::parse_num(&self.max_size);
                    query.date_from = Self::parse_date(&self.from);
                    query.date_to = Self::parse_date(&self.to);
                    query.sort_key = Some(match self.sort_idx {
                        0 => SortKey::Name,
                        1 => SortKey::Size,
                        _ => SortKey::Modified,
                    });
                    query.desc = self.desc;
                    query.limit = Self::parse_num(&self.limit);
                    query.offset = Self::parse_num(&self.offset);

                    if let Ok(rows) = indexer.search(&query) {
                        self.results = rows;
                    }
                }
            }
        });

        egui::ScrollArea::vertical().show(ui, |ui| {
            egui::Grid::new("results").striped(true).show(ui, |ui| {
                ui.heading("Name");
                ui.heading("Ext");
                ui.heading("Size");
                ui.heading("Modified");
                ui.heading("Path");
                ui.end_row();

                for record in &self.results {
                    ui.label(&record.name);
                    ui.label(record.ext.clone().unwrap_or_default());
                    ui.label(human_bytes(record.size as u64));
                    ui.label(record.modified.format("%Y-%m-%d %H:%M:%S").to_string());
                    ui.label(&record.path);
                    ui.end_row();
                }
            });
        });
    }

    fn ui_recent(&mut self, ui: &mut egui::Ui) {
        if ui.button("Reload").clicked() {
            if let Some(indexer) = self.current_indexer() {
                if let Ok(rows) = indexer.recently_added(200) {
                    self.results = rows;
                }
            }
        }

        egui::ScrollArea::vertical().show(ui, |ui| {
            egui::Grid::new("recent").striped(true).show(ui, |ui| {
                ui.heading("Added");
                ui.heading("Name");
                ui.heading("Path");
                ui.end_row();

                for record in &self.results {
                    ui.label(record.added_at.format("%Y-%m-%d %H:%M:%S").to_string());
                    ui.label(&record.name);
                    ui.label(&record.path);
                    ui.end_row();
                }
            });
        });
    }

    fn ui_duplicates(&mut self, ui: &mut egui::Ui) {
        if ui.button("Find").clicked() {
            if let Some(indexer) = self.current_indexer() {
                if let Ok(groups) = indexer.duplicate_groups(100) {
                    self.results.clear();
                    let epoch = epoch_time();
                    for group in groups {
                        let hash = group.hash.clone();
                        let size = group.size;
                        for path in group.paths {
                            self.results.push(FileRecord {
                                path,
                                name: String::new(),
                                ext: None,
                                size,
                                modified: epoch,
                                added_at: epoch,
                                hash: Some(hash.clone()),
                            });
                        }
                    }
                }
            }
        }

        egui::ScrollArea::vertical().show(ui, |ui| {
            egui::Grid::new("duplicates").striped(true).show(ui, |ui| {
                ui.heading("Hash");
                ui.heading("Size");
                ui.heading("Path");
                ui.end_row();

                for record in &self.results {
                    ui.label(record.hash.clone().unwrap_or_default());
                    ui.label(human_bytes(record.size as u64));
                    ui.label(&record.path);
                    ui.end_row();
                }
            });
        });
    }
}

fn epoch_time() -> DateTime<Utc> {
    DateTime::<Utc>::from_timestamp(0, 0).unwrap()
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
