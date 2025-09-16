use dashmap::DashMap;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use super::file_context::FileContext;

#[derive(Clone)]
pub struct RecordProcessingContext {
    pub file_headers: csv::StringRecord,
    pub split_column_idx: usize,
    pub category_writers: Arc<DashMap<String, csv::Writer<BufWriter<File>>>>,
    pub header_indexes: Vec<usize>,
    pub file_context: FileContext,
}
