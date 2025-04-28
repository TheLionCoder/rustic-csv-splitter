use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::sync::{Arc, Mutex};

use super::file_context::FileContext;

#[derive(Clone)]
pub struct RecordProcessingContext<'a> {
    pub file_headers: csv::StringRecord,
    pub split_column_idx: usize,
    pub category_writers: Arc<Mutex<HashMap<String, csv::Writer<BufWriter<File>>>>>,
    pub header_indexes: Vec<usize>,
    pub file_context: FileContext<'a>,
}
