use crate::delimiter::Delimiter;
use csv::StringRecord;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct RecordProcessingContext {
    pub(crate) headers: StringRecord,
    pub(crate) output_dir: PathBuf,
    pub(crate) create_directory: bool,
    pub(crate) file_name: String,
    pub(crate) delimiter: u8,
    pub(crate) split_column_idx: usize,
    pub(crate) writers: Arc<Mutex<HashMap<String, csv::Writer<BufWriter<File>>>>>,
    pub(crate) header_indexes: Vec<usize>,
}

impl Default for RecordProcessingContext {
    fn default() -> Self {
        RecordProcessingContext {
            headers: StringRecord::new(),
            output_dir: PathBuf::new(),
            create_directory: false,
            file_name: String::new(),
            delimiter: Delimiter::PIPE,
            split_column_idx: 0,
            writers: Arc::new(Mutex::new(HashMap::new())),
            header_indexes: Vec::new(),
        }
    }
}
