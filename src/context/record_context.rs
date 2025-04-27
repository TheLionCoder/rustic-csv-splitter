use csv::StringRecord;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct RecordProcessingContext<'a> {
    pub headers: StringRecord,
    pub output_dir: &'a PathBuf,
    pub create_directory: bool,
    pub file_name: String,
    pub delimiter: u8,
    pub split_column_idx: usize,
    pub writers: Arc<Mutex<HashMap<String, csv::Writer<BufWriter<File>>>>>,
    pub header_indexes: Vec<usize>,
    pub chunk_size: &'a usize,
}
