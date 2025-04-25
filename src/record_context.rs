use csv::StringRecord;
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub(crate) struct RecordProcessingContext<'a> {
    pub(crate) headers: StringRecord,
    pub(crate) output_dir: &'a PathBuf,
    pub(crate) create_directory: bool,
    pub(crate) file_name: String,
    pub(crate) delimiter: u8,
    pub(crate) split_column_idx: usize,
    pub(crate) writers: Arc<Mutex<HashMap<String, csv::Writer<BufWriter<File>>>>>,
    pub(crate) header_indexes: Vec<usize>,
}
