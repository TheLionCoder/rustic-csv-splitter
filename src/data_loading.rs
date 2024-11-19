use crate::delimiter::Delimiter;
use csv::{Reader, ReaderBuilder};
use std::fs::File;
use std::path::Path;
use std::string::String;

pub(crate) fn read_file(path: &Path, delimiter: &Delimiter) -> Result<Reader<File>, csv::Error> {
    let reader: Reader<File> = ReaderBuilder::new()
        .buffer_capacity(16 * 1024 * 1024)
        .has_headers(true)
        .delimiter(delimiter.clone().into())
        .from_path(path)?;

    Ok(reader)
}

pub(crate) fn extract_file_name(path: &Path) -> Result<String, Box<dyn std::error::Error>> {
    let file_stem: &str = path.file_stem().unwrap().to_str().unwrap();
    Ok(file_stem.to_string())
}
