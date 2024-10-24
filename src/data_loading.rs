use csv::{Reader, ReaderBuilder};
use std::fs::File;
use std::path::Path;

#[allow(dead_code)]
pub(crate) fn read_file(path: &Path, delimiter: u8) -> Result<Reader<File>, csv::Error> {
    let reader: Reader<File> = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(delimiter)
        .from_path(path)?;
    Ok(reader)
}

pub(crate) fn extract_file_name(path: &Path) -> Result<&str, Box<dyn std::error::Error>> {
    let file_stem: &str = path.file_stem().unwrap().to_str().unwrap();
    Ok(file_stem)
}
