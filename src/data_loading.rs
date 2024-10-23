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

mod tests {
    use super::*;

    #[test]
    fn test_read_file_comma_delimiter() {
        let path: &str = "././assets/city.csv";
        let file_path: &Path = Path::new(path);
        let mut reader: Reader<File> = read_file(file_path, b',').unwrap();
        let headers = reader.headers().unwrap();
        assert_eq!(
            headers,
            vec!["City", "State", "Population", "Latitude", "Longitude"]
        )
    }

    #[test]
    fn test_get_file_name() {
        let path: &str = "././assets/city.csv";
        let file_path: &Path = Path::new(path);
        let file_stem: &str = extract_file_name(file_path).unwrap();
        assert_eq!(file_stem, "city")
    }
}
