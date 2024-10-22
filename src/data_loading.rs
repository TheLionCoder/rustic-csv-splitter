use std::fs::File;
use std::path::Path;
use csv::{Reader, ReaderBuilder, StringRecord};

pub(crate) fn read_file(path: &str, delimiter: u8) -> Result<Reader<File>, csv::Error> {
    let reader: Reader<File> = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(delimiter)
        .from_path(path)?;
    Ok(reader)
}

fn extract_file_name(path: &str) -> Result<&str, Box<dyn std::error::Error>> {
    let path: &Path = Path::new(path);
    let file_stem: &str = path.file_stem().unwrap().to_str().unwrap();
    Ok(file_stem)
}


mod tests{
    use super::*;

    #[test]
    fn test_read_file_comma_delimiter() {
        let path: &str = "././assets/city.csv";
        let mut reader: Reader<File> = read_file(path, b',').unwrap();
        let headers: &StringRecord = reader.headers().unwrap();
        assert_eq!(headers, vec!["City", "State", "Population", "Latitude", "Longitude"])
    }

    #[test]
    fn test_get_file_name() {
        let path: &str = "././assets/city.csv";
        let file_stem: &str = extract_file_name(path).unwrap();
        assert_eq!(file_stem, "city")
    }
}