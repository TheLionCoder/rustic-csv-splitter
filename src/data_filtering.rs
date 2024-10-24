use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::{Arc, Mutex, MutexGuard};

use csv::{Reader, StringRecord};
use rayon::prelude::*;

use crate::data_loading::{extract_file_name, read_file};

#[derive(Clone)]
struct RecordProcessingContext<'a> {
    headers: &'a [&'a str],
    output_dir: &'a Path,
    create_directory: bool,
    file_name: &'a str,
}

#[allow(dead_code)]
pub(crate) fn split_file_by_category(
    path: &Path,
    input_column: &str,
    output_dir: &Path,
    create_directory: bool,
    delimiter: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader: Reader<File> = read_file(path, delimiter)?;

    let file_name: &str = extract_file_name(path)?;
    let headers: StringRecord = reader.headers()?.clone();
    let headers_vec: Vec<&str> = headers.iter().collect();

    // Get the index of the column to split by
    let split_column_idx: usize = headers.iter().position(|h| h == input_column).unwrap();

    let context: RecordProcessingContext = RecordProcessingContext {
        headers: &headers_vec,
        output_dir,
        create_directory,
        file_name,
    };

    // Collect records by category in parallel
    let category_records: Arc<Mutex<HashMap<String, Vec<StringRecord>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    reader.records().par_bridge().for_each(|result| {
        // Extract the category from the record
        let record: StringRecord = result.unwrap();
        let category: String = record
            .get(split_column_idx)
            .unwrap_or("unknown")
            .to_string();

        // Store records in a category
        let mut category_map: MutexGuard<HashMap<String, Vec<StringRecord>>> =
            category_records.lock().unwrap();
        category_map.entry(category).or_default().push(record);
    });

    // Write each category to file sequentially
    let category_map: HashMap<String, Vec<StringRecord>> = Arc::try_unwrap(category_records)
        .unwrap()
        .into_inner()
        .unwrap();
    category_map
        .into_par_iter()
        .try_for_each(|(category, records)| {
            write_category_files(&category, &records, delimiter, context.clone())
        })?;
    Ok(())
}

fn write_category_files(
    category: &str,
    records: &[StringRecord],
    delimiter: u8,
    context: RecordProcessingContext,
) -> Result<(), std::io::Error> {
    let file_path: String = if context.create_directory {
        let category_dir: String = format!("{}/{}", context.output_dir.display(), category);
        // Create a directory for the category if it not exists
        if !Path::new(&category_dir).exists() {
            fs::create_dir_all(&category_dir)?;
        }
        format!("{}/{}.csv", category_dir, context.file_name)
    } else {
        format!("{}/{}.csv", context.output_dir.display(), category)
    };

    let file_exists: bool = Path::new(&file_path).exists();
    let file: File = OpenOptions::new()
        .create(true)
        .append(true)
        .open(&file_path)?;
    let mut writer: BufWriter<File> = BufWriter::new(file);

    // Write headers if the file does not exist
    if !file_exists {
        write_headers(&mut writer, context.headers, delimiter)?;
    }
    // Write records to the file
    for record in records {
        writeln!(
            writer,
            "{}",
            record
                .iter()
                .map(|field| field.to_string())
                .collect::<Vec<_>>()
                .join(&(delimiter as char).to_string())
        )?;
    }
    Ok(())
}

fn write_headers<W: Write>(
    writer: &mut W,
    headers: &[&str],
    delimiter: u8,
) -> Result<(), std::io::Error> {
    writeln!(writer, "{}", headers.join(&(delimiter as char).to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;

    struct TestContext {
        files: Vec<PathBuf>,
    }

    impl TestContext {
        fn new() -> Self {
            TestContext { files: Vec::new() }
        }

        fn add_file(&mut self, file_path: PathBuf) {
            self.files.push(file_path);
        }
    }

    impl Drop for TestContext {
        fn drop(&mut self) {
            for file in &self.files {
                if file.exists() {
                    fs::remove_file(file).unwrap();
                }
            }
        }
    }

    #[test]
    fn test_write_headers() {
        let headers = vec!["doc", "id", "cost"];
        let delimiter = b'|';
        let mut buffer = Cursor::new(Vec::new());

        write_headers(&mut buffer, &headers, delimiter).unwrap();

        let written_data = String::from_utf8(buffer.into_inner()).unwrap();
        assert_eq!(written_data, "doc|id|cost\n");
    }

    #[test]
    fn test_split_file_by_category() {
        let mut context = TestContext::new();

        let input_file = PathBuf::from("./assets/city.csv");
        let output_dir = PathBuf::from("./assets/tmp");
        let delimiter = b',';
        let input_column = "State";

        context.add_file(output_dir.join("AK.csv"));
        context.add_file(output_dir.join("AL.csv"));

        split_file_by_category(&input_file, &input_column, &output_dir, false, delimiter).unwrap();
        let ak_file_path = format!("{}/AK.csv", output_dir.display());
        let al_file_path = format!("{}/AL.csv", output_dir.display());

        let ak_data = fs::read_to_string(ak_file_path).unwrap();
        let al_data = fs::read_to_string(al_file_path).unwrap();

        assert!(ak_data.contains("City,State,Population,Latitude,Longitude"));
        assert!(ak_data.contains("Davidson Landing,AK,,65.241944,-165.2716667"));
        assert!(ak_data.contains("Kenai,AK,7610,60.5544444,-151.2583333"));

        assert!(al_data.contains("City,State,Population,Latitude,Longitude"));
        assert!(al_data.contains("Oakman,AL,,33.7133333,-87.38861111"));
    }
}
