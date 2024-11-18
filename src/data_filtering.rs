use crate::data_loading::{extract_file_name, read_file};
use crate::record_context::RecordProcessingContext;
use crate::delimiter::Delimiter;
use csv::{Reader, StringRecord, Writer, WriterBuilder};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter};
use std::path::{Path, PathBuf};
use std::string::String;
use std::sync::{Arc, Mutex, MutexGuard};
use std::sync::atomic::{AtomicBool, Ordering};
use clap::Error;

/// Split a CSV file by a category in a column
pub(crate) fn split_file_by_category(
    path: &Path,
    input_column: &str,
    output_dir: PathBuf,
    create_directory: bool,
    delimiter: &Delimiter,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut reader: Reader<File> = read_file(path, delimiter)?;

    let file_name: String = extract_file_name(path)?;
    let headers: StringRecord = reader.headers()?.clone();

    let category_writers: Arc<Mutex<HashMap<String, Writer<BufWriter<File>>>>> = Arc::new(Mutex::new(HashMap::new()));
    // Get the index of the column to split by
    let split_column_idx: usize = headers.iter().position(|h| h == input_column).unwrap();
    let file_headers: StringRecord = get_headers(&headers, split_column_idx);

    let context: Arc<RecordProcessingContext> = Arc::new(RecordProcessingContext {
        headers: file_headers,
        output_dir,
        create_directory,
        file_name,
        delimiter: Delimiter::PIPE,
        split_column_idx,
        writers: category_writers.clone()
    });

    write_records_to_csv(&mut reader, &context)?;
    let mut writers: MutexGuard<HashMap<String, Writer<BufWriter<File>>>> = category_writers.lock().unwrap();
    for writer in writers.values_mut() {
        writer.flush().unwrap();
    }

    Ok(())

}


/// Write records to CSV file
fn write_records_to_csv(
    reader: &mut Reader<File>,
    context: &RecordProcessingContext
) -> Result<(), Error> {
    let records: Vec<StringRecord> = filter_columns(reader, &context).unwrap();
    records.par_chunks(10_000).for_each(|chunk| {
        let headers_written: Arc<AtomicBool> = Arc::new(AtomicBool::new(false));

        for record in chunk {
            let category = get_category(&record, &context);
            let mut writers: MutexGuard<HashMap<String, Writer<BufWriter<File>>>> = context.writers.lock().unwrap();
            let writer: &mut Writer<BufWriter<File>> = writers.entry(category.clone()).or_insert_with(|| {
                let file_path: PathBuf = create_category_path(&category, &context).unwrap();
                let file: File = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&file_path)
                    .unwrap();
                let buf_writer: BufWriter<File>= BufWriter::new(file);
                WriterBuilder::new()
                    .delimiter(context.delimiter)
                    .from_writer(buf_writer)
            });
            if !headers_written.load(Ordering::SeqCst) {
                writer.write_record(&context.headers).unwrap();
                headers_written.store(true, Ordering::SeqCst);
            }
            writer.write_record(&*record).unwrap();
            writer.flush().unwrap();
        }
    });
    Ok(())
}

/// Get the category value from a record
#[inline]
fn get_category(record: &StringRecord, context: &RecordProcessingContext) -> String {
    record
        .get(context.split_column_idx)
        .unwrap_or("unknown")
        .to_string()
}

/// Get headers
fn get_headers(current_headers: &StringRecord, split_column_id: usize) -> StringRecord {
    let headers: Vec<String> = current_headers
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if idx != split_column_id {
                Some(field.to_string())
            } else {
                None
            }
        }).collect();
    StringRecord::from(headers)
}

/// Filter the columns of a CSV file
fn filter_columns(
    reader: &mut Reader<File>,
    context: &RecordProcessingContext,
) ->  Result<Vec<StringRecord>, csv::Error>{
    reader
        .records()
        .map(|result| {
            let record: StringRecord = result?;
            let filtered_record: StringRecord = record
                .iter()
                .enumerate()
                .filter_map(|(idx, field)| {
                    if context.headers.iter().any(|header| header == record.get(idx).unwrap_or(""))  {
                        Some(field.to_string())
                    } else {
                        None
                    }
                })
                .collect();
            Ok(filtered_record)
        }).collect()
}


/// Create a path for a category
fn create_category_path(
    category: &str,
    context: &RecordProcessingContext,
) -> Result<PathBuf, std::io::Error> {
    if category.contains("..") || category.contains('/') || category.contains("\\") {
        panic!("Invalid category name: {}", category);
    }
    let file_path: PathBuf = if context.create_directory {
        let dir: PathBuf = context.output_dir.join(category);
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
        }
        dir.join(format!("{}.csv", context.file_name))
    } else {
        context.output_dir.join(format!("{}.csv", category))
    };
    Ok(file_path)
}

#[cfg(test)]
mod tests {
    use super::*;
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
    fn test_split_file_by_category() {
        let mut context = TestContext::new();

        let input_file = PathBuf::from("assets/city.csv");
        let output_dir = PathBuf::from("assets/tmp");
        let delimiter = Delimiter::Comma;
        let input_column = "State";

        if !input_file.exists() {
            panic!("Input file doesn't exist: {}", input_file.display());
        }

        // context.add_file(output_dir.join("AK.csv"));
        // context.add_file(output_dir.join("AL.csv"));
        // context.add_file(output_dir.join("NY.csv"));
        // context.add_file(output_dir.join("CA.csv"));

        split_file_by_category(
            &input_file,
            &input_column,
            output_dir.clone(),
            false,
            &delimiter,
        )
        .unwrap();
        let ak_file_path = format!("{}/AK.csv", output_dir.display());
        let al_file_path = format!("{}/AL.csv", output_dir.display());

        let ak_data = fs::read_to_string(ak_file_path).unwrap();
        let al_data = fs::read_to_string(al_file_path).unwrap();

        assert!(ak_data.contains("City|Population|Latitude|Longitude"));
        assert!(ak_data.contains("Davidson Landing||65.241944|-165.2716667"));
        assert!(ak_data.contains("Kenai|7610|60.5544444|-151.2583333"));

        assert!(al_data.contains("City|Population|Latitude|Longitude"));
        assert!(al_data.contains("Oakman||33.7133333|-87.38861111"));
    }

    #[test]
    fn test_get_category() {
        let context = &RecordProcessingContext {
            split_column_idx: 1,
            ..Default::default()
        };

        let record = StringRecord::from(vec!["1", "Bogota", "sur"]);
        let category = get_category(&record, context);

        assert_eq!(category, "Bogota");
    }

    #[test]
    fn test_get_headers() {
        let headers = StringRecord::from(vec!["city", "state", "year"]);
        let file_headers = StringRecord::from(vec!["city", "state"]);
        let split_column_idx = 2_usize;
        let headers = get_headers(&headers, split_column_idx);

        assert_eq!(file_headers, headers);

    }
}
