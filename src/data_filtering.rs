use crate::record_context::RecordProcessingContext;
use csv::{Reader, StringRecord, StringRecordsIter, Writer, WriterBuilder};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Error};
use std::path::PathBuf;
use std::string::String;
use std::sync::MutexGuard;

use rayon::prelude::*;

/// Write records to CSV file
pub(crate) fn write_records_to_csv(
    reader: &mut Reader<File>,
    context: &RecordProcessingContext,
) -> Result<(), Error> {
    let chunk_size: usize = 100_000;

    let record_iter: StringRecordsIter<File> = reader.records();
    let mut chunk: Vec<_> = Vec::with_capacity(chunk_size);

    for result in record_iter {
        let record: StringRecord = result?;
        chunk.push(record);

        if chunk.len() == chunk_size {
            process_chunk(&chunk, context)?;
            chunk.clear()
        }
    }
    if !chunk.is_empty() {
        process_chunk(&chunk, context)?;
    }

    Ok(())
}

/// Process records in parallel
fn process_chunk(
    chunk: &Vec<StringRecord>,
    context: &RecordProcessingContext,
) -> Result<(), Error> {
    let writers: HashMap<String, Vec<StringRecord>> = filter_records(chunk, context);
    write_records(writers, context)?;
    Ok(())
}

/// Filter records by category
fn filter_records(
    chunk: &Vec<StringRecord>,
    context: &RecordProcessingContext,
) -> HashMap<String, Vec<StringRecord>> {
    chunk
        .par_iter()
        .fold_with(
            HashMap::new(),
            |mut acc: HashMap<String, Vec<StringRecord>>, record| {
                let category: String = get_category(record, context);
                let filtered_records: StringRecord = context
                    .header_indexes
                    .iter()
                    .filter_map(|&idx| record.get(idx).map(|field| field.to_string()))
                    .collect();
                acc.entry(category).or_default().push(filtered_records);
                acc
            },
        )
        .reduce(HashMap::new, |mut acc, map| {
            for (key, mut value) in map {
                acc.entry(key).or_default().append(&mut value);
            }
            acc
        })
}

/// Write records to CSV file
fn write_records(
    writers: HashMap<String, Vec<StringRecord>>,
    context: &RecordProcessingContext,
) -> Result<(), Error> {
    let mut context_writers: MutexGuard<HashMap<String, Writer<BufWriter<File>>>> =
        context.writers.lock().unwrap();
    for (category, records) in writers {
        let writer: &mut Writer<BufWriter<File>> =
            context_writers.entry(category.clone()).or_insert_with(|| {
                let file_path: PathBuf = create_category_path(&category, context).unwrap();
                let file_exists: bool = file_path.exists();
                let file: File = OpenOptions::new()
                    .create(true)
                    .append(true)
                    .open(&file_path)
                    .unwrap();

                let buf_writer: BufWriter<File> = BufWriter::new(file);
                let mut csv_writer: Writer<BufWriter<File>> = WriterBuilder::new()
                    .delimiter(context.delimiter)
                    .from_writer(buf_writer);

                if !file_exists {
                    csv_writer.write_record(&context.headers).unwrap();
                }

                csv_writer
            });

        for record in records {
            writer.write_record(&record)?;
        }
        writer.flush()?;
    }
    Ok(())
}

/// Get the category value from a record
#[inline]
fn get_category(record: &StringRecord, context: &RecordProcessingContext) -> String {
    match record.get(context.split_column_idx) {
        Some(category) => category.to_string(),
        _ => String::from("unknown"),
    }
}

/// Get headers
pub(crate) fn get_headers(current_headers: &StringRecord, split_column_id: usize) -> StringRecord {
    let headers: Vec<String> = current_headers
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if idx != split_column_id {
                Some(field.to_string())
            } else {
                None
            }
        })
        .collect();
    StringRecord::from(headers)
}

/// Get the header indexes
pub(crate) fn get_header_indexes(
    headers: &StringRecord,
    file_headers: &StringRecord,
) -> Vec<usize> {
    file_headers
        .iter()
        .filter_map(|header| headers.iter().position(|h| h == header))
        .collect()
}

/// Create a path for a category
fn create_category_path(
    category: &str,
    context: &RecordProcessingContext,
) -> Result<PathBuf, Error> {
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
    use lazy_static::lazy_static;
    use std::path::PathBuf;

    lazy_static! {
        static ref FILE_HEADERS: StringRecord = StringRecord::from(vec!["city", "state"]);
        static ref HEADERS: StringRecord = StringRecord::from(vec!["city", "state", "year"]);
    }

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

        context.add_file(output_dir.join("AK.csv"));
        context.add_file(output_dir.join("AL.csv"));
        context.add_file(output_dir.join("NY.csv"));
        context.add_file(output_dir.join("CA.csv"));

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
        let headers = HEADERS.clone();
        let file_headers = FILE_HEADERS.clone();
        let split_column_idx = 2_usize;
        let headers = get_headers(&headers, split_column_idx);

        assert_eq!(file_headers, headers);
    }

    #[test]
    fn test_get_header_indexes() {
        let headers = HEADERS.clone();
        let file_headers = FILE_HEADERS.clone();
        let indexes = get_header_indexes(&headers, &file_headers);
        assert_eq!(indexes, vec![0, 1]);
    }
}
