use crate::data_loading::{extract_file_name, read_file};
use crate::delimiter::Delimiter;
use csv::{Reader, StringRecord};
use rayon::prelude::*;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::string::String;
use std::sync::Arc;

#[derive(Clone)]
struct RecordProcessingContext {
    headers: Vec<String>,
    output_dir: PathBuf,
    create_directory: bool,
    file_name: String,
    delimiter: u8,
    split_column_idx: usize,
}

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

    let headers_vec: Vec<String> = headers.iter().map(|s| s.to_string()).collect();
    // Get the index of the column to split by
    let split_column_idx: usize = headers.iter().position(|h| h == input_column).unwrap();

    let context: Arc<RecordProcessingContext> = Arc::new(RecordProcessingContext {
        headers: headers_vec,
        output_dir,
        create_directory,
        file_name,
        delimiter: Delimiter::PIPE,
        split_column_idx,
    });

    rayon::scope(|s| {
        let mut records_chunk: Vec<StringRecord> = Vec::with_capacity(10_000);
        for result in reader.records() {
            let record: StringRecord = result.unwrap();
            records_chunk.push(record);

            if records_chunk.len() == 10_000 {
                let records_chunk_clone = records_chunk.clone();
                let context_clone: Arc<RecordProcessingContext> = Arc::clone(&context);
                // process chunks in parallel using Rayon
                s.spawn(move |_| {
                    process_records_in_parallel(&records_chunk_clone, context_clone).unwrap();
                });
                // Reuse the vector without reallocating memory
                records_chunk.clear();
            }
        }

        if !records_chunk.is_empty() {
            let context_clone: Arc<RecordProcessingContext> = Arc::clone(&context);
            s.spawn(move |_| {
                process_records_in_parallel(&records_chunk, context_clone).unwrap();
            });
        }
    });
    Ok(())
}

/// Process records in parallel
fn process_records_in_parallel(
    records: &Vec<StringRecord>,
    context: Arc<RecordProcessingContext>,
) -> Result<(), Box<dyn std::error::Error>> {
    records.par_chunks(10_000).for_each(|chunk| {
        // Each chunk is processed in parallel
        process_chunk(chunk, &context).unwrap();
    });

    Ok(())
}

/// Process each chunk and write to the appropriate file
fn process_chunk(
    chunk: &[StringRecord],
    context: &RecordProcessingContext,
) -> Result<(), std::io::Error> {
    let mut writers: HashMap<String, BufWriter<File>> = HashMap::with_capacity(chunk.len());

    for record in chunk {
        let category = record
            .get(context.split_column_idx)
            .unwrap_or("unknown")
            .to_string();

        let writer: &mut BufWriter<File> = get_writer(&mut writers, &category, context)?;
        write_record(writer, record, context)?
    }

    // Flush all writers to ensure all data is written to disk
    for writer in writers.values_mut() {
        if let Err(e) = writer.flush() {
            eprintln!("Failed to flush writer: {}", e);
            return Err(e);
        }
    }
    Ok(())
}

/// Get or create a writer for a category
fn get_writer<'a>(
    writers: &'a mut HashMap<String, BufWriter<File>>,
    category: &str,
    context: &RecordProcessingContext,
) -> Result<&'a mut BufWriter<File>, std::io::Error> {
    if !writers.contains_key(category) {
        let file_path: PathBuf = create_category_path(category, context)?;
        let file_exists: bool = Path::new(&file_path).exists();
        let file: File = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;
        let mut writer: BufWriter<File> = BufWriter::new(file);
        let filtered_headers: Vec<String> = context
            .headers
            .iter()
            .enumerate()
            .filter_map(|(idx, field)| {
                if idx != context.split_column_idx {
                    Some(field.to_string())
                } else {
                    None
                }
            })
            .collect::<Vec<_>>();

        // Write headers if the file does not exist
        if !file_exists {
            writeln!(
                writer,
                "{}",
                filtered_headers.join(&(context.delimiter as char).to_string())
            )?;
        }
        writers.insert(category.to_string(), writer);
    }
    Ok(writers.get_mut(category).unwrap())
}

/// Write a single record in the file
fn write_record<W: Write>(
    writer: &mut W,
    record: &StringRecord,
    context: &RecordProcessingContext,
) -> Result<(), std::io::Error> {
    let filtered_record: Vec<String> = record
        .iter()
        .enumerate()
        .filter_map(|(idx, field)| {
            if idx != context.split_column_idx {
                Some(field.to_string())
            } else {
                None
            }
        })
        .collect::<Vec<_>>();

    writeln!(
        writer,
        "{}",
        filtered_record.join(&(context.delimiter as char).to_string())
    )?;
    Ok(())
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
}
