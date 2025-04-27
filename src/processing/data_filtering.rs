use crate::context::RecordProcessingContext;
use csv::{Reader, StringRecord, StringRecordsIter, Writer, WriterBuilder};
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Error};
use std::path::PathBuf;
use std::string::String;
use std::sync::MutexGuard;

use rayon::prelude::*;

/// Writes records from a CSV reader to a processing pipeline in chunks,
/// improving memory usage for large datasets.
pub fn write_records_to_csv(
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

/// Processes a chunk of `StringRecord` data by filtering and writing the records
/// based on the provided processing context.
///
/// # Arguments
/// * `chunk` - A reference to a vector of `StringRecord` that represents the input data
///   to be processed in the current chunk.
/// * `context` - A reference to a `RecordProcessingContext` that contains configuration
///   and state information necessary for processing the records.
///
/// # Returns
/// * `Ok(())` on successful processing of the chunk, which includes filtering and writing
///   the records.
/// * `Err(Error)` if an error occurs during processing, such as during
///   filtering or writing operations.
///
/// # Behavior
/// 1. Filters the input chunk of `StringRecord` using the `filter_records` function,
///    which returns a `HashMap` where the keys are strings and the values are vectors
///    of `StringRecord` that meet certain criteria.
/// 2. Write the filtered records to an output using the `write_records` function.
/// 3. Returns an `Err` if any step fails, or `Ok(())` if processing completes successfully.
///
/// # Dependencies
/// This function relies on the following:
/// * `filter_records` - A function that takes a chunk of records and a context and
///   produces a filtered map of records.
/// * `write_records` - A function that writes the filtered records to an output
///   based on the context.
///
/// # Errors
/// This function propagates any errors returned by `write_records` and may raise
/// errors caused by issues in filtering or writing records.
///
/// # Example
/// ```
/// let chunk = vec![/* some StringRecord data */];
/// let context = RecordProcessingContext::new(/* some context configuration */);
/// match process_chunk(&chunk, &context) {
///     Ok(()) => println!("Chunk processed successfully."),
///     Err(e) => eprintln!("Error processing chunk: {}", e),
/// }
/// ```
fn process_chunk(
    chunk: &Vec<StringRecord>,
    context: &RecordProcessingContext,
) -> Result<(), Error> {
    let writers: HashMap<String, Vec<StringRecord>> = filter_records(chunk, context);
    write_records(writers, context)?;
    Ok(())
}

/// Filters and groups record from a chunk based on their category.
///
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


/// Writes records to corresponding file outputs, categorized by a given `writer` map.
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

/// Retrieves the category value from a given `StringRecord` based on the provided processing context.
#[inline]
fn get_category(record: &StringRecord, context: &RecordProcessingContext) -> String {
    match record.get(context.split_column_idx) {
        Some(category) => category.to_string(),
        _ => String::from("unknown"),
    }
}

/// Extracts headers from a given `StringRecord`, excluding the field in a specified column.
pub fn get_headers(current_headers: &StringRecord, split_column_id: usize) -> StringRecord {
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

/// Retrieves the indexes of specified headers within a set of file headers.
pub fn get_header_indexes(headers: &StringRecord, file_headers: &StringRecord) -> Vec<usize> {
    file_headers
        .iter()
        .filter_map(|header| headers.iter().position(|h| h == header))
        .collect()
}

/// Creates a file path for a category, ensuring proper directory structure and input validation.
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
    use std::sync::LazyLock;

    static FILE_HEADERS: LazyLock<StringRecord> =
        LazyLock::new(|| StringRecord::from(vec!["city", "state"]));
    static HEADERS: LazyLock<StringRecord> =
        LazyLock::new(|| StringRecord::from(vec!["city", "state", "year"]));

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
