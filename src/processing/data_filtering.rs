use crate::context::RecordProcessingContext;
use csv::{Reader, StringRecord, Writer, WriterBuilder};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind};
use std::path::PathBuf;
use std::string::String;
use std::sync::MutexGuard;

use rayon::prelude::*;

/// Writes records from a CSV reader to a processing pipeline in chunks,
/// improving memory usage for large datasets.
pub fn write_records_to_csv(
    reader: &mut Reader<File>,
    context: &RecordProcessingContext,
) -> Result<(), io::Error> {

    let mut chunk: Vec<_> = Vec::with_capacity(*context.chunk_size);

    for result in reader.records() {
        let record: StringRecord = result?;
        chunk.push(record);

        if chunk.len() >= *context.chunk_size {
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
fn process_chunk(
    chunk: &[StringRecord],
    context: &RecordProcessingContext,
) -> Result<(), io::Error> {
    let filtered_data: HashMap<String, Vec<StringRecord>> = filter_records(chunk, context);
    write_records(filtered_data, context)?;
    Ok(())
}

/// Filters and groups record from a chunk based on their category.
fn filter_records(
    chunk: &[StringRecord],
    context: &RecordProcessingContext,
) -> HashMap<String, Vec<StringRecord>> {
    chunk
        .par_iter()
        .fold_with(
            // Initial accumulator for each thread,
            HashMap::new() ,
            |mut acc: HashMap<String, Vec<StringRecord>>, record| {
                let category: String = get_category(record, context);
                // Create the filtered record by selecting the fields based on the header indexes
                // StringRecord::from_iter()
                // clones the &str fields into owned Strings
                let filtered_records: StringRecord = StringRecord::from_iter(
                    context
                    .header_indexes
                    .iter()
                    .filter_map(|&idx| record.get(idx))
                );
                acc.entry(category).or_default().push(filtered_records);
                acc
            },
        )
        .reduce(HashMap::new, |mut acc, map| {
            // Combine the results from threads efficiently
            for (key, mut value) in map {
                // Append records for the same category
                acc.entry(key).or_default().append(&mut value);
            }
            acc
        })
}

// Helper function to get or create a CSV writer for a given category.
// Manages file creation, header writing, and storing the writer in the context map.
fn get_or_create_writer<'a>(
    category: &'a str,
    context: &RecordProcessingContext,
    writers_map: &'a mut HashMap<String, Writer<BufWriter<File>>>
) -> Result<&'a mut Writer<BufWriter<File>>, io::Error> {
    if !writers_map.contains_key(category) {
        let file_path: PathBuf = create_category_path(category, context)?;
        let file_exists: bool = file_path.exists();

        let file: File = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        // Use buffered writer
        let buf_writer: BufWriter<File> = BufWriter::new(file);
        let mut csv_writer: Writer<BufWriter<File>> = WriterBuilder::new()
            .delimiter(context.delimiter)
            .from_writer(buf_writer);

        // Write headers only if the file is newly created
        if !file_exists {
            csv_writer.write_record(&context.headers)?;
        }

        // Insert the newly created writer into the map
        writers_map.insert(String::from(category), csv_writer);
    }
        // Return mutable reference to the writer.
        Ok(writers_map.get_mut(category).unwrap())
}

// Writes categorized records to their corresponding CSV files.
fn write_records(
    categorized_records: HashMap<String, Vec<StringRecord>>,
    context: &RecordProcessingContext,
) -> Result<(), io::Error> {
    let mut writers_guard: MutexGuard<HashMap<String, Writer<BufWriter<File>>>> =
        context.writers.lock().map_err(|_| {
            io::Error::new(ErrorKind::Other, "Writer mutex was poisoned")
        })?;

    for (category, records) in categorized_records {
        let writer = get_or_create_writer(&category, context, &mut writers_guard)?;

       records.into_iter().for_each(|record| {
           writer.write_record(&record).unwrap();
       }
       );
        writer.flush()?;
    }
    Ok(())
}

/// Retrieves the category value from a given `StringRecord` based on the provided processing context.
#[inline]
fn get_category(record: &StringRecord, context: &RecordProcessingContext) -> String {
    record
        .get(context.split_column_idx)
        .map_or_else(|| String::from("unknown"), String::from)
}

// Extracts specific headers from a 'StringRecord', excluding one column.
pub fn get_headers(current_headers: &StringRecord, split_column_id: usize) -> StringRecord {
    let headers: Vec<String> = current_headers
        .iter()
        .enumerate()
        // Filter out the split column index
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

/// Finds the original indexes of selected headers within the full file headers
pub fn get_header_indexes(headers_to_keep: &StringRecord, all_file_headers: &StringRecord) -> Vec<usize> {
    all_file_headers
        .iter()
        .enumerate()
        // find the index for each header we want to keep
        .filter_map(|(idx, file_header)| {
            if headers_to_keep.iter().any(
                |h| h == file_header
            ) {
                Some(idx)
            } else {
                None
            }
        })
        .collect()
}

/// Creates a file path for a category, ensuring the directory exists if requested.
/// Returns and error of invalid category names
fn create_category_path(
    category: &str,
    context: &RecordProcessingContext,
) -> Result<PathBuf, io::Error> {
    if category.is_empty() || category.contains([
        '/', '\\', ':', '*', '?', '"', '<', '>', '|'
    ]) || category == "." || category == ".." {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("Invalid category name: '{}'", category),
        ));
    }

    let file_path: PathBuf = if context.create_directory {
        let dir_path: PathBuf = context.output_dir.join(category);
        fs::create_dir_all(&dir_path)?;
        // Construct the final file path within the category directory
        dir_path.join(format!("{}.csv", context.file_name))
    } else {
        // Construct the file path directly in the output directory
        context.output_dir.join(format!("{}_{}.csv", context.file_name, category))
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
