use crate::app_error;
use crate::context::RecordProcessingContext;
use anyhow::Result;
use csv::{Reader, StringRecord, Writer, WriterBuilder};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufWriter, ErrorKind};
use std::path::PathBuf;
use std::string::String;

use rayon::prelude::*;

type WriteGuard<'a> = dashmap::mapref::one::RefMut<'a, String, Writer<BufWriter<File>>>;

/// Prepares the headers for the output files and identifies the column indexes to keep.
///
/// This function takes the complete set of headers from the original CSV file and removes
/// the column that is being used to split the data. It returns both the new, filtered
/// header record and a vector containing the original indexes of the columns that were retained.
/// This index vector is crucial for efficiently selecting the correct data fields from each record later on.
///
/// # Arguments
///
/// * `original_headers` - A `&StringRecord` containing all headers from the input file.
/// * `split_column_idx` - The zero-based index of the column to remove (the column used for categorization).
///
/// # Returns
///
/// A tuple containing:
/// * `StringRecord`: The new set of headers, excluding the split column.
/// * `Vec<usize>`: A vector of the original column indexes that are being kept.
///
/// # Example
///
/// ```rust
/// use csv::StringRecord;
/// // Assume prepare_header_info is available in the current scope
///
/// let original = StringRecord::from(vec!["id", "city", "state", "zip"]);
/// let split_idx = 2; // We are splitting by "state"
///
/// let (new_headers, kept_indexes) = prepare_header_info(&original, split_idx);
///
/// assert_eq!(new_headers, StringRecord::from(vec!["id", "city", "zip"]));
/// assert_eq!(kept_indexes, vec![0, 1, 3]);
pub fn prepare_header_info(
    original_headers: &StringRecord,
    split_column_idx: usize,
) -> (StringRecord, Vec<usize>) {
    let mut headers_to_keep = Vec::with_capacity(original_headers.len() - 1);
    let mut indexes_to_keep = Vec::with_capacity(original_headers.len() - 1);

    for (idx, header) in original_headers.iter().enumerate() {
        if idx != split_column_idx {
            headers_to_keep.push(header);
            indexes_to_keep.push(idx);
        }
    }
    (StringRecord::from_iter(headers_to_keep), indexes_to_keep)
}

/// Reads records from a CSV reader and processes them in chunks.
///
/// # Arguments
///
/// * `reader` - A mutable reference to a `csv::Reader<File>` to read records from.
/// * `context` - A reference to a `RecordProcessingContext` containing configuration
///   (like `chunk_size`) and potentially other details needed for processing.
///
/// # Errors
///
/// Returns an `io::Error` if:
/// - An error occurs while reading from the CSV (`csv::Error` is mapped to `io::Error`).
/// - An error occurs during chunk processing via `process_chunk`.
///
/// # Example
///
/// ```rust
/// use csv::Reader;
/// use std::fs::File;
/// use std::io;
/// let path = std::path::Path::new("./foo/bar.csv");
/// let reader = ReaderBuilder::new()
///    .buffer_capacity(16 * 1024 * 1024)
///    .has_headers(true)
///    .delimiter(delimiter.clone().into())
///    .from_path(path)?;
///let context = RecordProcessingContext {
///         file_context: FileContext {
///             chunk_size: 1000 // Process 1000 records at a time
///         },
///data_filtering::write_records_to_csv(&mut reader, &context).unwrap()
/// ```
pub fn write_records_to_csv(
    reader: &mut Reader<File>,
    context: &RecordProcessingContext,
) -> Result<()> {
    let mut chunk: Vec<_> = Vec::with_capacity(context.file_context.chunk_size);

    for result in reader.records() {
        let record: StringRecord = result?;
        chunk.push(record);

        if chunk.len() >= context.file_context.chunk_size {
            process_chunk(&chunk, context)?;
            chunk.clear()
        }
    }
    if !chunk.is_empty() {
        process_chunk(&chunk, context)?;
    }

    Ok(())
}

/// Processes a chunk of `StringRecord` data. This function filters the input records
/// based on a provided context and writes the filtered results to storage.
///
/// # Arguments
///
/// * `chunk` - A slice of `StringRecord` objects representing the chunk of data to be processed.
/// * `context` - A reference to a `RecordProcessingContext` object, which provides
///   the configuration and context needed for filtering and writing operations.
///
/// # Returns
///
/// * `Ok(())` - If all records are successfully processed and written.
/// * `Err(io::Error)` - If there is an error during the filtering or writing process.
///
///
/// # Examples
///
/// ```rust
/// let chunk: Vec<StringRecord> = vec![/* some records */];
/// let context = RecordProcessingContext::new(/* configuration */);
/// process_chunk(&chunk, &context).unwrap();
/// ```
fn process_chunk(chunk: &[StringRecord], context: &RecordProcessingContext) -> Result<()> {
    let categorized_data: HashMap<String, Vec<StringRecord>> =
        filter_records_by_category(chunk, context);
    write_categorized_records(categorized_data, context)?;
    Ok(())
}

/// Filters and categorizes records in a chunk based on specific processing context.
///
/// The function takes a slice of `StringRecord` and processes it in parallel using Rayon.
/// Each record is categorized into different buckets based on a derived category,
/// and certain fields of the records are selected and stored as filtered records.
///
/// # Parameters
///
/// - `chunk`: A slice of `StringRecord`. Each `StringRecord` represents a row of data.
/// - `context`: A reference to a `RecordProcessingContext` that provides additional
///   processing metadata, like header indexes for filtering fields.
///
/// # Returns
///
/// - `HashMap<String, Vec<StringRecord>>`: A mapping where the key is the category
///   (as derived by the `get_category` function) and the value is a vector of filtered
///   `StringRecord` objects associated with that category.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
/// use csv::StringRecord;
///
/// let chunk: Vec<StringRecord> = vec![/* Your records here */];
/// let context: RecordProcessingContext = /* Your context setup here */;
///
/// let filtered_result = filter_records(&chunk, &context);
///
/// for (category, records) in filtered_result {
///     println!("Category: {}", category);
///     for record in records {
///         println!("{:?}", record);
///     }
/// }
/// ```
fn filter_records_by_category(
    chunk: &[StringRecord],
    context: &RecordProcessingContext,
) -> HashMap<String, Vec<StringRecord>> {
    chunk
        .par_iter()
        .fold_with(
            // Initial accumulator for each thread,
            HashMap::new(),
            |mut acc: HashMap<String, Vec<StringRecord>>, record| {
                let category: String = get_category(record, context);
                // Create the filtered record by selecting the fields based on the header indexes
                // StringRecord::from_iter()
                // clones the &str fields into owned Strings
                let filtered_records: StringRecord =
                    StringRecord::from_iter(context.header_indexes.iter().map(|&idx| &record[idx]));
                acc.entry(category).or_default().push(filtered_records);
                acc
            },
        )
        .reduce(HashMap::new, |mut main_map, thread_map| {
            // Combine the results from threads efficiently
            for (key, mut records) in thread_map {
                // Append records for the same category
                main_map.entry(key).or_default().append(&mut records);
            }
            main_map
        })
}

/// Writes categorized records to their respective destinations using shared writers.
///
/// # Arguments
///
/// * `categorized_records` - A `HashMap` where keys are category names (`String`) and
///   values are `Vec<StringRecord>` containing the records belonging to that category.
///   This function takes ownership of the map and its contents.
/// * `context` - A reference to `RecordProcessingContext` which must contain `category_writers`:
///   a `Mutex`-protected `HashMap` storing the actual `Writer` instances for each category.
///   This allows multiple threads to potentially call `write_records` safely.
///
/// # Errors
///
/// Returns `io::Error` if:
/// * The `category_writers` mutex is poisoned (i.e., another thread panicked while holding the lock).
/// * `get_or_create_writer` fails (e.g., cannot create a new file, invalid path, permissions error).
/// * `writer.flush()` fails (e.g., disk full, I/O error).
///
/// # Panics
///
/// **This function will panic** if `writer.write_record(&record)` returns an error.
/// This typically happens due to I/O issues (e.g., disk full, broken pipe, permission denied).
/// Using `.unwrap()` here assumes writes will never fail, which is unsafe for I/O operations.
/// Consider handling the `Result` returned by `write_record` explicitly for robust error handling.
/// It's like assuming the mail bag *cannot* catch fire during loading – better be prepared! categorized records to their corresponding CSV files.
fn write_categorized_records(
    categorized_records: HashMap<String, Vec<StringRecord>>,
    context: &RecordProcessingContext,
) -> Result<()> {
    categorized_records
        .into_par_iter()
        .try_for_each(|(category, records)| -> Result<()> {
            let mut writer_guard = get_or_create_writer(&category, context)?;

            for record in records {
                writer_guard.write_record(&record)?;
            }
            writer_guard.flush()?;
            Ok(())
        })
}

/// This function manages the creation or retrieval of a CSV writer for a specific category,
/// ensuring proper file handling with buffered writing. It uses a context map (`writers_map`)
/// to store and reuse writers, avoiding redundant file operations for the same category.
///
/// # Arguments
///
/// * `category` - A string slice representing the category of the file. This string is used
///   to differentiate between different output files.
/// * `context` - A reference to the `RecordProcessingContext` which provides information
///   required for file handling, such as headers and file delimiter settings.
/// * `writers_map` - A mutable reference to a `HashMap` which maps category names to their
///   respective `Writer<BufWriter<File>>` instances used for writing CSV records.
///
/// # Returns
///
/// Returns a mutable reference to the corresponding `Writer<BufWriter<File>>` for the given
/// category. The writer is used to write CSV data to the respective category's file.
///
/// # Errors
///
/// * Returns an `io::Error` if there are issues during file creation, opening, or writing
///   operations.
/// * Propagates errors from the helper function `create_category_path` if the file path
///   resolution fails.
///
/// # Example
///
/// ```rust
/// use std::collections::HashMap;
/// use std::fs::File;
/// use std::io::{self, BufWriter};
///
/// fn example_usage() -> Result<(), io::Error> {
///     let mut writers_map: HashMap<String, Writer<BufWriter<File>>> = HashMap::new();
///     let context: RecordProcessingContext = RecordProcessingContext::new();
///     let category = "category1";
///
///     let writer = get_or_create_writer(category, &context, &mut writers_map)?;
///     
///     Ok(())
/// }
/// ```
///
fn get_or_create_writer<'a>(
    category: &str,
    context: &'a RecordProcessingContext,
) -> Result<WriteGuard<'a>, app_error::WriterError> {
    let entry = context.category_writers.entry(category.to_string());
    entry.or_try_insert_with(|| {
        let file_path: PathBuf = create_category_path(category, context)?;
        let file_exists: bool = file_path.exists();

        let file: File = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&file_path)?;

        // Use buffered writer
        let buf_writer: BufWriter<File> = BufWriter::new(file);
        let mut csv_writer: Writer<BufWriter<File>> = WriterBuilder::new()
            .delimiter(context.file_context.output_delimiter.into())
            .from_writer(buf_writer);

        // Write headers only if the file is newly created
        if !file_exists {
            csv_writer.write_record(&context.file_headers)?;
        }
        Ok(csv_writer)
    })
}

#[inline]
fn get_category(record: &StringRecord, context: &RecordProcessingContext) -> String {
    record
        .get(context.split_column_idx)
        .map_or_else(|| String::from("unknown"), String::from)
}

/// Creates a file path for a category, ensuring the directory exists if requested.
/// Returns and error of invalid category names
fn create_category_path(
    category: &str,
    context: &RecordProcessingContext,
) -> Result<PathBuf, io::Error> {
    if category.is_empty()
        || category.contains(['/', '\\', ':', '*', '?', '"', '<', '>', '|'])
        || category == "."
        || category == ".."
    {
        return Err(io::Error::new(
            ErrorKind::InvalidInput,
            format!("Invalid category name: '{}'", category),
        ));
    }

    let file_path: PathBuf = if context.file_context.create_directory {
        let dir_path: PathBuf = context.file_context.output_dir.join(category);
        fs::create_dir_all(&dir_path)?;
        // Construct the final file path within the category directory
        dir_path.join(format!("{}.csv", context.file_context.file_name))
    } else {
        // Construct the file path directly in the output directory
        context.file_context.output_dir.join(format!(
            "{}_{}.csv",
            context.file_context.file_name, category
        ))
    };
    Ok(file_path)
}
