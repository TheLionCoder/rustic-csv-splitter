use crate::context::Delimiter;
use anyhow::{anyhow, Result};
use csv::{Reader, ReaderBuilder};
use std::fs::File;
use std::path::Path;
use std::string::String;

/// Reads a file at the specified path and returns a CSV reader with the configured settings.
///
/// # Parameters
/// - `path`: A reference to a `Path` that specifies the file to be read.
/// - `delimiter`: A reference to a `Delimiter` that determines the character used as the delimiter
///   in the CSV file.
///
/// # Returns
/// A `Result` containing:
/// - `Ok(Reader<File>)`: A CSV reader instance configured to read the file at the given path.
/// - `Err(csv::Error)`: An error that occurred while attempting to open or read the file.
///
/// # Notes
/// - The reader is configured to handle files with headers by default (`has_headers(true)`).
/// - The buffer size for reading the file is set to 16 MB (`buffer_capacity(16 * 1024 * 1024)`).
/// - The delimiter character is derived from the provided `Delimiter` and converted before usage.
///
/// # Errors
/// This function returns an error if:
/// - The specified file path does not exist or cannot be accessed.
/// - The file reading operation encounters any issues specific to `csv::Reader`.
///
/// # Example
/// ```rust
/// use std::path::Path;
/// use csv::{Reader, Error};
///
/// let path = Path::new("example.csv");
/// let delimiter = Delimiter::from(b',');
///
/// match read_file(&path, &delimiter) {
///     Ok(reader) => {
///         // Successfully created the CSV reader, proceed with reading records.
///     }
///     Err(e) => {
///         // Handle the error (e.g., file not found, invalid CSV format, etc.).
///         eprintln!("Error reading file: {}", e);
///     }
/// }
/// ```
pub fn read_file(
    path: &Path,
    delimiter: &Delimiter,
    buffer_size: usize,
) -> Result<Reader<File>, csv::Error> {
    let reader: Reader<File> = ReaderBuilder::new()
        .buffer_capacity(buffer_size)
        .has_headers(true)
        .delimiter(delimiter.clone().into())
        .from_path(path)?;

    Ok(reader)
}

/// Extracts the file name (without extension) from a given file path.
///
/// # Arguments
///
/// * `path` - A reference to a `Path` that represents the file path from which the file name will be extracted.
///
/// # Returns
///
/// * `Ok(String)` - A string representing the file name (without the extension) if the operation succeeds.
/// * `Err(Box<dyn std::error::Error>)` - An error if the file name cannot be extracted.
///
/// # Errors
///
/// This function will return an error if:
/// * The `file_stem` cannot be retrieved (e.g., the `path` does not point to a file with a valid name).
/// * The `file_stem` or any intermediate value cannot be converted to a UTF-8 `&str`.
///
/// # Panics
///
/// This function may panic if:
/// * The `path.file_stem()` method returns `None`, which is then unwrapped.
/// * The conversion of the file stem to a string slice fails and the `unwrap()` is called.
///
/// # Examples
///
/// ```
/// use std::path::Path;
/// use your_crate::extract_file_name;
///
/// let path = Path::new("/some/directory/file_name.txt");
/// match extract_file_name(&path) {
///     Ok(file_name) => println!("File name: {}", file_name),
///     Err(e) => eprintln!("Error extracting file name: {}", e),
/// }
/// ```
///
/// # Note
///
/// Proper error handling should be implemented to avoid potential panics caused by any invalid input
/// or unexpected conditions.
pub fn extract_file_name(path: &Path) -> Result<String> {
    let file_stem = path
        .file_stem()
        .ok_or_else(|| anyhow!("Invalid file path: no file stem"))?
        .to_str()
        .ok_or_else(|| anyhow!("Invalid file name: not valid UTF-8"))?;

    Ok(file_stem.to_string())
}
