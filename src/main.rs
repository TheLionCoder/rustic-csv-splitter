use anyhow::{anyhow, bail, Result};
use clap::parser::ValuesRef;
use clap::ArgMatches;
use csv::{Reader, StringRecord, Writer};
use dashmap::DashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::Arc;
use tracing::{event, span, Level, Span};

use crate::context::record_context::RecordProcessingContext;
use crate::context::Delimiter;
use crate::context::FileContext;
use crate::processing::data_filtering;
use crate::processing::data_loading::{extract_file_name, read_file};

mod cli_parsing;
mod context;
mod processing;

/// Processes a single CSV file by splitting it based on the specified column.
///
/// # Arguments
///
/// * `path` - The path to the CSV file to process.
/// * `delimiter` - The delimiter used in the CSV file.
/// * `input_column` - The name of the column to split by.
/// * `output_dir` - The directory to save the split files.
/// * `chunk_size` - The base chunk size for processing records.
/// * `create_directory` - Whether to create subdirectories for each category.
///
/// # Errors
///
/// Returns an `anyhow::Error` if file processing fails.
fn process_file(
    path: &PathBuf,
    delimiter: &Delimiter,
    input_column: &str,
    output_dir: &PathBuf,
    chunk_size: usize,
    create_directory: bool,
) -> Result<(), anyhow::Error> {
    event!(Level::INFO, "Reading file: {:?}", path);
    let file_size = path.metadata()?.len();
    let buffer_size = ((file_size / 10) as usize).max(16 * 1024 * 1024).min(128 * 1024 * 1024);
    let adaptive_chunk_size = ((file_size / 1000) as usize).max(1000).min(chunk_size);
    let mut reader: Reader<File> = read_file(path, delimiter, buffer_size)?;
    let file_name: String = extract_file_name(path)?;
    let file_context: FileContext = FileContext {
        output_dir: output_dir.clone(),
        create_directory,
        file_name,
        output_delimiter: Delimiter::PIPE,
        chunk_size: adaptive_chunk_size,
    };

    let headers: StringRecord = reader.headers()?.clone();

    let category_writers: Arc<DashMap<String, Writer<BufWriter<File>>>> =
        Arc::new(DashMap::new());

    let split_column_idx: usize = headers
        .iter()
        .position(|h| h == input_column)
        .ok_or_else(|| anyhow!("Column '{}' not found in file headers", input_column))?;
    let file_headers: StringRecord = data_filtering::get_headers(&headers, split_column_idx);
    let header_indexes: Vec<usize> =
        data_filtering::get_header_indexes(&headers, &file_headers);

    let context: Arc<RecordProcessingContext> = Arc::new(RecordProcessingContext {
        file_headers,
        split_column_idx,
        category_writers: category_writers.clone(),
        header_indexes,
        file_context,
    });

    event!(Level::INFO, "Writing records to CSV...");
    data_filtering::write_records_to_csv(&mut reader, &context)?;
    event!(Level::INFO, "Finished writing records to CSV...\n");
    Ok(())
}

fn main() -> Result<(), anyhow::Error> {
    tracing_subscriber::fmt::init();
    let span: Span = span!(Level::INFO, "Splitting file...");
    let _guard = span.enter();

    let matches: ArgMatches = cli_parsing::parse_cli();
    let paths: ValuesRef<PathBuf> = matches
        .get_many::<PathBuf>("path")
        .ok_or_else(|| anyhow!("No valid paths provided"))?;
    if paths.len() == 0 {
        bail!("No paths provided");
    }
    let delimiter: &Delimiter = matches
        .get_one::<Delimiter>("delimiter")
        .ok_or_else(|| anyhow!("Delimiter not provided"))?;
    let input_column: &str = matches
        .get_one::<String>("input-column")
        .ok_or_else(|| anyhow!("Input column not provided"))?;
    let output_dir: &PathBuf = matches
        .get_one::<PathBuf>("output-dir")
        .ok_or_else(|| anyhow!("Output directory not provided"))?;
    let chunk_size: usize = *matches
        .get_one::<usize>("chunk-size")
        .ok_or_else(|| anyhow!("Chunk size not provided"))?;
    let create_directory: bool = matches.get_flag("create-dir");

    for path in paths {
        process_file(path, delimiter, input_column, output_dir, chunk_size, create_directory)?;
    }
    Ok(())
}
