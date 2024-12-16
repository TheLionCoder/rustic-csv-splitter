use clap::ArgMatches;
use csv::{Reader, StringRecord, Writer};
use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, MutexGuard};
use tracing::{event, span, Level, Span};

use crate::data_loading::{extract_file_name, read_file};
use crate::delimiter::Delimiter;
use crate::record_context::RecordProcessingContext;

mod cli_parsing;
mod data_filtering;
mod data_loading;
mod delimiter;
mod record_context;

fn main() {
    tracing_subscriber::fmt::init();
    let span: Span = span!(Level::INFO, "Splitting file...");
    let _guard = span.enter();

    let matches: ArgMatches = cli_parsing::parse_cli();
    let path: &str = matches.get_one::<String>("path").unwrap();
    let delimiter: &Delimiter = matches.get_one::<Delimiter>("delimiter").unwrap();
    let input_column: &str = matches.get_one::<String>("input-column").unwrap();
    let output_dir_str: &str = matches.get_one::<String>("output-dir").unwrap();
    let create_dir: bool = matches.get_flag("create-dir");

    let path: &Path = Path::new(path);
    let output_dir: PathBuf = PathBuf::from(output_dir_str);

    event!(Level::INFO, "Reading file: {:?}", path);
    let mut reader: Reader<File> = read_file(path, delimiter).unwrap();

    let file_name: String = extract_file_name(path).unwrap();
    let headers: StringRecord = reader.headers().unwrap().clone();

    let category_writers: Arc<Mutex<HashMap<String, Writer<BufWriter<File>>>>> =
        Arc::new(Mutex::new(HashMap::new()));
    // Get the index of the column to split by

    let split_column_idx: usize = headers.iter().position(|h| h == input_column).unwrap();
    let file_headers: StringRecord = data_filtering::get_headers(&headers, split_column_idx);
    let header_indexes: Vec<usize> = data_filtering::get_header_indexes(&headers, &file_headers);

    let context: Arc<RecordProcessingContext> = Arc::new(RecordProcessingContext {
        headers: file_headers,
        output_dir,
        create_directory: create_dir,
        file_name,
        delimiter: Delimiter::PIPE,
        split_column_idx,
        writers: category_writers.clone(),
        header_indexes,
    });

    event!(Level::INFO, "Writing records to CSV...");
    data_filtering::write_records_to_csv(&mut reader, &context).unwrap();
    let mut writers: MutexGuard<HashMap<String, Writer<BufWriter<File>>>> =
        category_writers.lock().unwrap();
    for writer in writers.values_mut() {
        writer.flush().unwrap();
    }
    event!(Level::INFO, "Finished writing records to CSV");
}
