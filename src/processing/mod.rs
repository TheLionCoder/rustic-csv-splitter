pub mod data_filtering;
pub mod data_loading;

pub mod process_file {

    use crate::context::{FileContext, RecordProcessingContext};
    use crate::processing::data_filtering;
    use crate::{cli, processing::data_loading};
    use anyhow::{Context, Result};

    const MIN_BUFFER_SIZE: u64 = 16 * 1024 * 1024;
    const MAX_BUFFER_SIZE: u64 = 128 * 1024 * 1024;
    const FILE_SIZE_BUFFER_RATIO: u64 = 10;
    const MIN_ADAPTIVE_CHUNK: usize = 1_000;
    const FILE_SIZE_CHUNK_RATIO: u64 = 1_000;

    #[tracing::instrument(skip_all, fields(path = %path.display()))]
    pub fn process_file(path: &std::path::Path, config: &cli::AppConfig) -> Result<()> {
        tracing::info!("Starting processing");

        let file_size = path
            .metadata()
            .with_context(|| format!("Failed to read metadata for file: {}", path.display()))?
            .len();
        let buffer_size = calculate_buffer_size(file_size);
        let adaptive_chunk_size = calculate_adaptive_chunk_size(file_size, config.chunk_size);
        tracing::info!(
            buffer_size,
            adaptive_chunk_size,
            "Calculated adaptize sizes."
        );

        let mut reader = data_loading::read_file(path, &config.reader_delimiter, buffer_size)?;
        let file_name = data_loading::extract_file_name(path)?;

        let file_context = FileContext {
            output_dir: config.output_dir.clone(),
            create_directory: config.create_directory,
            file_name,
            output_delimiter: config.writer_delimiter,
            chunk_size: adaptive_chunk_size,
        };

        let headers = reader.headers()?.clone();
        let split_column_idx = headers
            .iter()
            .position(|h| h == config.input_column)
            .with_context(|| {
                format!(
                    "Column '{}' not found in {}",
                    config.input_column,
                    path.display()
                )
            })?;

        let (file_headers, header_indexes) =
            data_filtering::prepare_header_info(&headers, split_column_idx);

        let context = RecordProcessingContext::new(
            file_headers,
            split_column_idx,
            header_indexes,
            file_context,
        );

        tracing::info!("Processing records and writing to destination files.");
        data_filtering::write_records_to_csv(&mut reader, &context)?;

        tracing::info!("Finished processing.");
        Ok(())
    }

    /// Calculates an adaptive buffer size for reading the CSV file
    /// based on its size.
    fn calculate_buffer_size(file_size: u64) -> usize {
        (file_size / FILE_SIZE_BUFFER_RATIO).clamp(MIN_BUFFER_SIZE, MAX_BUFFER_SIZE) as usize
    }

    /// Calculates an adaptive chunk size for processinf records
    /// in batches.
    fn calculate_adaptive_chunk_size(file_size: u64, base_chunk_size: usize) -> usize {
        ((file_size / FILE_SIZE_CHUNK_RATIO) as usize).clamp(MIN_ADAPTIVE_CHUNK, base_chunk_size)
    }
}
