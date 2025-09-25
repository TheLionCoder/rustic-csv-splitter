pub mod data_filtering;
pub mod data_loading;

pub mod process_file {

    use crate::cli;
    use anyhow::Result;

    const MIN_BUFFER_SIZE: u64 = 16 * 1024 * 1024;
    const MAX_BUFFER_SIZE: u64 = 128 * 1024 * 1024;
    const FILE_SIZE_BUFFER_RATIO: u64 = 10;
    const MIN_ADAPTIVE_CHUNK: u64 = 1_000;
    const FILE_SIZE_CHUNK_RATIO: u64 = 1_000;

    #[tracing::instrument(skip_all, fields(path = %path.display()))]
    fn process_file(path: &std::path::Path, config: &cli::AppConfig) -> Result<()> {
        todo!()
    }

    /// Calculates an adaptive buffer size for reading the CSV file
    /// based on its size.
    fn calculate_buffer_size(file_size: u64) -> usize {
        (file_size / FILE_SIZE_BUFFER_RATIO)
            .max(MIN_BUFFER_SIZE)
            .min(MAX_BUFFER_SIZE) as usize
    }

    /// Calculates an adaptive chunk size for processinf records
    /// in batches.
    fn calculate_adaptive_chunk_size(file_size: u64, base_chunk_size: u64) -> usize {
        (file_size / FILE_SIZE_CHUNK_RATIO)
            .max(MIN_ADAPTIVE_CHUNK)
            .min(base_chunk_size) as usize
    }
}
