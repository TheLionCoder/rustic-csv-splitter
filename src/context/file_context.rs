#[derive(Clone)]
pub struct FileContext {
    pub output_dir: std::path::PathBuf,
    pub create_directory: bool,
    pub file_name: String,
    pub output_delimiter: u8,
    pub chunk_size: usize,
}
