#[derive(Clone)]
pub struct FileContext<'a> {
    pub output_dir: &'a std::path::PathBuf,
    pub create_directory: bool,
    pub file_name: String,
    pub output_delimiter: u8,
    pub chunk_size: &'a usize,
}
