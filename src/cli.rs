use crate::context::Delimiter;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about=None)]
pub struct AppConfig {
    #[arg(required = true, num_args = 1..)]
    pub paths: Vec<std::path::PathBuf>,
    #[arg(short, long, value_enum, default_value_t = Delimiter::Comma)]
    pub reader_delimiter: Delimiter,
    #[arg(short, long = "column")]
    pub input_column: String,
    #[arg(short, long, default_value_t = Delimiter::Pipe)]
    pub writer_delimiter: Delimiter, 
    #[arg(short = 'o', long = "dir")]
    pub output_dir: std::path::PathBuf,
    #[arg(short = 's', long, default_value_t = 100_000)]
    pub chunk_size: usize,
    #[arg(short, long = "create-dir", action)]
    pub create_directory: bool,
}
