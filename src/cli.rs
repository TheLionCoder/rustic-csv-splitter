use crate::context::Delimiter;
use clap::Parser;

#[derive(Parser, Debug)]
#[command(author, version, about, long_about=None)]
pub struct AppConfig {
    #[arg(required = true, num_args = 1..)]
    paths: Vec<std::path::PathBuf>,
    #[arg(short, long, value_enum, default_value_t = Delimiter::Comma)]
    delimiter: Delimiter,
    #[arg(short = 'c', long = "column")]
    input_column: String,
    #[arg(short = 'o', long = "dir")]
    output_dir: std::path::PathBuf,
    #[arg(short = 's', long, default_value_t = 100_000)]
    chunk_size: usize,
    #[arg(short = 'r', long = "creat-dir", action)]
    create_directory: bool,
}
