use anyhow::{bail, Result} ;
use clap::Parser;
use rayon::iter::{IntoParallelRefIterator, ParallelIterator};

mod app_error;
mod cli;
mod context;
mod processing;

fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let span = tracing::span!(tracing::Level::INFO, "Splitting file...");
    let _guard = span.enter();

    let config = cli::AppConfig::parse();

    if config.paths.is_empty() {
        bail!("No input paths provided.");
    }
    tracing::info!(
        input_files = config.paths.len(),
        output_dir = %config.output_dir.display(),
        "Configuration loaded, started parallel file processing."
    );

    config
    .paths
    .par_iter()
    .try_for_each(|path| processing::process_file::process_file(path, &config))?;

    tracing::info!("All files processed successfully.");
    Ok(())
}
