#[derive(Debug, thiserror::Error)]
pub enum WriterError {
    #[error("IO/Error: {0}")]
    Io(#[from] std::io::Error),
    #[error("CSV processing error: {0}")]
    Csv(#[from] csv::Error),
}
