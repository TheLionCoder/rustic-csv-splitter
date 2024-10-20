use polars::prelude::*;

use std::path::Path;
use polars::prelude::CsvEncoding;
use polars::prelude::LazyCsvReader;

pub(crate) fn load_data(path: &Path, encoding: CsvEncoding) -> PolarsResult<LazyFrame> {
    let q: LazyFrame = LazyCsvReader::new(path)
        .with_infer_schema_length(Some(0))
        .with_encoding(encoding)
        .finish()?;
    Ok(q)
}

#[cfg(test)]
mod tests {
    use std::path::PathBuf;
    use super::*;

    #[test]
    fn test_load_data() {
        let path: PathBuf = PathBuf::from("./assets/city.csv");
        let encoding: CsvEncoding = CsvEncoding::Utf8;

        let result: PolarsResult<LazyFrame> = load_data(&path, encoding);
        assert!(result.is_ok(), "Failed to load data");

        let q: LazyFrame = result.unwrap();
        assert!(!q.collect().unwrap().is_empty(), "Dataframe is empty");
    }
}