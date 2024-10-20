use polars::prelude::*;


fn has_null(input_column: &Series) -> bool {
    input_column.null_count() > 0
}



fn extract_unique_categories(query: LazyFrame, input_column: &str) -> Vec<String> {
    let q: DataFrame = query
        .clone()
        .select([col(input_column)])
        .unique(None, Default::default())
        .collect().unwrap();

    let series: &Series = q.column(input_column).unwrap();
    todo!()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_dataframe() -> PolarsResult<DataFrame> {
        let df: PolarsResult<DataFrame> = df!(
            "state" => &["CA", "NY", "TX", "TX", "CA"],
            "city" => &["San Francisco", "New York", "Austin", "Dallas", "Los Angeles"],
            "population" => &[Some(1000_0000), Some(800_000), Some(1_0000), None, None]
        );
        df
    }

    #[test]
    fn test_has_null() {
        let series: Series = Series::new("state".into(),
                                         &[Some("CA"), Some("NY"), None, Some("TX")]);
        assert!(has_null(&series), "Failed to detect null values");
    }

    #[test]
    fn test_extract_unique_categories() {
        let df:  LazyFrame = make_dataframe().unwrap().lazy();
        let categories: Vec<String> = extract_unique_categories(df, "state");
        assert_eq!(categories, vec!["CA", "TX", "NY"])
    }
}