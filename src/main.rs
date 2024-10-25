use std::path::Path;

use crate::delimiter::Delimiter;
use clap::ArgMatches;

mod cli_parsing;
mod data_filtering;
mod data_loading;
mod delimiter;

fn main() {
    let matches: ArgMatches = cli_parsing::parse_cli();
    let path: &str = matches.get_one::<String>("path").unwrap();
    let delimiter: &Delimiter = matches.get_one::<Delimiter>("delimiter").unwrap();
    let input_column: &str = matches.get_one::<String>("input-column").unwrap();
    let output_dir_str: &str = matches.get_one::<String>("output-dir").unwrap();
    let create_dir: bool = matches.get_flag("create-dir");

    let path: &Path = Path::new(path);
    let output_dir: &Path = Path::new(output_dir_str);

    data_filtering::split_file_by_category(path, input_column, output_dir, create_dir, delimiter)
        .unwrap()
}
