use std::path::Path;
use std::str::FromStr;
use clap::ArgMatches;

mod cli_parsing;
mod data_filtering;
mod data_loading;


enum Delimiter {
    Comma,
    Pipe,
    Tab,
}

impl FromStr for Delimiter {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "," => Ok(Delimiter::Comma),
            "|" => Ok(Delimiter::Pipe),
            "\t" => Ok(Delimiter::Tab),
            _ => Err(())
        }
    }
}

impl From<Delimiter> for u8 {
    fn from(val: Delimiter) -> Self {
        match val {
            Delimiter::Comma => b',',
            Delimiter::Pipe => b'|',
            Delimiter::Tab => b'\t'
        }
    }
}


fn main() {
    let matches: ArgMatches = cli_parsing::parse_cli();
    let path_str: &str = matches.get_one::<String>("path").unwrap();
    let delimiter_str: &str = matches.get_one::<String>("delimiter").unwrap();
    let input_column: &str = matches.get_one::<String>("input-column").unwrap();
    let output_dir_str: &str = matches.get_one::<String>("output-dir").unwrap();
    let create_dir: bool = matches.get_flag("create-dir");

    let path: &Path = Path::new(path_str);
    let output_dir: &Path = Path::new(output_dir_str);
    let delimiter: Delimiter = Delimiter::from_str(delimiter_str).unwrap();
    let delimiter_byte: u8 = delimiter.into();

    data_filtering::split_file_by_category(path, input_column, output_dir, create_dir,
    delimiter_byte).unwrap()
}
