use clap::{Arg, ArgMatches};

use crate::delimiter::Delimiter;

pub(crate) fn parse_cli() -> ArgMatches {
    clap::Command::new("Csv Splitter")
        .version("0.1.0")
        .author("TheLionCoder")
        .about("Split a CSV file into multiple files based on a column values")
        .arg(
            Arg::new("path")
                .short('p')
                .long("path")
                .required(true)
                .help("Path to the CSV file to split"),
        )
        .arg(
            Arg::new("delimiter")
                .short('d')
                .long("delimiter")
                .default_value(",")
                .value_parser(|v: &str| v.parse::<Delimiter>())
                .help("Delimiter used in the CSV file"),
        )
        .arg(
            Arg::new("input-column")
                .short('c')
                .long("column")
                .required(true)
                .help("Column to split the CSV file by"),
        )
        .arg(
            Arg::new("output-dir")
                .short('o')
                .long("dir")
                .required(true)
                .help("Output directory to save the split files"),
        )
        .arg(
            Arg::new("create-dir")
                .short('r')
                .long("create-dir")
                .action(clap::ArgAction::SetTrue)
                .help("Save the split files in a directory with the name of the column value"),
        )
        .get_matches()
}

#[cfg(test)]
mod tests {
    use super::*;
    use clap::Command;

    #[test]
    fn test_parse_cli_command() {
        let matches = Command::new("test")
            .arg(Arg::new("delimiter").short('d').long("delimiter")
                .value_parser(|v: &str| v.parse::<Delimiter>()))
            .try_get_matches_from(vec!["test", "-d", ";"])
            .unwrap();

        assert_eq!(
            *matches
                .get_one::<Delimiter>("delimiter").unwrap(), Delimiter::SemiColon
        )
    }
}
