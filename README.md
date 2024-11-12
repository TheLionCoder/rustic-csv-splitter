# CSV Splitter

This project is designed to split a large dataset into multiple smaller files
based on a specified column value, for default the output CSV files use "|" delimiter.

[!NOTE]: Missing values will be filled with the "unknown" literal

## Instalation

Clone the repository:

```sh
git clone git@github.com:TheLionCoder/rustic-csv-splitter.git
cd rustic-csv-splitter
```

### Usage

To run the program, use the following command:

_Get Help_:

```sh
cargo run release -- --help
```

### Arguments

- `-p, --path <path> Path to the CSV file to split`
- `-d, --delimiter <delimiter> Delimiter used in the CSV file [default: ,]`
- `-c, --column <input-column> Column to split the CSV file by`
- `-o, --dir <output-dir> Output directory to save the split files`
- `-r, --create-dir Save the split files in a directory with the name of the column value`

### Example

To split a csv file, without create directories for each column value

```sh
cargo run --release -- -p assets/city.csv -c "State" -o assets/tmp/
```
