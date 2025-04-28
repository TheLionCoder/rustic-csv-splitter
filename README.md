# :crab: CSV Splitter

This project is designed to split a large dataset into multiple smaller files
based on a specified column value, for default the output CSV files use "|" delimiter.

> [!NOTE]
> Missing values in the input-column will be filled with the "unknown" literal.

> [!WARNING]
> The input-column should be the same for all the files.

---

## :package: Installation

Clone the repository:

```sh
git clone git@github.com:TheLionCoder/rustic-csv-splitter.git
cd rustic-csv-splitter
```

---

## :hammer_and_wrench: Usage

To run the program, use the followings commands:

```sh
cargo build --release
```

_then_:

```sh
cargo target/release/rustic-csv-splitter
```

## Arguments

- `-p, --path <path> Path to the CSV file to split`
- `-d, --delimiter <delimiter> Delimiter used in the CSV file [default: ,]`
- `-c, --column <input-column> Column to split the CSV file by`
- `-o, --dir <output-dir> Output directory to save the split files`
- `-r, --create-dir Save the split files in a directory with the name`
- `-s, --chunk-size <chunk-size>  Number of records to read from the column value
CSV file before writing to a new file [default: 100000]`

## Example 1

To split a couple of CSV files, without create directories for each column value

```sh
target/release/rustic-csv-splitter \n
-p assets/worldcitiespop.csv assets/city.csv assets/new_city.csv \n
-c State\n
-o assets/tmp
```

_The result will be like:_

```sh
assets/tmp
├── city_AK.csv
├── city_AL.csv
├── city_CA.csv
├── city_NY.csv
├── new_city_AK.csv
├── new_city_AL.csv
├── new_city_CA.csv
└── new_city_NY.csv

1 directory, 8 files
```

## Example 2

To split a couple of CSV files, creating directories for each column value

```sh
target/release/rustic-csv-splitter \n
-p assets/worldcitiespop.csv assets/city.csv assets/new_city.csv \n
-c State \n
-r
-o assets/tmp
```

The result will be like:

```sh
assets/tmp
├── AK
│   ├── city.csv
│   └── new_city.csv
├── AL
│   ├── city.csv
│   └── new_city.csv
├── CA
│   ├── city.csv
│   └── new_city.csv
└── NY
    ├── city.csv
    └── new_city.csv

5 directories, 8 files
```
