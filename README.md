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
cargo run --release -- --help
```

## Arguments

```sh
Arguments:
  <PATHS>...

Options:
  -r, --reader-delimiter <READER_DELIMITER>  [default: ,]\
    [possible values: ,, |, "\t", ;]
  -i, --column <INPUT_COLUMN>
  -w, --writer-delimiter <WRITER_DELIMITER>  [default: |]\
    [possible values: ,, |, "\t", ;]
  -o, --dir <OUTPUT_DIR>
  -s, --chunk-size <CHUNK_SIZE>              [default: 100000]
  -c, --create-dir
  -h, --help                                 Print help
  -V, --version                              Print version

```

## Example 1

To split a couple of CSV files, without create directories for each column value

```sh
target/release/rustic-csv-splitter \n
assets/worldcitiespop.csv assets/city.csv assets/new_city.csv \n
-i State\n
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
-c
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
