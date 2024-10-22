use std::collections::{HashMap};
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use csv::{Reader, StringRecord};

//TODO: implement mkdir
pub(crate) fn split_csv_by_category(reader: &mut Reader<File>, headers: &StringRecord,
                                    split_column: &str, output_dir: &str) -> Result<(), csv::Error> {
    let mut file_buffer: HashMap<String, BufWriter<File>> = HashMap::new();
    let headers_vec: Vec<&str> = headers.iter().collect();
    let split_column_idx: usize = headers.iter().position(|h| h == split_column).unwrap();

    for result in reader.records() {
        let record: StringRecord = result?;
        let category = match record.get(split_column_idx) {
            Some(value) if !value.is_empty() => value,
            _  => "unknown"
        };

        let buffer = file_buffer.entry(category.to_string()).or_insert_with(|| {
            let file_path: String = format!("{}/{}.csv", output_dir, category);
            let file: File = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path).unwrap();
            let mut writer: BufWriter<File> = BufWriter::new(file);
            writeln!(writer, "{}", headers_vec.join(",")).unwrap();
            writer
        });
        writeln!(buffer, "{}", record.iter()
            .map(|field| field.to_string())
            .collect::<Vec<_>>()
            .join(","))?;
    }
    Ok(())
}


fn collect_headers(reader: &mut Reader<File>) -> StringRecord{
    let headers= reader.headers().unwrap().clone();
    headers
}