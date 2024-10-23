use csv::StringRecord;
use std::collections::HashMap;
use std::fs;
use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::Path;

use crate::data_loading::{extract_file_name, read_file};

#[allow(dead_code)]
struct RecordProcessingContext<'a> {
    headers: &'a [&'a str],
    file_buffer: &'a mut HashMap<String, BufWriter<File>>,
    output_dir: &'a Path,
    create_directory: bool,
    file_name: &'a str,
}

#[allow(dead_code)]
pub(crate) fn split_file_by_category(
    path: &Path,
    input_column: &str,
    output_dir: &Path,
    create_directory: bool,
    delimiter: u8,
) -> Result<(), Box<dyn std::error::Error>> {
    let file_name: &str = extract_file_name(path)?;
    let mut file_buffer: HashMap<String, BufWriter<File>> = HashMap::new();

    let mut reader = read_file(path, delimiter)?;
    let headers: StringRecord = reader.headers()?.clone();
    let headers_vec: Vec<&str> = headers.iter().collect();
    let split_column_idx: usize = headers.iter().position(|h| h == input_column).unwrap();

    let mut context: RecordProcessingContext = RecordProcessingContext {
        headers: &headers_vec,
        file_buffer: &mut file_buffer,
        output_dir,
        create_directory,
        file_name,
    };

    for result in reader.records() {
        let record: StringRecord = result?;
        process_record(&record, delimiter, split_column_idx, &mut context)?;
    }
    Ok(())
}

fn process_record(
    record: &StringRecord,
    delimiter: u8,
    split_column_idx: usize,
    context: &mut RecordProcessingContext,
) -> Result<(), std::io::Error> {
    let category = record.get(split_column_idx).unwrap_or("unknown");
    let buffer = get_or_create_writer(context, delimiter, category)?;

    writeln!(
        buffer,
        "{}",
        record
            .iter()
            .map(|field| field.to_string())
            .collect::<Vec<_>>()
            .join(&(delimiter as char).to_string())
    )?;
    Ok(())
}

fn get_or_create_writer<'a>(
    context: &'a mut RecordProcessingContext,
    delimiter: u8,
    category: &str,
) -> Result<&'a mut BufWriter<File>, std::io::Error> {
    let file_writer: &mut BufWriter<File> = context
        .file_buffer
        .entry(category.to_string())
        .or_insert_with(|| {
            let file_path: String = if context.create_directory {
                let category_dir: String = format!("{}/{}", context.output_dir.display(), category);
                if !Path::new(&category_dir).exists() {
                    fs::create_dir_all(&category_dir).unwrap();
                }
                format!("{}/{}.csv", category_dir, context.file_name)
            } else {
                format!("{}/{}.csv", context.output_dir.display(), category)
            };

            let file_exists: bool = Path::new(&file_path).exists();
            let file: File = OpenOptions::new()
                .create(true)
                .append(true)
                .open(&file_path)
                .unwrap();
            let mut writer: BufWriter<File> = BufWriter::new(file);

            if !file_exists {
                write_headers(&mut writer, context.headers, delimiter).unwrap();
            }
            writer
        });
    Ok(file_writer)
}

fn write_headers<W: Write>(
    writer: &mut W,
    headers: &[&str],
    delimiter: u8,
) -> Result<(), std::io::Error> {
    writeln!(writer, "{}", headers.join(&(delimiter as char).to_string()))?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;
    use std::path::PathBuf;

    #[test]
    fn test_write_headers() {
        let headers = vec!["doc", "id", "cost"];
        let delimiter = b'|';
        let mut buffer = Cursor::new(Vec::new());

        write_headers(&mut buffer, &headers, delimiter).unwrap();

        let written_data = String::from_utf8(buffer.into_inner()).unwrap();
        assert_eq!(written_data, "doc|id|cost\n");
    }

    #[test]
    fn test_get_or_create_writer() {
        let headers = vec!["City", "State", "Population", "Latitude"];
        let mut file_buffer = HashMap::new();
        let output_dir = PathBuf::from("./assets/tmp");
        let file_name = "city";
        let mut context = RecordProcessingContext {
            headers: &headers,
            file_buffer: &mut file_buffer,
            output_dir: &output_dir,
            create_directory: false,
            file_name,
        };

        let delimiter = b',';
        let category = "State";
        {
            let writer = get_or_create_writer(&mut context, delimiter, &category).unwrap();

            writeln!(writer, "AK").unwrap();
            writer.flush().unwrap();
        }

        // check if the writer is correctly created and added
        assert!(context.file_buffer.contains_key(category));
        assert_eq!(context.file_buffer.len(), 1);

        // verify file content
        let file_path = format!("{}/{}.csv", output_dir.display(), category);
        let written_data = fs::read_to_string(file_path).unwrap();

        assert!(written_data.contains("City,State,Population,Latitude"));
        assert!(written_data.contains("AK"));
    }

    #[test]
    fn test_process_record() {
        let headers = vec!["City", "State", "Population"];
        let mut file_buffer = HashMap::new();
        let output_dir = PathBuf::from("./assets/");

        if !output_dir.exists() {
            fs::create_dir_all(&output_dir).unwrap();
        }

        let file_name = "city";
        let mut context = RecordProcessingContext {
            headers: &headers,
            file_buffer: &mut file_buffer,
            output_dir: &output_dir,
            create_directory: false,
            file_name,
        };

        let category = "NY";
        let delimiter = b'|';
        let split_column_idx = 1;
        let record = StringRecord::from(vec!["New York", "NY", "833681"]);

        process_record(&record, delimiter, split_column_idx, &mut context).unwrap();

        // check if the writer is correctly created and added
        assert!(context.file_buffer.contains_key(category));
        assert_eq!(context.file_buffer.len(), 1);

        // verify file content
        let file_path = format!("{}/{}.csv", output_dir.display(), category);
        let written_data = fs::read_to_string(file_path).unwrap();

        assert!(written_data.contains("City|State|Population"));
        assert!(written_data.contains("New York|NY|833681"));
    }

    #[test]
    fn test_split_file_by_category() {
        let input_file = PathBuf::from("./assets/city.csv");
        let output_dir = PathBuf::from("./assets/tmp");
        let delimiter = b',';
        let input_column = "State";

        split_file_by_category(&input_file, &input_column, &output_dir, false, delimiter).unwrap();
        let ak_file_path = format!("{}/AK.csv", output_dir.display());
        let al_file_path = format!("{}/AL.csv", output_dir.display());

        let ak_data = fs::read_to_string(ak_file_path).unwrap();
        let al_data = fs::read_to_string(al_file_path).unwrap();

        assert!(ak_data.contains("City,State,Population,Latitude,Longitude"));
        assert!(ak_data.contains("Davidson Landing,AK,,65.241944,-165.2716667"));
        assert!(ak_data.contains("Kenai,AK,7610,60.5544444,-151.2583333"));

        assert!(al_data.contains("City,State,Population,Latitude,Longitude"));
        assert!(al_data.contains("Oakman,AL,,33.7133333,-87.38861111"));

    }
}
