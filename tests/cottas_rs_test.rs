use cottas_rs::*;
use polars::prelude::*;

#[test]
fn test_rdf2cottas() {
    let source_file = "tests/data/example.ttl";
    let target_file = "tests/data/example.cottas";
    let index = "spo";

    rdf2cottas(source_file, target_file, index).unwrap();

    // Check that target file exists
    assert!(std::path::Path::new(target_file).exists());

    let df = ParquetReader::new(target_file).finish().unwrap();
    assert!(df.height() > 0, "The file .cottas is empty");
    println!("{:?}", df.head(Some(5)));
}

