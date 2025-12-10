use cottas_rs::*;

#[test]
fn test_rdf2cottas() {
    let source_file = "tests/data/example.ttl";
    let target_file = "tests/data/example.cottas";
    let index = "spo";

    rdf2cottas(source_file, target_file, index).unwrap();

    // Check that target file exists
    assert!(std::path::Path::new(target_file).exists());

    let df = ParquetReader::from_path(target_file)?
        .finish()?;
    assert!(df.height() > 0, "The file .cottas is empty");
    println!("{:?}", df.head(Some(5)));
}

