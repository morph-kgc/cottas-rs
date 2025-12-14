use cottas_rs::*;
use polars::prelude::*;

#[test]
fn test_rdf2cottas() {
    let source_file = "tests/data/example.ttl";
    let target_file = "tests/data/output.cottas";
    let index = "spo";

    rdf2cottas(source_file, target_file, index).unwrap();

    // Check that target file exists
    assert!(std::path::Path::new(target_file).exists());

    let file = std::fs::File::open(target_file).unwrap();
    let df = ParquetReader::new(file).finish().unwrap();

    assert!(df.height() > 0, "The file .cottas is empty");
    println!("{:?}", df.head(Some(5)));
}

#[test]
fn test_cottas2rdf() {
    let cottas_file = "tests/data/example.cottas";
    let rdf_file = "tests/data/output.rdf";

    cottas2rdf(cottas_file, rdf_file).unwrap();

    assert!(std::path::Path::new(rdf_file).exists());

    let content = std::fs::read_to_string(rdf_file).unwrap();
    println!(
        "{}",
        &content.lines().take(5).collect::<Vec<_>>().join("\n")
    );
}
