use cottas_rs::*;

#[test]
fn test_rdf2cottas() {
    let source = "tests/data/example.ttl";
    let target = "tests/data/example.cottas";
    let index = "spo";

    let _ = rdf2cottas(source, target, index);

    // Check that target file exists
    assert!(std::path::Path::new(target).exists());
}
