use cottas_rs::*;
use polars::polars_utils::parma::raw::Key;
use polars::prelude::*;
use std::path::Path;

#[test]
fn test_rdf2cottas() {
    let source_file = "tests/data/example.ttl";
    let target_file = "tests/data/output.cottas";
    let index = "spo";

    rdf2cottas(source_file, target_file, index).unwrap();

    // Check that target file exists
    assert!(Path::new(target_file).exists());

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

    assert!(Path::new(rdf_file).exists());

    let content = std::fs::read_to_string(rdf_file).unwrap();
    println!(
        "{}",
        &content.lines().take(5).collect::<Vec<_>>().join("\n")
    );
}

#[test]
fn test_search_all_triples() {
    let cottas_file = "tests/data/example.cottas";
    let pattern = "?s ?p ?o";

    let results = search(cottas_file, pattern).unwrap();

    println!("Found {} triples:", results.len());
    for (i, row) in results.iter().enumerate() {
        println!("  {}: {} {} {}", i + 1, row[0], row[1], row[2]);
    }

    assert_eq!(results.len(), 3, "Expected 3 triples");

    for row in &results {
        assert_eq!(row.len(), 3, "Each row should have 3 elements");
    }
}

#[test]
fn test_search_by_predicate() {
    let cottas_file = "tests/data/example.cottas";
    let pattern = "?s <http://example.org/knows> ?o";

    if !Path::new(cottas_file).exists() {
        println!("Cottas file not found, skipping test");
        return;
    }

    let results = search(cottas_file, pattern).unwrap();

    println!("Found {} 'knows' relationships:", results.len());
    for row in &results {
        println!("  {} knows {}", row[0], row[2]);
    }

    assert_eq!(results.len(), 3);

    // Verify all have the correct predicate
    for row in &results {
        assert_eq!(row[1], "<http://example.org/knows>");
    }
}

#[test]
fn test_search_specific_subject() {
    let cottas_file = "tests/data/example.cottas";
    let pattern = "<http://example.org/Alice> ?p ?o";

    if !Path::new(cottas_file).exists() {
        println!("Cottas file not found, skipping test");
        return;
    }

    let results = search(cottas_file, pattern).unwrap();

    println!("Alice's relationships:");
    for row in &results {
        println!("  Alice {} {}", row[1], row[2]);
    }

    // Alice knows Bob (1 triple)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0][0], "<http://example.org/Alice>");
    assert_eq!(results[0][2], "<http://example.org/Bob>");
}

#[test]
fn test_search_specific_object() {
    let cottas_file = "tests/data/example.cottas";
    let pattern = "?s <http://example.org/knows> <http://example.org/Alice>";

    if !Path::new(cottas_file).exists() {
        println!("Cottas file not found, skipping test");
        return;
    }

    let results = search(cottas_file, pattern).unwrap();

    println!("Who knows Alice:");
    for row in &results {
        println!("  {} knows Alice", row[0]);
    }

    // Charlie knows Alice (1 triple)
    assert_eq!(results.len(), 1);
    assert_eq!(results[0][0], "<http://example.org/Charlie>");
    assert_eq!(results[0][2], "<http://example.org/Alice>");
}

#[test]
fn test_search_exact_triple() {
    let cottas_file = "tests/data/example.cottas";
    let pattern =
        "<http://example.org/Bob> <http://example.org/knows> <http://example.org/Charlie>";

    if !Path::new(cottas_file).exists() {
        println!("Cottas file not found, skipping test");
        return;
    }

    let results = search(cottas_file, pattern).unwrap();

    println!("Exact match results: {}", results.len());

    // Should find exactly one triple: Bob knows Charlie
    assert_eq!(results.len(), 1);
    assert_eq!(results[0][0], "<http://example.org/Bob>");
    assert_eq!(results[0][1], "<http://example.org/knows>");
    assert_eq!(results[0][2], "<http://example.org/Charlie>");
}

#[test]
fn test_search_no_results() {
    let cottas_file = "tests/data/example.cottas";
    let pattern = "?s <http://example.org/teachs> ?o";

    if !Path::new(cottas_file).exists() {
        println!("Cottas file not found, skipping test");
        return;
    }

    let results = search(cottas_file, pattern).unwrap();

    // No 'teachs' relationships in the data
    assert_eq!(
        results.len(),
        0,
        "Should find no results for non-existent predicate"
    );
}
