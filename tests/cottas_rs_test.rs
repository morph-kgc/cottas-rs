use cottas_rs::*;
use polars::prelude::*;
use std::fs;
use std::path::Path;
use tempfile::TempDir;

#[test]
fn test_rdf2cottas() {
    let source_file = "tests/data/example.ttl";
    let target_file = "tests/data/example_2cottas.cottas";
    let index = "spo";

    rdf2_cottas(source_file, target_file, index).unwrap();

    // Check that target file exists
    assert!(Path::new(target_file).exists());

    let file = fs::File::open(target_file).unwrap();
    let df = ParquetReader::new(file).finish().unwrap();

    assert!(df.height() > 0, "The file .cottas is empty");
    println!("{:?}", df.head(Some(5)));
}

#[test]
fn test_cottas2_rdf() {
    let cottas_file = "tests/data/example.cottas";
    let rdf_file = "tests/data/output.rdf";

    cottas2_rdf(cottas_file, rdf_file).unwrap();

    assert!(Path::new(rdf_file).exists());

    let content = fs::read_to_string(rdf_file).unwrap();
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

#[test]
fn test_cat_cottas() {
    let input_files = vec![
        "tests/data/example.cottas".to_string(),
        "tests/data/example.cottas".to_string(),
    ];
    let output_file = "tests/data/merged.cottas";

    // Call the cat function
    cat(&input_files[..], output_file, Some("spo"), Some(false)).unwrap();

    // Check output exists
    assert!(Path::new(output_file).exists());

    // Optional: check number of rows
    let file = fs::File::open(output_file).unwrap();
    let df = ParquetReader::new(file).finish().unwrap();
    assert!(df.height() > 0, "Merged .cottas file is empty");

    println!("{:?}", df.head(Some(5)));
}

#[test]
fn test_cat_invalid_index() {
    let input_files = vec!["tests/data/example.cottas".to_string()];
    let output_file = "tests/data/merged_invalid.cottas";

    let result = cat(&input_files[..], output_file, Some("invalid"), Some(false));

    assert!(result.is_err());

    let err = result.unwrap_err();
    assert!(err.to_string().contains("is not valid"));
}

#[test]
fn test_cat_remove_input_files() {
    // Create temporary files
    let temp_dir = TempDir::new().unwrap();
    let file1 = temp_dir.path().join("file1.cottas");
    fs::copy("tests/data/example.cottas", &file1).unwrap();

    let output_file = temp_dir.path().join("merged.cottas");

    let input_files = vec![file1.to_string_lossy().to_string()];

    cat(
        &input_files,
        &output_file.to_string_lossy(),
        None,
        Some(true),
    )
    .unwrap();

    // Input files should be removed
    assert!(!file1.exists());

    // Output file should exist
    assert!(output_file.exists());
}

#[test]
fn test_diff_cottas() {
    let source_file1 = "tests/data/example1.ttl";
    let target_file1 = "tests/data/example1.cottas";
    let index = "spo";

    rdf2_cottas(source_file1, target_file1, index).unwrap();

    let source_file2 = "tests/data/example2.ttl";
    let target_file2 = "tests/data/example2.cottas";
    let index = "spo";

    rdf2_cottas(source_file2, target_file2, index).unwrap();

    let file1 = "tests/data/example1.cottas";
    let file2 = "tests/data/example2.cottas";
    let output_file = "tests/data/diff_output.cottas";

    // Call the diff function
    diff(file1, file2, output_file, Some("spo"), Some(true)).unwrap();

    // Check output exists
    assert!(Path::new(output_file).exists());

    // Check the diff file can be read and has data
    let file = fs::File::open(output_file).unwrap();
    let df = ParquetReader::new(file).finish().unwrap();

    println!("Diff result: {} rows", df.height());
    println!("{:?}", df.head(Some(5)));

    // Cleanup
    fs::remove_file(output_file).ok();
}

#[test]
fn test_info_real_cottas() {
    let source_file = "tests/data/example1.ttl";
    let target_file = "tests/data/example1.cottas";
    let index = "spo";

    rdf2_cottas(source_file, target_file, index).unwrap();

    let info = info(target_file).unwrap();

    assert!(info.triples > 0);
    assert!(info.distinct_subjects > 0);
    assert!(info.distinct_objects > 0);
    assert!(info.quads == false);
}

#[test]
fn test_verify_valid_cottas() {
    let result = verify("tests/data/example.cottas").unwrap();
    assert!(result, "Should be a valid cottas file");
}
