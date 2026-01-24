pub mod duckdb;
pub mod export;
pub mod parser;
pub mod utils;

use crate::duckdb::{diff_duckdb, info_duckdb, verify_duckdb, CottasInfo};
pub use duckdb::{
    cat_duckdb, connection_in_memory, has_column, load_into_duckdb, search_in_duckdb,
};
pub use export::{export_to_cottas, write_quads_to_file};
pub use parser::parse_rdf_file;
use std::error::Error;
use std::fs::File;
pub use utils::extract_format;

//! Main library API for COTTAS-RS based on PYCOTTAS.
//!
//! # Modules
//! - `duckdb`: DuckDB integration and utilities.
//! - `export`: Exporting data to Cottas format.
//! - `parser`: RDF file parsing utilities.
//! - `utils`: Helper functions.

//! Converts an RDF file to a Cottas file, using the specified index.
//!
//! # Arguments
//! * `rdf_file_path` - Path to the input RDF file.
//! * `cottas_file_path` - Path to the output Cottas file.
//! * `index` - Index type (e.g., "spo").
//!
//! # Errors
//! Returns an error if parsing, loading, or exporting fails.
pub fn rdf2cottas(
    rdf_file_path: &str,
    cottas_file_path: &str,
    index: &str,
) -> Result<(), Box<dyn Error>> {
    let quads = parse_rdf_file(rdf_file_path)?;
    let quad_mode = quads.iter().any(|q| q.3.is_some());
    let conn = load_into_duckdb(&quads);
    export_to_cottas(&conn, index, cottas_file_path, quad_mode);
    Ok(())
}


//! Converts a Cottas file back to RDF format.
//!
//! # Arguments
//! * `cottas_file_path` - Path to the input Cottas file.
//! * `rdf_file_path` - Path to the output RDF file.
//!
//! # Errors
//! Returns an error if file creation or writing fails.
pub fn cottas2rdf(cottas_file_path: &str, rdf_file_path: &str) -> Result<(), Box<dyn Error>> {
    let conn = connection_in_memory();
    let has_named_graph = has_column(&conn, cottas_file_path, "g")?;

    let mut file = File::create(rdf_file_path)?;
    write_quads_to_file(&conn, cottas_file_path, has_named_graph, &mut file)?;

    Ok(())
}

//! Searches for triples/quads in a Cottas file matching a pattern.
//!
//! # Arguments
//! * `cottas_file_path` - Path to the Cottas file.
//! * `triple_pattern` - Pattern to search for.
//!
//! # Returns
//! A vector of matching results.
//!
//! # Errors
//! Returns an error if the search fails.
pub fn search(
    cottas_file_path: &str,
    triple_pattern: &str,
) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    search_in_duckdb(cottas_file_path, triple_pattern)
}

//! Concatenates multiple Cottas files into one.
//!
//! # Arguments
//! * `cottas_file_paths` - List of input file paths.
//! * `cottas_cat_file_path` - Output file path.
//! * `index` - Optional index type.
//! * `remove_input_files` - Optionally remove input files after concatenation.
//!
//! # Errors
//! Returns an error if concatenation fails.
pub fn cat(
    cottas_file_paths: &[String], //array of file paths
    cottas_cat_file_path: &str,
    index: Option<&str>,
    remove_input_files: Option<bool>,
) -> Result<(), Box<dyn Error>> {
    let index = index.unwrap_or("spo");
    let remove_input_files = remove_input_files.unwrap_or(false);
    cat_duckdb(
        cottas_file_paths,
        cottas_cat_file_path,
        index,
        remove_input_files,
    )
}

//! Computes the difference between two Cottas files.
//!
//! # Arguments
//! * `cottas_file_1_path` - First input file.
//! * `cottas_file_2_path` - Second input file.
//! * `cottas_diff_file_path` - Output file for the diff.
//! * `index` - Optional index type.
//! * `remove_input_files` - Optionally remove input files after diff.
//!
//! # Errors
//! Returns an error if diffing fails.
pub fn diff(
    cottas_file_1_path: &str,
    cottas_file_2_path: &str,
    cottas_diff_file_path: &str,
    index: Option<&str>,
    remove_input_files: Option<bool>,
) -> Result<(), Box<dyn Error>> {
    let index = index.unwrap_or("spo");
    let remove_input_files = remove_input_files.unwrap_or(false);
    diff_duckdb(
        cottas_file_1_path,
        cottas_file_2_path,
        cottas_diff_file_path,
        index,
        remove_input_files,
    )
}

//! Retrieves information about a Cottas file.
//!
//! # Arguments
//! * `cottas_file_path` - Path to the Cottas file.
//!
//! # Returns
//! `CottasInfo` struct with file metadata.
//!
//! # Errors
//! Returns an error if info retrieval fails.
pub fn info(cottas_file_path: &str) -> Result<CottasInfo, Box<dyn Error>> {
    info_duckdb(cottas_file_path)
}

//! Verifies the integrity of a Cottas file.
//!
//! # Arguments
//! * `cottas_file_path` - Path to the Cottas file.
//!
//! # Returns
//! `true` if the file is valid, `false` otherwise.
//!
//! # Errors
//! Returns an error if verification fails.
pub fn verify(cottas_file_path: &str) -> Result<bool, Box<dyn Error>> {
    verify_duckdb(cottas_file_path)
}
