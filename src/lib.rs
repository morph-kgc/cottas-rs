pub mod duckdb;
pub mod export;
pub mod parser;
pub mod utils;

pub use duckdb::{connection_in_memory, has_column, load_into_duckdb, search_in_duckdb, cat_duckdb};
pub use export::{export_to_cottas, write_quads_to_file};
pub use parser::parse_rdf_file;
use std::error::Error;
use std::fs::File;
pub use utils::build_order_by;
pub use utils::extract_format;

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

pub fn cottas2rdf(cottas_file_path: &str, rdf_file_path: &str) -> Result<(), Box<dyn Error>> {
    let conn = connection_in_memory();
    let has_named_graph = has_column(&conn, cottas_file_path, "g")?;

    let mut file = File::create(rdf_file_path)?;
    write_quads_to_file(&conn, cottas_file_path, has_named_graph, &mut file)?;

    Ok(())
}

pub fn search(
    cottas_file_path: &str,
    triple_pattern: &str,
) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    search_in_duckdb(cottas_file_path, triple_pattern)
}

pub fn cat(
    cottas_file_paths: &str,
    cottas_cat_file_path: &str,
    index: Option<&str>,
    remove_input_files: Option<&bool>
) -> Result<(), Box<dyn Error>> {
    cat_duckdb(
        cottas_file_paths,
        cottas_cat_file_path,
        index,
        remove_input_files
    )
}
