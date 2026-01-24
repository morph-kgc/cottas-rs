//! Export utilities for writing query results to Cottas and files.

use crate::utils::build_order_by;
use duckdb::Connection;
use std::error::Error;
use std::fs::File;
use std::io::Write;

/// Exports query results from DuckDB to a Cottas (Parquet) file.
///
/// # Arguments
///
/// * `conn` - The DuckDB connection.
/// * `index` - The index string used for ordering.
/// * `path` - The output file path.
/// * `quad_mode` - If true, exports quads; otherwise, exports triples.
pub fn export_to_cottas(conn: &Connection, index: &str, path: &str, quad_mode: bool) {
    let select = if quad_mode {
        "SELECT DISTINCT s, p, o, g FROM quads"
    } else {
        "SELECT DISTINCT s, p, o FROM quads"
    };

    let order_by = build_order_by(index, quad_mode);

    let query = format!(
        "COPY ({} {}) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22, PARQUET_VERSION 'V2')",
        select, order_by, path
    );

    conn.execute(query.as_str(), []).unwrap();
}

/// Writes quads or triples from a Cottas (Parquet) file to a text file.
///
/// # Arguments
///
/// * `conn` - The DuckDB connection.
/// * `cottas_file_path` - Path to the Cottas (Parquet) file.
/// * `has_named_graph` - If true, expects quads; otherwise, triples.
/// * `file` - Mutable reference to the output file.
///
/// # Errors
///
/// Returns an error if reading or writing fails.
pub fn write_quads_to_file(
    conn: &Connection,
    cottas_file_path: &str,
    has_named_graph: bool,
    file: &mut File,
) -> Result<(), Box<dyn Error>> {
    let select = if has_named_graph {
        "s, p, o, g"
    } else {
        "s, p, o"
    };
    let query = format!(
        "SELECT {} FROM PARQUET_SCAN('{}')",
        select, cottas_file_path
    );

    let mut stmt = conn.prepare(&query)?;
    let mut rows = stmt.query([])?;

    while let Some(row) = rows.next()? {
        let line = if has_named_graph {
            format!(
                "{} {} {} {} .\n",
                row.get::<_, String>(0).unwrap_or_default(),
                row.get::<_, String>(1).unwrap_or_default(),
                row.get::<_, String>(2).unwrap_or_default(),
                row.get::<_, String>(3).unwrap_or_default(),
            )
        } else {
            format!(
                "{} {} {} .\n",
                row.get::<_, String>(0).unwrap_or_default(),
                row.get::<_, String>(1).unwrap_or_default(),
                row.get::<_, String>(2).unwrap_or_default(),
            )
        };
        file.write_all(line.as_bytes())?;
    }

    Ok(())
}
