//! DuckDB integration utilities for Cottas: loading, querying, and managing Parquet-based RDF data.

pub use crate::parser::*;
use crate::utils::build_order_by;
pub use crate::utils::is_valid_index;
use chrono::{DateTime, Utc};
use duckdb::{Connection, ToSql};
use serde::{Deserialize, Serialize};
use std::collections::HashSet;
use std::error::Error;
use std::fs;

/// Loads a vector of RDF quads into an in-memory DuckDB database.
///
/// # Arguments
///
/// * `quads` - A slice of tuples representing subject, predicate, object, and optional graph.
///
/// # Returns
///
/// * `Connection` - An in-memory DuckDB connection with the loaded data.
pub fn load_into_duckdb(quads: &[(String, String, String, Option<String>)]) -> Connection {
    let conn = connection_in_memory();

    // Create table
    conn.execute("CREATE TABLE quads (s TEXT, p TEXT, o TEXT, g TEXT)", [])
        .unwrap();

    // Insert quads directly
    for (s, p, o, g) in quads {
        let g_ref: &dyn ToSql = match g {
            Some(val) => val,
            None => &Option::<String>::None,
        };

        conn.execute(
            "INSERT INTO quads (s, p, o, g) VALUES (?, ?, ?, ?)",
            [s as &dyn ToSql, p, o, g_ref],
        )
        .unwrap();
    }

    conn
}

/// Creates a new in-memory DuckDB connection.
///
/// # Returns
///
/// * `Connection` - An in-memory DuckDB connection.
pub fn connection_in_memory() -> Connection {
    Connection::open_in_memory().unwrap()
}

/// Checks if a Parquet file contains a specific column.
///
/// # Arguments
///
/// * `conn` - The DuckDB connection.
/// * `cottas_file_path` - Path to the Parquet file.
/// * `column` - The column name to check.
///
/// # Returns
///
/// * `Ok(true)` if the column exists, `Ok(false)` otherwise.
pub fn has_column(
    conn: &Connection,
    cottas_file_path: &str,
    column: &str,
) -> Result<bool, Box<dyn Error>> {
    let mut stmt = conn.prepare("SELECT name FROM PARQUET_SCHEMA(?)")?;
    let mut rows = stmt.query([cottas_file_path])?;

    while let Some(row) = rows.next()? {
        let name: String = row.get(0)?;
        if name == column {
            return Ok(true);
        }
    }

    Ok(false)
}

/// Translates a triple or quad pattern into a DuckDB SQL query.
///
/// # Arguments
///
/// * `cottas_file_path` - Path to the Parquet file.
/// * `triple_pattern` - The triple or quad pattern as a string.
///
/// # Returns
///
/// * `String` - The generated SQL query.
pub fn translate_triple_pattern(cottas_file_path: &str, triple_pattern: &str) -> String {
    // Parse the triple pattern
    let tp_tuple = parse_tp(triple_pattern);

    let select_clause = if tp_tuple.len() == 3 {
        "SELECT s, p, o"
    } else if tp_tuple.len() == 4 {
        "SELECT s, p, o, g"
    } else {
        panic!("Pattern must be a tuple of length 3 (triple) or 4 (quad)");
    };

    // Start building query
    let mut query = format!(
        "{} FROM PARQUET_SCAN('{}') WHERE ",
        select_clause, cottas_file_path
    );

    // Build WHERE clause - iterate over all positions
    let mut has_conditions = false;
    for i in 0..tp_tuple.len() {
        let term = &tp_tuple[i];

        // Only add condition if it's not a variable (doesn't start with ?)
        if !term.starts_with('?') && !term.starts_with('$') {
            // Escape single quotes to prevent SQL injection
            let escaped_value = term.replace('\'', "''");
            query.push_str(&format!("{}='{}' AND ", I_POS[i], escaped_value));
            has_conditions = true;
        }
    }

    // Clean up trailing 'AND ' or 'WHERE '
    if query.ends_with("AND ") {
        query.truncate(query.len() - 4);
    }
    if query.ends_with("WHERE ") {
        query.truncate(query.len() - 6);
    }

    query
}

/// Searches for matches of a triple or quad pattern in a Parquet file using DuckDB.
///
/// # Arguments
///
/// * `cottas_file_path` - Path to the Parquet file.
/// * `triple_pattern` - The triple or quad pattern as a string.
///
/// # Returns
///
/// * `Ok(Vec<Vec<String>>)` - Query results as vectors of strings.
pub fn search_in_duckdb(
    cottas_file_path: &str,
    triple_pattern: &str,
) -> Result<Vec<Vec<String>>, Box<dyn Error>> {
    let conn = connection_in_memory();

    // Translate pattern to SQL
    let sql = translate_triple_pattern(cottas_file_path, triple_pattern);

    // Prepare statement
    let mut stmt = conn.prepare(&sql)?;

    // Determine number of columns based on pattern
    let tp_tuple = parse_tp(triple_pattern);
    let column_count = tp_tuple.len();

    // Execute query and collect results
    let rows = stmt.query_map([], |row| {
        let mut result = Vec::new();
        for i in 0..column_count {
            let val: String = row.get(i)?;
            result.push(val);
        }
        Ok(result)
    })?;

    // Collect all rows
    let results: Result<Vec<Vec<String>>, duckdb::Error> = rows.collect();
    Ok(results?)
}

/// Concatenates multiple Parquet files into a single file, optionally removing the inputs.
///
/// # Arguments
///
/// * `cottas_file_paths` - Slice of input file paths.
/// * `cottas_cat_file_path` - Output file path.
/// * `index` - Index string for ordering.
/// * `remove_input_files` - If true, deletes input files after concatenation.
///
/// # Returns
///
/// * `Ok(())` on success.
pub fn cat_duckdb(
    cottas_file_paths: &[String],
    cottas_cat_file_path: &str,
    index: &str,
    remove_input_files: bool,
) -> Result<(), Box<dyn Error>> {
    if !is_valid_index(index) {
        eprintln!("Index `{}` is not valid.", index);
        return Ok(());
    }

    // Join file paths for DuckDB PARQUET_SCAN
    let parquet_files = cottas_file_paths
        .iter()
        .map(|p| format!("'{}'", p))
        .collect::<Vec<_>>()
        .join(", ");

    // Open DuckDB connection
    let conn = connection_in_memory();

    // Use build_order_by (Python-style: quad_mode = false)
    let order_by = build_order_by(index, false);

    // Build KV_METADATA index
    let index_metadata = index.to_lowercase();

    // Build the COPY SQL query
    let cat_query = format!(
        "COPY (SELECT DISTINCT s, p, o FROM PARQUET_SCAN([{}], union_by_name = true) {}) TO '{}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 22,
            PARQUET_VERSION v2,
            KV_METADATA {{index: '{}'}}
        )",
        parquet_files, order_by, cottas_cat_file_path, index_metadata
    );

    // Execute query
    conn.execute(&cat_query, [])?;

    // Optionally remove input files
    if remove_input_files {
        for file in cottas_file_paths {
            fs::remove_file(file)?;
        }
    }

    Ok(())
}

/// Computes the difference between two Parquet files and writes the result to a new file.
///
/// # Arguments
///
/// * `cottas_file_1_path` - First input file path.
/// * `cottas_file_2_path` - Second input file path.
/// * `cottas_diff_file_path` - Output file path.
/// * `index` - Index string for ordering.
/// * `remove_input_files` - If true, deletes input files after diff.
///
/// # Returns
///
/// * `Ok(())` on success.
pub fn diff_duckdb(
    cottas_file_1_path: &str,
    cottas_file_2_path: &str,
    cottas_diff_file_path: &str,
    index: &str,
    remove_input_files: bool,
) -> Result<(), Box<dyn Error>> {
    if !is_valid_index(index) {
        eprintln!("Index `{}` is not valid.", index);
        return Ok(());
    }

    // Open DuckDB connection
    let conn = connection_in_memory();

    // Use build_order_by (Python-style: quad_mode = false)
    let order_by = build_order_by(index, false);

    // Build KV_METADATA index
    let index_metadata = index.to_lowercase();

    // Build the COPY SQL query
    let diff_query = format!(
        "COPY (SELECT * FROM (SELECT DISTINCT * FROM PARQUET_SCAN('{}') EXCEPT SELECT * FROM PARQUET_SCAN('{}')) {}) TO '{}' (
            FORMAT PARQUET,
            COMPRESSION ZSTD,
            COMPRESSION_LEVEL 22,
            PARQUET_VERSION v2,
            KV_METADATA {{index: '{}'}}
        )",
        cottas_file_1_path,
        cottas_file_2_path,
        order_by,
        cottas_diff_file_path,
        index_metadata
    );

    // Execute query
    conn.execute(&diff_query, [])?;

    // Optionally remove input files
    if remove_input_files {
        fs::remove_file(cottas_file_1_path)?;
        fs::remove_file(cottas_file_2_path)?;
    }

    Ok(())
}

/// Verifies that a Parquet file has the required columns for a Cottas file.
///
/// # Arguments
///
/// * `cottas_file_path` - Path to the Parquet file.
///
/// # Returns
///
/// * `Ok(true)` if valid, `Ok(false)` otherwise.
pub fn verify_duckdb(cottas_file_path: &str) -> Result<bool, Box<dyn Error>> {
    let conn = connection_in_memory();

    let verify_query = format!(
        "DESCRIBE SELECT * FROM PARQUET_SCAN('{}') LIMIT 1",
        cottas_file_path
    );

    let mut stmt = conn.prepare(&verify_query)?;
    let rows = stmt.query_map([], |row| {
        let column_name: String = row.get(0)?;
        Ok(column_name)
    })?;

    let mut cottas_columns = HashSet::new();
    for row in rows {
        cottas_columns.insert(row?);
    }

    for pos in ['s', 'p', 'o'] {
        if !cottas_columns.contains(&pos.to_string()) {
            return Ok(false);
        }
    }

    let valid_columns: HashSet<String> = ['s', 'p', 'o', 'g']
        .iter()
        .map(|&c| c.to_string())
        .collect();

    let is_valid = cottas_columns.is_subset(&valid_columns);

    Ok(is_valid)
}

/// Metadata about a Cottas Parquet file.
#[derive(Debug, Serialize, Deserialize)]
pub struct CottasInfo {
    /// Index string (e.g., "spo").
    pub index: String,
    /// Number of triples.
    pub triples: i64,
    /// Number of row groups.
    pub triples_groups: i64,
    /// Number of distinct properties.
    pub properties: i64,
    /// Number of distinct subjects.
    pub distinct_subjects: i64,
    /// Number of distinct objects.
    pub distinct_objects: i64,
    /// File creation or modification timestamp (RFC3339).
    pub issued: String,
    /// File size in megabytes.
    pub size_mb: f64,
    /// Compression algorithm used.
    pub compression: String,
    /// True if file contains quads (has a graph column).
    pub quads: bool,
}

/// Extracts metadata and statistics from a Cottas Parquet file.
///
/// # Arguments
///
/// * `cottas_file_path` - Path to the Parquet file.
///
/// # Returns
///
/// * `Ok(CottasInfo)` with file metadata and statistics.
pub fn info_duckdb(cottas_file_path: &str) -> Result<CottasInfo, Box<dyn Error>> {
    let conn = connection_in_memory();

    // Get file metadata
    let metadata = fs::metadata(cottas_file_path)?;
    let ctime = metadata.created().or_else(|_| metadata.modified())?;
    let cottas_issued: DateTime<Utc> = ctime.into();
    let size_mb = metadata.len() as f64 / 1_000_000.0;

    // Build queries
    let kv_query = format!(
        "SELECT * FROM PARQUET_KV_METADATA('{}') WHERE key='index'",
        cottas_file_path
    );
    let row_query = format!(
        "SELECT num_rows AS triples, num_row_groups AS triples_groups FROM PARQUET_FILE_METADATA('{}')",
        cottas_file_path
    );
    let properties_query = format!(
        "SELECT COUNT(DISTINCT p) FROM PARQUET_SCAN('{}')",
        cottas_file_path
    );
    let distinct_subjects_query = format!(
        "SELECT COUNT(DISTINCT s) FROM PARQUET_SCAN('{}')",
        cottas_file_path
    );
    let distinct_objects_query = format!(
        "SELECT COUNT(DISTINCT o) FROM PARQUET_SCAN('{}')",
        cottas_file_path
    );
    let schema_query = format!(
        "DESCRIBE SELECT * FROM PARQUET_SCAN('{}') LIMIT 1",
        cottas_file_path
    );
    let compression_query = format!(
        "SELECT compression FROM PARQUET_METADATA('{}')",
        cottas_file_path
    );

    // Execute queries and collect results
    let index: String = conn.query_row(&kv_query, [], |row| {
        let value: Vec<u8> = row.get(2)?;
        Ok(String::from_utf8_lossy(&value).to_string())
    })?;

    let (triples, triples_groups): (i64, i64) =
        conn.query_row(&row_query, [], |row| Ok((row.get(0)?, row.get(1)?)))?;

    let properties: i64 = conn.query_row(&properties_query, [], |row| row.get(0))?;
    let distinct_subjects: i64 = conn.query_row(&distinct_subjects_query, [], |row| row.get(0))?;
    let distinct_objects: i64 = conn.query_row(&distinct_objects_query, [], |row| row.get(0))?;
    let compression: String = conn.query_row(&compression_query, [], |row| row.get(0))?;

    // Check if 'g' column exists (quads)
    let mut stmt = conn.prepare(&schema_query)?;
    let column_names: Vec<String> = stmt
        .query_map([], |row| {
            let col_name: String = row.get(0)?;
            Ok(col_name)
        })?
        .collect::<Result<Vec<_>, _>>()?;

    let quads = column_names.contains(&"g".to_string());

    Ok(CottasInfo {
        index,
        triples,
        triples_groups,
        properties,
        distinct_subjects,
        distinct_objects,
        issued: cottas_issued.to_rfc3339(),
        size_mb,
        compression,
        quads,
    })
}
