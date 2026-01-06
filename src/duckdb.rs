use crate::parser::*;
use duckdb::{Connection, ToSql};
use std::error::Error;

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

pub fn connection_in_memory() -> Connection {
    Connection::open_in_memory().unwrap()
}

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
