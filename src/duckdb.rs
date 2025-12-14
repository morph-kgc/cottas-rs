use std::error::Error;
use duckdb::{Connection, ToSql};
use crate::utils::translate_triple_pattern;

pub fn load_into_duckdb(
    quads: &[(String, String, String, Option<String>)]
) -> Connection {
    let conn = connection_in_memory();

    // Create table
    conn.execute(
        "CREATE TABLE quads (s TEXT, p TEXT, o TEXT, g TEXT)",
        []
    ).unwrap();

    // Insert quads directly
    for (s, p, o, g) in quads {
        let g_ref: &dyn ToSql = match g {
            Some(val) => val,
            None => &Option::<String>::None,
        };

        conn.execute(
            "INSERT INTO quads (s, p, o, g) VALUES (?, ?, ?, ?)",
            [s as &dyn ToSql, p, o, g_ref]
        ).unwrap();
    }

    conn
}

pub fn connection_in_memory() -> Connection {
    Connection::open_in_memory().unwrap()
}

pub fn has_column(conn: &Connection, cottas_file_path: &str, column: &str) -> Result<bool, Box<dyn Error>> {
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

pub fn search_in_duckdb(cottas_file_path: &str, triple_pattern: &str) -> Result<bool, Box<dyn Error>>{
    let conn = connection_in_memory();
    conn.execute(translate_triple_pattern(cottas_file_path, triple_pattern)).fetchall()
}
