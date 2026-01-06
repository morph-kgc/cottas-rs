use duckdb::Connection;
use std::error::Error;
use std::fs::File;
use std::io::Write;

pub fn export_to_cottas(conn: &Connection, _index: &str, path: &str, quad_mode: bool) {
    //let order_by = build_order_by(index);
    let select = if quad_mode {
        "SELECT DISTINCT s, p, o, g FROM quads"
    } else {
        "SELECT DISTINCT s, p, o FROM quads"
    };

    let query = format!(
        "COPY ({}) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22, PARQUET_VERSION 'V2')",
        select, path
    );

    conn.execute(query.as_str(), []).unwrap();
}

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
