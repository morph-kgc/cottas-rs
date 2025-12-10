use crate::utils::build_order_by;
use duckdb::Connection;

pub fn export_to_cottas(conn: &Connection, index: &str, path: &str, quad_mode: bool) {
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

