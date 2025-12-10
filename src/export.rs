use crate::utils::build_order_by;
use duckdb::Connection;

pub fn export_to_cottas(conn: &Connection, index: &str, path: &str, quad_mode: bool) {
    let order_by = build_order_by(index);
    let select = if quad_mode {
        "SELECT DISTINCT s, p, o, g FROM quads"
    } else {
        "SELECT DISTINCT s, p, o FROM quads"
    };

    // DuckDB solo acepta un string simple en kv_metadata
    let kv_metadata = format!("index={}", index);

    let query = format!(
        "COPY ({} ORDER BY {}) TO '{}' (FORMAT PARQUET, COMPRESSION ZSTD, COMPRESSION_LEVEL 22, PARQUET_VERSION '2.0', kv_metadata='{}')",
        select, order_by, path, kv_metadata
    );

    conn.execute(query.as_str(), []).unwrap();
}

