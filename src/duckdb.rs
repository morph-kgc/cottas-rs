use duckdb::{Connection, ToSql};

pub fn load_into_duckdb(
    quads: &[(String, String, String, Option<String>)]
) -> Connection {
    let conn = Connection::open_in_memory().unwrap();

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
