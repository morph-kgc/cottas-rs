use duckdb::{Connection, ToSql};

pub fn load_into_duckdb(quads: &[(String, String, String, Option<String>)]) -> Connection {
    let mut conn = Connection::open_in_memory().unwrap();
    conn.execute("CREATE TABLE quads (s TEXT, p TEXT, o TEXT, g TEXT)", [])
        .unwrap();

    let tx = conn.transaction().unwrap();
    let mut stmt = tx
        .prepare("INSERT INTO quads (s, p, o, g) VALUES (?, ?, ?, ?)")
        .unwrap();
    for (s, p, o, g) in quads {
        stmt.execute([s as &dyn ToSql, p, o, g]).unwrap();
    }
    tx.commit().unwrap();
    conn
}
