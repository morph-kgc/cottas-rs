fn main() {
    // Solo en Windows (MSVC)
    #[cfg(target_os = "windows")]
    {
        // Necesario cuando DuckDB se compila con `bundled`:
        // DuckDB usa la Windows Restart Manager API → Rstrtmgr.lib
        println!("cargo:rustc-link-lib=Rstrtmgr");

        // Necesario para que el linker encuentre librerías estándar del sistema
        // Evita errores tipo "cannot open input file 'duckdb.lib'"
        println!("cargo:rustc-link-lib=shlwapi");
        println!("cargo:rustc-link-lib=version");

        // DuckDB produce *.lib y *.dll dentro de OUT_DIR
        // Informamos a cargo dónde buscar librerías generadas.
        let out_dir = std::env::var("OUT_DIR").unwrap();
        println!("cargo:rustc-link-search=native={}", out_dir);
    }

    // Forzar recompilación si este archivo cambia
    println!("cargo:rerun-if-changed=build.rs");
}
