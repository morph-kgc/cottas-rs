// build.rs: Cargo build script for Windows linking
//
// This script is executed automatically by Cargo before building the project.
// On Windows, it instructs Cargo to link the Rust binary with the `Rstrtmgr`,
// `shlwapi`, and `version` system libraries, and adds the build output
// directory to the native library search path. This ensures the Rust project
// can find and link these required system libraries during compilation.
//
// The script also tells Cargo to rerun the build script if `build.rs` changes.
fn main() {
    #[cfg(target_os = "windows")]
    {
        println!("cargo:rustc-link-lib=Rstrtmgr");
        println!("cargo:rustc-link-lib=shlwapi");
        println!("cargo:rustc-link-lib=version");

        let out_dir = std::env::var("OUT_DIR").unwrap();
        println!("cargo:rustc-link-search=native={}", out_dir);
    }

    println!("cargo:rerun-if-changed=build.rs");
}
