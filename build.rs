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
