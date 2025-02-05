use std::env;
use std::path::PathBuf;

fn get_platform_dir() -> String {
    let os = env::consts::OS;
    let arch = env::consts::ARCH;
    format!("{}-{}", os, arch)
}

fn main() {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    let platform_dir = get_platform_dir();

    // Look for static library in platform-specific directory
    let static_lib_dir = manifest_dir
        .join("vendor")
        .join("static")
        .join(&platform_dir);

    if !static_lib_dir.exists() {
        panic!(
            "No pre-built static library found for platform: {}",
            &platform_dir
        );
    }

    // Link against our bundled static library
    println!(
        "cargo:rustc-link-search=native={}",
        static_lib_dir.display()
    );
    println!("cargo:rustc-link-lib=static=ncbi-vdb");

    // Platform-specific C++ standard library
    if cfg!(target_os = "macos") {
        println!("cargo:rustc-link-lib=c++");
    } else {
        println!("cargo:rustc-link-lib=stdc++");
    }

    // Rebuild if the static libraries change
    println!("cargo:rerun-if-changed=vendor/static");
}
