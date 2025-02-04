use std::env;
use std::path::PathBuf;

fn find_vdb_lib() -> Option<PathBuf> {
    // Check environment variable first
    if let Ok(path) = env::var("NCBI_VDB_PATH") {
        let lib_path = PathBuf::from(path).join("lib");
        if lib_path.exists() {
            return Some(lib_path);
        }
    }

    // Common installation paths
    let paths: Vec<String> = vec![
        // System-wide installation
        String::from("/usr/local/ncbi/ncbi-vdb/lib64"),
        String::from("/usr/local/ncbi/ncbi-vdb/lib"),
        String::from("/usr/lib64"),
        String::from("/usr/lib"),
        // Home directory builds
        format!(
            "{}/ncbi-outdir/ncbi-vdb/linux/gcc/x86_64/rel/lib",
            env::var("HOME").unwrap_or_default()
        ),
        format!(
            "{}/ncbi-outdir/sra-tools/linux/gcc/x86_64/rel/lib",
            env::var("HOME").unwrap_or_default()
        ),
    ];

    // Look for either libncbi-vdb.so or libncbi-vdb.so.* in each path
    for path in paths {
        let path = PathBuf::from(path);
        if path.exists() {
            // Check for exact match
            if path.join("libncbi-vdb.so").exists() {
                return Some(path);
            }

            // Check for versioned library
            if let Ok(entries) = std::fs::read_dir(&path) {
                for entry in entries {
                    if let Ok(entry) = entry {
                        let file_name = entry.file_name();
                        let file_name = file_name.to_string_lossy();
                        if file_name.starts_with("libncbi-vdb.so.") {
                            return Some(path);
                        }
                    }
                }
            }
        }
    }

    None
}

fn main() {
    // Find VDB library
    let vdb_path = find_vdb_lib()
        .expect("Could not find NCBI VDB library. Please install it or set NCBI_VDB_PATH");

    println!("cargo:rustc-link-search=native={}", vdb_path.display());
    println!("cargo:rustc-link-lib=ncbi-vdb");
    println!("cargo:rustc-link-lib=stdc++");

    // Add pkg-config support if available
    if let Ok(lib) = pkg_config::probe_library("ncbi-vdb") {
        for path in lib.link_paths {
            println!("cargo:rustc-link-search=native={}", path.display());
        }
    }

    // Rebuild if environment changes
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-env-changed=NCBI_VDB_PATH");
}
