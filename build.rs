// build.rs
fn main() {
    // Point to where libncbi-vdb.so actually is
    println!("cargo:rustc-link-search=native=/usr/local/ncbi/ncbi-vdb/lib64");
    println!("cargo:rustc-link-lib=ncbi-vdb");

    // We also need the C++ standard library since libncbi-vdb depends on it
    println!("cargo:rustc-link-lib=stdc++");

    // Rebuild if the build script changes
    println!("cargo:rerun-if-changed=build.rs");
}
