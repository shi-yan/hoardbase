use std::env;

fn main() {
    // Tell Cargo that if the given file changes, to rerun this build script.
    println!("test build script ========================");
    println!("cargo:rerun-if-changed=rust_helper.h");
    println!("cargo:rerun-if-changed=rust_helper.cpp");
    println!("cargo:rerun-if-changed=record.h");
    println!("cargo:rerun-if-changed=record.cpp");
    
    let case_sensitive = env::var("CORROSION_BUILD_DIR").unwrap();
    println!("search localtion: {}", case_sensitive);
    println!("cargo:rustc-link-search=native={}", case_sensitive);
    println!("cargo:rustc-link-lib=static=hoardbase_cpp_rust_helper");
}