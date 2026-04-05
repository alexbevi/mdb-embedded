fn main() {
    println!("cargo:rerun-if-changed=src/wt_shims.c");
    cc::Build::new()
        .file("src/wt_shims.c")
        .compile("wt_shims");
}
