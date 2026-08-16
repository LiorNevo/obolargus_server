//! Code-driven documentation generation runs as part of the test pipeline
//! (Constitution VI, mirroring `lx_engine`'s `test_generate_rust_docs`).
//!
//! rustdoc is the code generator for this crate's API reference; this test
//! executes it and asserts the generated documentation exists, so the docs
//! cannot silently drift from the code.

use std::path::Path;
use std::process::Command;

#[test]
fn rust_docs_generate_successfully() {
    let manifest_dir = env!("CARGO_MANIFEST_DIR");
    let target_dir = Path::new(manifest_dir).join("target");

    let status = Command::new(env!("CARGO"))
        .args(["doc", "--no-deps", "--target-dir"])
        .arg(&target_dir)
        .current_dir(manifest_dir)
        .status()
        .expect("failed to run cargo doc");

    assert!(status.success(), "cargo doc --no-deps failed");

    let crate_dir = env!("CARGO_PKG_NAME").replace('-', "_");
    let index = target_dir.join("doc").join(&crate_dir).join("index.html");
    assert!(
        index.exists(),
        "generated rustdoc missing at {}",
        index.display()
    );
    assert!(
        std::fs::metadata(&index)
            .map(|m| m.len() > 0)
            .unwrap_or(false),
        "generated rustdoc is empty"
    );
}
