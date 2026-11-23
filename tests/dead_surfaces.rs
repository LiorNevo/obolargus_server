//! Dead-surface inventory (T097).
//!
//! Pins the cleanup promised by FR-035: the empty temporal modules are gone,
//! the reserved market-data knob is documented, and the Portfolios placeholder
//! is explicitly marked unavailable rather than implying a shipped page.

use std::path::Path;

#[test]
fn empty_temporal_modules_are_removed() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("../obolargus_core/src/temporal");
    for module in ["ingest", "positions", "retention"] {
        assert!(
            !root.join(module).join("mod.rs").exists(),
            "empty temporal module `{module}` still exists"
        );
    }
}

#[test]
fn reserved_knob_and_placeholder_are_documented() {
    let readme = include_str!("../../README.md");
    assert!(
        readme.contains("MARKET_DATA_REFRESH_BATCH_SIZE"),
        "README must mention the reserved batch-size knob"
    );
    assert!(
        readme.contains("reserved"),
        "README must mark the reserved knob as having no effect"
    );

    let pages = include_str!("../../obolargus_ui/js/navigation/pages.tsx");
    assert!(
        pages.contains("not_yet_available"),
        "the Portfolios placeholder must be marked unavailable"
    );
}
