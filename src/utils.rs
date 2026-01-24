//! Utility functions for file format extraction, index validation, and SQL order building.

/// Extracts the RDF format from a file path based on its extension.
///
/// # Arguments
///
/// * `path` - The file path as a string slice.
///
/// # Returns
///
/// * `Some(&'static str)` - The RDF format as a string if recognized.
/// * `None` - If the extension is not recognized.
pub fn extract_format(path: &str) -> Option<&'static str> {
    if path.ends_with(".ttl") {
        Some("turtle")
    } else if path.ends_with(".nt") {
        Some("nt")
    } else if path.ends_with(".nq") {
        Some("nq")
    } else if path.ends_with(".trig") {
        Some("trig")
    } else if path.ends_with(".rdf") || path.ends_with(".xml") {
        Some("xml")
    } else {
        None
    }
}

/// Checks if the given index string is a valid triple or quad index.
///
/// # Arguments
///
/// * `index` - The index string (e.g., "spo", "gspo").
///
/// # Returns
///
/// * `true` if the index is valid, `false` otherwise.
pub fn is_valid_index(index: &str) -> bool {
    let mut chars: Vec<char> = index.to_lowercase().chars().collect();
    chars.sort_unstable();

    match chars.len() {
        3 => chars == ['o', 'p', 's'],
        4 => chars == ['g', 'o', 'p', 's'],
        _ => false,
    }
}

/// Builds an SQL ORDER BY clause from the given index string.
///
/// # Arguments
///
/// * `index` - The index string (e.g., "spo", "gspo").
/// * `quad_mode` - If true, includes the graph column.
///
/// # Returns
///
/// * `String` - The ORDER BY clause for SQL queries.
///
/// # Panics
///
/// Panics if the index is invalid.
pub fn build_order_by(index: &str, quad_mode: bool) -> String {
    if !is_valid_index(index) {
        panic!("Invalid index: {}", index);
    }

    let mut cols: Vec<&str> = index
        .chars()
        .filter_map(|c| match c {
            's' => Some("s"),
            'p' => Some("p"),
            'o' => Some("o"),
            _ => None,
        })
        .collect();

    if quad_mode {
        cols.push("g");
    }

    format!("ORDER BY {}", cols.join(", "))
}
