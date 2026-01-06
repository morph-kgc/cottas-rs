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

fn is_valid_index(index: &str) -> bool {
    let mut chars: Vec<char> = index.to_lowercase().chars().collect();
    chars.sort_unstable();

    match chars.len() {
        3 => chars == ['o', 'p', 's'],
        4 => chars == ['g', 'o', 'p', 's'],
        _ => false,
    }
}

pub fn build_order_by(index: &str) -> String {
    if !is_valid_index(index) {
        panic!("Invalid index: {}", index);
    }

    index
        .chars()
        .filter_map(|c| match c {
            's' => Some("s"),
            'p' => Some("p"),
            'o' => Some("o"),
            'g' => Some("g"),
            _ => None,
        })
        .collect::<Vec<_>>()
        .join(", ")
}
