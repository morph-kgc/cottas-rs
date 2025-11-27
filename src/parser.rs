use crate::utils::extract_format;

pub fn parse_rdf_file(
    path: &str,
) -> Result<Vec<(String, String, String, Option<String>)>, Box<dyn std::error::Error>> {
    let content = std::fs::read_to_string(path)?;
    let format = extract_format(path).ok_or("Unknown RDF format")?;
    let mut quads_vec = Vec::new();

    match format {
        "turtle" => {
            let parser = turtle::parse_str(&content);

            // collect triples and convert them to quads (graph = None)
            let quads_vec = parser.collect_triples()?
                .into_iter()
                .map(|t| (
                    t.s().value().to_string(),
                    t.p().value().to_string(),
                    t.o().value().to_string(),
                    None,
                ))
                .collect();
        }
        _ => unreachable!("unsupported RDF format"),
    }

    Ok(quads_vec)
}
