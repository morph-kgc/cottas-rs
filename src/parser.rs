//! RDF parser utilities for reading and handling triple/quad patterns.

use crate::utils::extract_format;
use oxrdf::{GraphName, Quad};
use oxrdfio::{RdfFormat, RdfParser};
use std::error::Error;
use std::fs::File;
use std::io;
use std::io::{BufReader, ErrorKind};

/// Parses an RDF file and returns its contents as a vector of tuples.
///
/// # Arguments
///
/// * `path` - The file path to the RDF file.
///
/// # Returns
///
/// * `Ok(Vec<(String, String, String, Option<String>)>)` - A vector of subject, predicate, object, and optional graph name.
/// * `Err(Box<dyn Error>)` - If the file cannot be read or parsed.
///
/// # Errors
///
/// Returns an error if the file extension is unsupported or if parsing fails.
pub fn parse_rdf_file(
    path: &str,
) -> Result<Vec<(String, String, String, Option<String>)>, Box<dyn Error>> {
    // 1. Handle unsupported extension error
    let format_str = extract_format(path).ok_or_else(|| {
        Box::new(io::Error::new(
            ErrorKind::InvalidData,
            format!("Unsupported RDF extension for file: {}", path),
        )) as Box<dyn Error>
    })?;

    let format = match format_str {
        "nt" => RdfFormat::NTriples,
        "nq" => RdfFormat::NQuads,
        "turtle" => RdfFormat::Turtle,
        "trig" => RdfFormat::TriG,
        "xml" => RdfFormat::RdfXml,
        _ => {
            return Err(Box::new(io::Error::new(
                ErrorKind::InvalidData,
                format!("Unsupported RDF format: {}", format_str),
            )));
        }
    };

    let file = File::open(path)?;
    let reader = BufReader::new(file);

    let parser = RdfParser::from_format(format);
    let mut quads_vec = Vec::new();

    for quad_result in parser.for_reader(reader) {
        let quad: Quad = quad_result?;

        let g: Option<String> = match &quad.graph_name {
            GraphName::NamedNode(ref node) => Some(node.to_string()),
            GraphName::BlankNode(node) => Some(format!("_:{}", node)),
            GraphName::DefaultGraph => None,
        };

        quads_vec.push((
            quad.subject.to_string(),
            quad.predicate.to_string(),
            quad.object.to_string(),
            g,
        ));
    }

    Ok(quads_vec)
}

/// Position indices for triple/quad patterns
pub const I_POS: [&str; 4] = ["s", "p", "o", "g"];

/// Parses a triple or quad pattern string and returns its components.
///
/// # Arguments
///
/// * `tp_str` - The triple or quad pattern as a string (e.g., "?s <http://pred> ?o" or "?s <http://pred> ?o <http://graph>").
///
/// # Returns
///
/// * `Vec<String>` - A vector containing the terms (3 for triple, 4 for quad).
///
/// # Panics
///
/// Panics if the input string has fewer than 3 terms.
pub fn parse_tp(tp_str: &str) -> Vec<String> {
    let parts: Vec<&str> = tp_str.split_whitespace().collect();

    if parts.len() < 3 {
        panic!("Triple pattern must have at least 3 terms");
    }

    let s_term = parts[0].to_string();
    let p_term = parts[1].to_string();

    // Object might contain spaces, so reconstruct it
    let after_predicate = tp_str
        .replacen(&s_term, "", 1)
        .replacen(&p_term, "", 1)
        .trim()
        .to_string();

    // Check if there's a graph term (quad pattern)
    let (o_term, g_term_opt) = if parts.len() > 3 {
        let last_token = parts[parts.len() - 1];
        if last_token.starts_with('<') || last_token.starts_with('?') {
            // It's a quad - last token is graph
            let o = after_predicate
                .replacen(last_token, "", 1)
                .trim()
                .to_string();
            (o, Some(last_token.to_string()))
        } else {
            (after_predicate, None)
        }
    } else {
        (after_predicate, None)
    };

    // Build result vector
    let mut result = vec![s_term, p_term, o_term];
    if let Some(g_term) = g_term_opt {
        result.push(g_term);
    }

    result
}
