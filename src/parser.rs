use oxrdfio::{RdfFormat, RdfParser};
use oxrdf::{Quad, GraphName};
use std::error::Error;
use std::fs;
use std::io;
use std::io::ErrorKind;
use crate::utils::extract_format;

pub fn parse_rdf_file(
    path: &str,
) -> Result<Vec<(String, String, String, Option<String>)>, Box<dyn Error>> {
    let data = fs::read(path)?;

    let create_io_error = |msg: String| {
        Box::from(io::Error::new(ErrorKind::InvalidData, msg))
    };

    // 1. Handle unsupported extension error
    let format_str = extract_format(path)
        .ok_or_else(|| {
            create_io_error(format!("Unsupported RDF extension for file: {}", path))
        })?;

    let format = match format_str{
        "nt" => RdfFormat::NTriples,
        "nq" => RdfFormat::NQuads,
        "turtle" => RdfFormat::Turtle,
        "trig" => RdfFormat::TriG,
        "xml" => RdfFormat::RdfXml,
        _ => {
            return Err(create_io_error(format!("Unsupported RDF format: {}", format_str)))
        }
    };

    let parser = RdfParser::from_format(format);
    let mut quads_vec = Vec::new();

    for quad_result in parser.for_reader(&data[..]) {
        let quad: Quad = quad_result?;

        let g: Option<String> = match &quad.graph_name {
            GraphName::NamedNode(ref node) => Some(node.to_string()),
            GraphName::BlankNode(node) => Some(format!("_:{}", node)),
            GraphName::DefaultGraph => None
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