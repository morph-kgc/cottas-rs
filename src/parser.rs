use crate::utils::extract_format;
use oxrdf::{GraphName, Quad};
use oxrdfio::{RdfFormat, RdfParser};
use std::error::Error;
use std::io::ErrorKind;
use std::{fs, io};

pub fn parse_rdf_file(
    path: &str,
) -> Result<Vec<(String, String, String, Option<String>)>, Box<dyn Error>> {
    let data = fs::read(path)?;

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

    let parser = RdfParser::from_format(format);
    let mut quads_vec = Vec::new();

    for quad_result in parser.for_reader(&data[..]) {
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
