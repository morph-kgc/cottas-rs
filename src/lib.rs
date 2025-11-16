pub fn rdf2cottas() -> &'static str {
    "Hello from cottas-rs!"
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_rdf2cottas() {
        assert_eq!(rdf2cottas(), "Hello from cottas-rs!");
    }
}
