<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/cottas-rdf/.github/main/logo/logo_full_white.png">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/cottas-rdf/.github/main/logo/logo_full.png">
    <img alt="COTTAS"  height="120" src="[https://raw.githubusercontent.com/cottas-rdf/.github/main/logo/logo_full.png](https://raw.githubusercontent.com/cottas-rdf/.github/main/logo/logo_full.png)">
  </picture>
</p>

**cottas-rs** is a library for working with **compressed** **[RDF](https://www.w3.org/TR/rdf11-concepts/)** files in the **COTTAS** format. COTTAS stores triples as a triple table in [Apache Parquet](https://parquet.apache.org/). It is built on top of [DuckDB](https://duckdb.org/) and provides an [HDT](https://www.rdfhdt.org/)-like interface.

## Features 🚀

- **Compression** and **decompression** of RDF files.
- Querying COTTAS files with **[triple patterns](https://www.w3.org/TR/sparql11-query/#sparqlTriplePatterns)**.
- [RDFLib](https://github.com/RDFLib/rdflib) store backend for querying COTTAS files with **[SPARQL](https://www.w3.org/TR/sparql11-query/)**.
- Supports [RDF datasets](https://www.w3.org/TR/rdf11-concepts/#section-dataset) (**quads**).
- Can be used as a **library** or via **command line**.

## Documentation 📚

The documentation for **cottas-rs** can be found **[here](https://docs.rs/cottas-rs/latest/cottas_rs/)**.

## Getting Started 🚀

Install the Rust crate using cargo:
```bash
cargo install cottas-rs
```

To install Rust and Cargo check the official [website](https://www.rust-lang.org/tools/install).

```rust
// basic example
```

## License 🔓

**cottas-rs** is available under the **[Apache License 2.0](https://github.com/morph-kgc/cottas-rs/blob/main/LICENSE)**.

## Author & Contact 📬

- **[Stephanya Casanova-Marroquin](https://github.com/savacano28/) - [stephanya.casanova@gmail.com](mailto:stephanya.casanova@gmail.com)**
- **[Julián Arenas-Guerrero](https://github.com/arenas-guerrero-julian/) - [julian.arenas.guerrero@upm.es](mailto:julian.arenas.guerrero@upm.es)**

*[Universidad Politécnica de Madrid](https://www.upm.es/internacional)*.

## Citing :speech_balloon:

If you used pycottas in your work, please cite the **[ISWC paper](https://oa.upm.es/91920/1/arenas2026cottas.pdf)** and the GitHub repository:

```bib
@inproceedings{arenas2026cottas,
  title     = {{COTTAS: Columnar Triple Table Storage for Efficient and Compressed RDF Management}},
  author    = {Arenas-Guerrero, Julián and Ferrada, Sebastián},
  booktitle = {Proceedings of the 24th International Semantic Web Conference},
  year      = {2026},
  publisher = {Springer Nature Switzerland},
  isbn      = {978-3-032-09530-5},
  pages     = {313--331},
  doi       = {10.1007/978-3-032-09530-5_18},
}

@misc{cottas-rs,
  author       = {Casanova-Marroquin, Stephanya},
  title        = {cottas-rs},
  year         = {2026},
  howpublished = {\url{https://github.com/cottas-rdf/cottas-rs}},
}
```
