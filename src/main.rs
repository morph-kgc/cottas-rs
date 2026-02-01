use clap::{Parser, Subcommand};
use std::error::Error;

use cottas_rs::*;

#[derive(Parser)]
#[command(name = "cottas")]
#[command(about = "COTTAS implementation and CLI written in Rust", version)]
struct Cli {
    #[command(subcommand)]
    command: Commands,
}

#[derive(Subcommand)]
enum Commands {
    /// Convert RDF to COTTAS
    Rdf2Cottas {
        rdf_file_path: String,
        cottas_file_path: String,
        index: String,
    },

    /// Convert COTTAS to RDF
    Cottas2Rdf {
        cottas_file_path: String,
        rdf_file_path: String,
    },

    /// Search in a COTTAS file
    Search {
        cottas_file_path: String,
        triple_pattern: String,
    },

    /// Concatenate multiple COTTAS files
    Cat {
        #[arg(required = true)]
        cottas_file_paths: Vec<String>,

        #[arg(short, long)]
        output: String,

        #[arg(short, long)]
        index: Option<String>,

        #[arg(long)]
        remove_input_files: bool,
    },

    /// Diff two COTTAS files
    Diff {
        cottas_file_1_path: String,
        cottas_file_2_path: String,

        #[arg(short, long)]
        output: String,

        #[arg(short, long)]
        index: Option<String>,

        #[arg(long)]
        remove_input_files: bool,
    },

    /// Show info about a COTTAS file
    Info { cottas_file_path: String },

    /// Verify a COTTAS file
    Verify { cottas_file_path: String },
}

fn main() -> Result<(), Box<dyn Error>> {
    let cli = Cli::parse();

    match cli.command {
        Commands::Rdf2Cottas {
            rdf_file_path,
            cottas_file_path,
            index,
        } => {
            rdf2_cottas(&rdf_file_path, &cottas_file_path, &index)?;
        }

        Commands::Cottas2Rdf {
            cottas_file_path,
            rdf_file_path,
        } => {
            cottas2_rdf(&cottas_file_path, &rdf_file_path)?;
        }

        Commands::Search {
            cottas_file_path,
            triple_pattern,
        } => {
            let results = search(&cottas_file_path, &triple_pattern)?;
            for row in results {
                println!("{}", row.join(" "));
            }
        }

        Commands::Cat {
            cottas_file_paths,
            output,
            index,
            remove_input_files,
        } => {
            cat(
                &cottas_file_paths,
                &output,
                index.as_deref(),
                Some(remove_input_files),
            )?;
        }

        Commands::Diff {
            cottas_file_1_path,
            cottas_file_2_path,
            output,
            index,
            remove_input_files,
        } => {
            diff(
                &cottas_file_1_path,
                &cottas_file_2_path,
                &output,
                index.as_deref(),
                Some(remove_input_files),
            )?;
        }

        Commands::Info { cottas_file_path } => {
            let info = info(&cottas_file_path)?;
            println!("{info:#?}");
        }

        Commands::Verify { cottas_file_path } => {
            let valid = verify(&cottas_file_path)?;
            println!("{valid}");
        }
    }

    Ok(())
}
