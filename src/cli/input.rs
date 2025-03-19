use clap::Parser;

#[derive(Debug, Parser)]
#[clap(next_help_heading = "INPUT OPTIONS")]
pub struct InputOptions {
    /// SRA accession or path to discrete SRA file or Directory
    #[clap(name = "SRA accession", required = true)]
    pub accession: String,
}
