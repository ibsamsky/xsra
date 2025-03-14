use clap::Parser;

#[derive(Debug, Parser)]
#[clap(next_help_heading = "INPUT OPTIONS")]
pub struct InputOptions {
    /// Path to the SRA file or directory
    #[clap(name = "SRA file", required = true)]
    pub sra_file: String,
}
