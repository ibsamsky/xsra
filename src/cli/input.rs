use std::fmt::Display;

use clap::{Parser, ValueEnum};

#[derive(Debug, Parser)]
#[clap(next_help_heading = "INPUT OPTIONS")]
pub struct InputOptions {
    /// SRA accession or path to discrete SRA file or Directory
    #[clap(name = "SRA accession", required = true)]
    pub accession: String,

    /// Only download an SRA with complete quality scores
    ///
    /// Default: lite
    #[clap(short = 'Q', long)]
    pub full_quality: bool,

    /// URL provider
    #[clap(short = 'P', long, default_value = "https")]
    pub provider: Provider,
}

#[derive(Debug, Clone, Copy, ValueEnum)]
pub enum Provider {
    Https,
    Gcp,
    Aws,
}
impl Display for Provider {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Https => f.write_str("https"),
            Self::Gcp => f.write_str("gcp"),
            Self::Aws => f.write_str("aws"),
        }
    }
}
impl Provider {
    pub fn url_prefix(&self) -> &str {
        match self {
            Self::Https => "https://",
            Self::Gcp => "gs://",
            Self::Aws => "s3://",
        }
    }
}
