use clap::Parser;

use super::MultiInputOptions;

#[derive(Debug, Parser)]
pub struct PrefetchArgs {
    #[clap(flatten)]
    pub input: MultiInputOptions,

    /// Path to write the .sra file to
    ///
    /// default: './<accession>.sra'
    #[clap(short, long, help_heading = "OUTPUT OPTIONS")]
    pub output: Option<String>,
}
