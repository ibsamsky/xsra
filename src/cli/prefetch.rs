use clap::Parser;

#[derive(Debug, Parser)]
pub struct PrefetchArgs {
    #[clap(required = true)]
    pub accession: String,

    /// Path to write the .sra file to
    ///
    /// default: './<accession>.sra'
    #[clap(short, long)]
    pub output: Option<String>,
}
