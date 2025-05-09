use clap::Parser;

#[derive(Debug, Parser)]
#[clap(next_help_heading = "FILTER OPTIONS")]
pub struct FilterOptions {
    /// Minimum segment read length to include
    /// 0: include all read segments (including zero-length segments)
    #[clap(short = 'L', long, default_value = "1")]
    #[clap(next_help_heading = "SPOT / SEGMENT OPTIONS")]
    pub min_read_len: usize,

    /// Skip technical reads
    ///
    /// Default: include all reads
    #[clap(short = 't', long)]
    pub skip_technical: bool,

    /// Only process up to N spots
    ///
    /// Note: this is not the number of reads, but the number of spots.
    /// If a spot has 4 read segments this will output 4xN reads.
    #[clap(short = 'l', long)]
    pub limit: Option<u64>,

    /// Only process specific segments
    ///
    /// Default: include all segments
    #[clap(short = 'I', long, num_args = 0.., value_delimiter = ',', required_if_eq("named_pipes", "true"))]
    pub include: Vec<usize>,
}
