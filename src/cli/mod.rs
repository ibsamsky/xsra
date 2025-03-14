use clap::{
    builder::{
        styling::{AnsiColor, Effects},
        Styles,
    },
    Parser, Subcommand,
};

mod describe;
mod fastq;
mod filter;
mod input;
pub use describe::{DescribeArgs, DescribeOptions};
pub use fastq::{FastqArgs, FastqOutput};
pub use filter::FilterOptions;

// Configures Clap v3-style help menu colors
const STYLES: Styles = Styles::styled()
    .header(AnsiColor::Green.on_default().effects(Effects::BOLD))
    .usage(AnsiColor::Green.on_default().effects(Effects::BOLD))
    .literal(AnsiColor::Cyan.on_default().effects(Effects::BOLD))
    .placeholder(AnsiColor::Cyan.on_default());

#[derive(Parser, Debug)]
#[command(styles = STYLES)]
pub struct Cli {
    #[clap(subcommand)]
    pub command: Command,
}

#[derive(Debug, Subcommand)]
pub enum Command {
    /// Extract the records of the SRA file and output them as FASTQ
    #[clap(name = "fastq")]
    Fastq(FastqArgs),

    /// Describe the read segments in the SRA file within a specified limit
    #[clap(name = "describe")]
    Describe(DescribeArgs),
}
