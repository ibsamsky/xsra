use anyhow::Result;

mod cli;
mod describe;
mod dump;
mod output;

use clap::Parser;
use cli::Cli;
use describe::describe;
use dump::fastq_dump;

pub const BUFFER_SIZE: usize = 1024 * 1024;
pub const RECORD_CAPACITY: usize = 512;

fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        cli::Command::Fastq(args) => fastq_dump(
            &args.input.sra_file,
            args.threads() as u64,
            &args.output,
            args.filter,
        ),
        cli::Command::Describe(args) => describe(&args.input.sra_file, args.options),
    }
}
