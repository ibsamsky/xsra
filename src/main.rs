use anyhow::Result;

mod cli;
mod describe;
mod dump;
mod output;
mod prefetch;

use clap::Parser;
use cli::Cli;
use describe::describe;
use dump::dump;
use prefetch::prefetch;

pub const BUFFER_SIZE: usize = 1024 * 1024;
pub const RECORD_CAPACITY: usize = 512;

fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        cli::Command::Dump(args) => dump(
            &args.input.sra_file,
            args.threads() as u64,
            &args.output,
            args.filter,
        ),
        cli::Command::Describe(args) => describe(&args.input.sra_file, args.options),
        cli::Command::Prefetch(args) => prefetch(&args.accession, args.output.as_deref()),
    }
}
