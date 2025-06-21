use anyhow::Result;

mod cli;
mod describe;
mod dump;
mod output;
mod prefetch;
mod recode;
mod utils;

use clap::Parser;
use cli::Cli;
use describe::describe;
use dump::dump;
use prefetch::prefetch;
use recode::recode;

pub const BUFFER_SIZE: usize = 1024 * 1024;
pub const RECORD_CAPACITY: usize = 1024;

fn main() -> Result<()> {
    let args = Cli::parse();
    match args.command {
        cli::Command::Dump(args) => dump(
            &args.input,
            args.runtime.threads(),
            &args.output,
            args.filter,
        ),
        cli::Command::Recode(args) => recode(&args),
        cli::Command::Describe(args) => describe(&args.input, &args.options),
        cli::Command::Prefetch(args) => {
            // Async is overkill for other commands,
            // only prefetch gets full treatment
            let runtime = tokio::runtime::Runtime::new()?;
            runtime.block_on(prefetch(&args.input, args.output.as_deref()))
        }
    }
}
