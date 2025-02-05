use anyhow::Result;
use clap::Parser;

mod cli;
mod fastq_dump;
mod output;
mod summary;
pub mod utils;

use cli::Arguments;
use fastq_dump::launch_threads;
use summary::get_num_spots;
use utils::open_table;

const BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer
const RECORD_CAPACITY: usize = 512;

fn main() -> Result<()> {
    let args = Arguments::parse();

    // Get number of rows in SRA file
    let (first_row_id, row_count) = get_num_spots(&args.sra_file)?;

    // Set the maximum number of rows to process
    let row_count = if let Some(limit) = args.limit {
        row_count.min(limit)
    } else {
        row_count
    };

    // Launch threads
    launch_threads(
        &args.sra_file,
        args.threads(),
        first_row_id,
        row_count,
        args.min_read_len,
        args.skip_technical,
        args.split,
        &args.outdir,
        &args.prefix,
        args.compression,
    )?;

    Ok(())
}
