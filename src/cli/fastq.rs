use crate::output::Compression;
use clap::Parser;

use super::filter::FilterOptions;
use super::input::InputOptions;

#[derive(Parser, Debug)]
pub struct FastqArgs {
    #[clap(flatten)]
    pub input: InputOptions,

    #[clap(flatten)]
    pub filter: FilterOptions,

    #[clap(flatten)]
    pub output: FastqOutput,

    #[clap(flatten)]
    runtime: FastqRuntime,
}
impl FastqArgs {
    pub fn threads(&self) -> usize {
        if self.runtime.threads == 0 {
            num_cpus::get()
        } else {
            self.runtime.threads.min(num_cpus::get())
        }
    }
}

#[derive(Parser, Debug)]
#[clap(next_help_heading = "OUTPUT OPTIONS")]
pub struct FastqOutput {
    /// Output directory
    ///
    /// Only used when splitting read segments to separate files
    #[clap(short = 'o', long, default_value = "output")]
    pub outdir: String,

    /// Split read segments to separate files
    ///
    /// Default will output interleaved reads to stdout
    #[clap(short = 's', long)]
    pub split: bool,

    /// Prefix for segment files
    ///
    /// Output will follow the pattern: <outdir>/<prefix><segment>.<ext>
    #[clap(short = 'p', long, default_value = "seg_")]
    pub prefix: String,

    /// Compress output files
    ///
    /// [uncompressed, gzip, bgzip, zstd]
    #[clap(short = 'c', long, default_value = "u")]
    pub compression: Compression,
}

#[derive(Parser, Debug)]
#[clap(next_help_heading = "RUNTIME OPTIONS")]
pub struct FastqRuntime {
    /// Number of threads to use
    ///
    /// [0: all available cores]
    #[clap(short = 'T', long, default_value = "8")]
    threads: usize,
}
