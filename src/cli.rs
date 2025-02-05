use clap::Parser;

#[derive(Debug, Parser)]
pub struct Arguments {
    /// Path to the SRA file or directory
    #[clap(name = "SRA file", required = true)]
    pub sra_file: String,

    /// Number of threads to use
    /// 0: use all available cores
    #[clap(short = 'T', long, default_value = "0")]
    threads: usize,

    /// Minimum read length to include
    /// 0: include all reads
    #[clap(short = 'L', long, default_value = "1")]
    pub min_read_len: u32,

    /// Skip technical reads
    ///
    /// Default: include all reads
    #[clap(short = 't', long)]
    pub skip_technical: bool,

    /// Split read segments to separate files
    ///
    /// Default will output interleaved reads to stdout
    #[clap(short = 's', long)]
    pub split: bool,

    /// Output directory
    ///
    /// Only used when splitting read segments to separate files
    #[clap(short = 'o', long, default_value = "output")]
    pub outdir: String,
}
impl Arguments {
    pub fn threads(&self) -> usize {
        if self.threads == 0 {
            num_cpus::get()
        } else {
            self.threads.min(num_cpus::get())
        }
    }
}
