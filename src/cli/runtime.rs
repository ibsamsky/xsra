use clap::Parser;

#[derive(Parser, Debug)]
#[clap(next_help_heading = "RUNTIME OPTIONS")]
pub struct RuntimeOptions {
    /// Number of threads to use
    ///
    /// [0: all available cores]
    #[clap(short = 'T', long, default_value_t = 1)]
    threads: u64,
}
impl RuntimeOptions {
    pub fn threads(&self) -> u64 {
        if self.threads == 0 {
            num_cpus::get() as u64
        } else {
            self.threads.min(num_cpus::get() as u64)
        }
    }
}
