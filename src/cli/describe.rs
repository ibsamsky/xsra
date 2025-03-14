use clap::Parser;

use super::input::InputOptions;

#[derive(Debug, Parser)]
pub struct DescribeArgs {
    #[clap(flatten)]
    pub input: InputOptions,

    #[clap(flatten)]
    pub options: DescribeOptions,
}

#[derive(Debug, Parser, Clone, Copy)]
pub struct DescribeOptions {
    /// Number of spots to describe
    #[clap(short = 'l', long, default_value = "100")]
    pub limit: usize,

    /// Number of spots to skip before describing
    #[clap(short = 's', long, default_value = "0")]
    pub skip: usize,
}
