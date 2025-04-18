use super::{FilterOptions, InputOptions};
use anyhow::{bail, Result};
use clap::Parser;

#[derive(Parser, Debug)]
pub struct RecodeArgs {
    #[clap(flatten)]
    pub input: InputOptions,

    #[clap(flatten)]
    pub filter: FilterOptions,

    #[clap(short = 'T', long, default_value_t = 1)]
    threads: u64,

    #[clap(flatten)]
    pub output: RecodeOutput,
}
impl RecodeArgs {
    pub fn validate(&self) -> Result<()> {
        match &self.filter.include.len() {
            0 => bail!(
                "Recoding requires specifying which spot segments to use (see help for commands)"
            ),
            1 | 2 => Ok(()),
            _ => bail!("Recoding can only use one or two spot segments"),
        }
    }

    pub fn threads(&self) -> u64 {
        if self.threads == 0 {
            num_cpus::get() as u64
        } else {
            self.threads.min(num_cpus::get() as u64)
        }
    }

    pub fn paired(&self) -> bool {
        self.filter.include.len() == 2
    }

    pub fn primary_sid(&self) -> usize {
        self.filter.include[0]
    }

    pub fn extended_sid(&self) -> Option<usize> {
        if self.paired() {
            Some(self.filter.include[1])
        } else {
            None
        }
    }
}

#[derive(Parser, Debug)]
pub struct RecodeOutput {
    /// BINSEQ output name (default: "output.{bq,vbq}")
    #[clap(short, long)]
    pub name: Option<String>,

    /// BINSEQ output flavor
    #[clap(short, long)]
    pub flavor: BinseqFlavor,
}
impl RecodeOutput {
    pub fn name(&self) -> String {
        if let Some(name) = &self.name {
            name.clone()
        } else {
            let ext = self.flavor.extension();
            format!("output.{}", ext)
        }
    }
}

#[derive(clap::ValueEnum, Clone, Copy, Debug)]
pub enum BinseqFlavor {
    #[clap(name = "b", help = "BINSEQ")]
    Binseq,
    #[clap(name = "v", help = "VBINSEQ")]
    VBinseq,
}
impl BinseqFlavor {
    pub fn extension(&self) -> &str {
        match self {
            BinseqFlavor::Binseq => "bq",
            BinseqFlavor::VBinseq => "vbq",
        }
    }
}
