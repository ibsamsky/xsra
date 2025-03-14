use std::fs::File;
use std::io::{stdout, BufWriter, Write};

use anyhow::Result;
use clap::ValueEnum;
use gzp::deflate::{Bgzf, Gzip};
use gzp::par::compress::{ParCompress, ParCompressBuilder};
use zstd::Encoder;

use crate::cli::OutputFormat;

use super::BUFFER_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub enum Compression {
    #[clap(name = "u")]
    Uncompressed,
    #[clap(name = "g")]
    Gzip,
    #[clap(name = "b")]
    Bgzip,
    #[clap(name = "z")]
    Zstd,
}
impl Compression {
    pub fn ext(&self) -> Option<&str> {
        match self {
            Compression::Uncompressed => None,
            Compression::Gzip => Some("gz"),
            Compression::Bgzip => Some("bgz"),
            Compression::Zstd => Some("zst"),
        }
    }
}

fn writer_from_path(path: Option<&str>) -> Result<Box<dyn Write + Send>> {
    if let Some(path) = path {
        let file = File::create(path)?;
        let writer = BufWriter::with_capacity(BUFFER_SIZE, file);
        Ok(Box::new(writer))
    } else {
        let writer = BufWriter::with_capacity(BUFFER_SIZE, stdout());
        Ok(Box::new(writer))
    }
}

fn compression_passthrough<W: Write + Send + 'static>(
    writer: W,
    compression: Compression,
    num_threads: usize,
) -> Result<Box<dyn Write + Send>> {
    match compression {
        Compression::Uncompressed => Ok(Box::new(writer)),
        Compression::Gzip => {
            let pt: ParCompress<Gzip> = ParCompressBuilder::default()
                .num_threads(num_threads)?
                .from_writer(writer);
            Ok(Box::new(pt))
        }
        Compression::Bgzip => {
            let pt: ParCompress<Bgzf> = ParCompressBuilder::default()
                .num_threads(num_threads)?
                .from_writer(writer);
            Ok(Box::new(pt))
        }
        Compression::Zstd => {
            let mut pt = Encoder::new(writer, 3)?;
            pt.multithread(num_threads as u32)?;
            Ok(Box::new(pt.auto_finish()))
        }
    }
}

pub fn build_path_name(
    outdir: &str,
    prefix: &str,
    compression: Compression,
    format: OutputFormat,
    seg_id: usize,
) -> String {
    let format_ext = format.ext();
    if let Some(comp_ext) = compression.ext() {
        format!("{outdir}/{prefix}{seg_id}.{format_ext}.{comp_ext}")
    } else {
        format!("{outdir}/{prefix}{seg_id}.{format_ext}")
    }
}

pub fn build_writers(
    outdir: Option<&str>,
    prefix: &str,
    compression: Compression,
    format: OutputFormat,
    num_threads: usize,
) -> Result<Vec<Box<dyn Write + Send>>> {
    if let Some(outdir) = outdir {
        // create directory if it doesn't exist
        if !std::path::Path::new(outdir).exists() {
            std::fs::create_dir(outdir)?;
        }

        let c_threads = num_threads / 4;
        let mut writers = vec![];
        for i in 0..4 {
            let path = build_path_name(outdir, prefix, compression, format, i);
            let writer = writer_from_path(Some(&path))?;
            let writer = compression_passthrough(writer, compression, c_threads)?;
            writers.push(writer);
        }
        Ok(writers)
    } else {
        let mut writers = vec![];
        let writer = writer_from_path(None)?;
        let writer = compression_passthrough(writer, compression, num_threads)?;
        writers.push(writer);
        Ok(writers)
    }
}

pub fn build_local_buffers<T>(global_writer: &[T]) -> Vec<Vec<u8>> {
    let num_buffers = global_writer.len();
    let buffers = vec![Vec::with_capacity(BUFFER_SIZE); num_buffers];
    buffers
}
