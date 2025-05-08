use std::fmt;
use std::fs::File;
use std::io::{stdout, BufWriter, Write};

use anyhow::{bail, Result};
use clap::ValueEnum;
use gzp::deflate::{Bgzf, Gzip};
use gzp::par::compress::{ParCompress, ParCompressBuilder};
use std::process::Command;
use zstd::Encoder;

use crate::cli::FilterOptions;
use crate::cli::OutputFormat;
#[cfg(target_family = "unix")]
use std::os::unix::fs::FileTypeExt;

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

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OutputFileType<'a> {
    RegularFile(&'a str),
    NamedPipe(&'a str),
    StdOut,
}

impl fmt::Display for OutputFileType<'_> {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::StdOut => write!(f, "stdout"),
            Self::RegularFile(fname) => write!(f, "{}", fname),
            Self::NamedPipe(fname) => write!(f, "{}", fname),
        }
    }
}

impl OutputFileType<'_> {
    fn sep(&self) -> &str {
        match self {
            OutputFileType::RegularFile(_) => "/",
            OutputFileType::NamedPipe(_) => ".",
            OutputFileType::StdOut => unreachable!("should not happen"),
        }
    }
}

fn create_fifo_if_absent(path: OutputFileType) -> Result<()> {
    match path {
        OutputFileType::NamedPipe(path) => {
            // check if the file already exists and IS a fifo; if so
            // then open it for writing, otherwise make it.
            let fifo_exists = if std::fs::exists(path)? {
                let minfo = std::fs::metadata(path)?;
                if cfg!(target_family = "unix") {
                    if minfo.file_type().is_fifo() {
                        eprintln!(
                            "The path {} already existed as is a fifo, so using that for communication.",
                            path
                        );
                        true
                    } else {
                        // the file existed but wasn't a fifo
                        bail!("The file {} existed already, but wasn't a fifo, so it can't be used as a named pipe. Please remove the file or provide a named pipe instead.", path);
                    }
                } else {
                    // the file existed but wasn't a fifo
                    bail!(
                        "Named pipes are not supported on non-unix (i.e. non linux/MacOS) systems."
                    );
                }
            } else {
                false
            };

            if !fifo_exists {
                if cfg!(target_family = "unix") {
                    let status = Command::new("mkfifo").arg(path).status()?;
                    if !status.success() {
                        bail!("`mkfifo` command failed with exit status {:#?}", status);
                    }
                    //create_fifo(path, 0o644)?;
                } else {
                    bail!(
                        "Named pipes are not supported on non-unix (i.e. non linux/MacOS) systems."
                    );
                }
            }
        }
        _ => {
            bail!("`create_fifo_if_absent` should not be called for a non-fifo output!");
        }
    }
    Ok(())
}

fn writer_from_path(path: OutputFileType) -> Result<Box<dyn Write + Send>> {
    match path {
        OutputFileType::RegularFile(path) => {
            let file = File::create(path)?;
            let writer = BufWriter::with_capacity(BUFFER_SIZE, file);
            Ok(Box::new(writer))
        }
        OutputFileType::StdOut => {
            let writer = BufWriter::with_capacity(BUFFER_SIZE, stdout());
            Ok(Box::new(writer))
        }
        OutputFileType::NamedPipe(path) => {
            let file = std::fs::OpenOptions::new().write(true).open(path)?;
            let writer = BufWriter::with_capacity(BUFFER_SIZE, file);
            Ok(Box::new(writer))
        }
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
    outdir: OutputFileType,
    prefix: &str,
    compression: Compression,
    format: OutputFormat,
    seg_id: usize,
) -> String {
    let out_sep = outdir.sep();
    let format_ext = format.ext();
    if let Some(comp_ext) = compression.ext() {
        format!("{outdir}{out_sep}{prefix}{seg_id}.{format_ext}.{comp_ext}")
    } else {
        format!("{outdir}{out_sep}{prefix}{seg_id}.{format_ext}")
    }
}

pub fn build_writers(
    outdir: Option<(&str, bool)>,
    prefix: &str,
    compression: Compression,
    format: OutputFormat,
    num_threads: usize,
    filter_opts: &FilterOptions,
) -> Result<Vec<Box<dyn Write + Send>>> {
    if let Some((outdir, is_named_pipe)) = outdir {
        // create directory if it doesn't exist
        if !std::path::Path::new(outdir).exists() && !is_named_pipe {
            std::fs::create_dir(outdir)?;
        }

        // If four or more threads were allocated to `xsra`, use that number divided by four for
        // compression. If fewer than four total threads were allocated, just set aside one thread.
        let c_threads = (num_threads / 4).max(1);
        let mut writers = vec![];
        if is_named_pipe {
            for i in 0..4 {
                if filter_opts.include.is_empty() || filter_opts.include.contains(&i) {
                    let path = build_path_name(
                        OutputFileType::NamedPipe(outdir),
                        prefix,
                        compression,
                        format,
                        i,
                    );
                    create_fifo_if_absent(OutputFileType::NamedPipe(&path))?;
                }
            }
        }

        for i in 0..4 {
            let outf = |x| {
                if is_named_pipe {
                    OutputFileType::NamedPipe(x)
                } else {
                    OutputFileType::RegularFile(x)
                }
            };
            // only create actual writers if we won't filter out this segment anyway
            if filter_opts.include.is_empty() || filter_opts.include.contains(&i) {
                let path = build_path_name(outf(outdir), prefix, compression, format, i);
                let writer = writer_from_path(outf(&path))?;
                let writer = compression_passthrough(writer, compression, c_threads)?;
                writers.push(writer);
            } else {
                // otherwise, use the empty writer
                let empty_writer = Box::new(std::io::empty());
                writers.push(empty_writer);
            }
        }
        Ok(writers)
    } else {
        let mut writers = vec![];
        let writer = writer_from_path(OutputFileType::StdOut)?;
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
