use libc::mode_t;
use std::ffi::OsStr;
use std::fmt;
use std::fs::File;
use std::io::{stdout, BufWriter, Write};

use anyhow::Result;
use clap::ValueEnum;
use gzp::deflate::{Bgzf, Gzip};
use gzp::par::compress::{ParCompress, ParCompressBuilder};
use zstd::Encoder;

use crate::cli::OutputFormat;
use std::{ffi::CString, io, path::Path};

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
pub enum OutputType<'a> {
    RegularFile(&'a str),
    NamedPipe(&'a str),
    StdIO,
}

impl fmt::Display for OutputType<'_> {
    // This trait requires `fmt` with this exact signature.
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            Self::StdIO => write!(f, "stdout"),
            Self::RegularFile(fname) => write!(f, "{}", fname),
            Self::NamedPipe(fname) => write!(f, "{}", fname),
        }
    }
}

impl<'a> OutputType<'a> {
    fn sep(&self) -> &str {
        match self {
            OutputType::RegularFile(_) => "/",
            OutputType::NamedPipe(_) => ".",
            OutputType::StdIO => unreachable!("should not happen"),
        }
    }
}

// From : https://github.com/kotauskas/interprocess/blob/main/src/os/unix/fifo_file.rs
/// Creates a FIFO file at the specified path with the specified permissions.
///
/// Since the `mode` parameter is masked with the [`umask`], it's best to leave it at `0o777` unless
/// a different value is desired.
///
/// ## System calls
/// - [`mkfifo`]
///
/// [`mkfifo`]: https://pubs.opengroup.org/onlinepubs/9699919799/utilities/mkfifo.html
/// [`umask`]: https://en.wikipedia.org/wiki/Umask
pub fn create_fifo<P: AsRef<Path>>(path: P, mode: mode_t) -> io::Result<()> {
    _create_fifo(path.as_ref(), mode)
}
fn _create_fifo(path: &Path, mode: mode_t) -> io::Result<()> {
    let path = CString::new(path.as_os_str().as_encoded_bytes())?;
    let res = unsafe { libc::mkfifo(path.as_bytes_with_nul().as_ptr().cast(), mode) != -1 };
    if res {
        Ok(())
    } else {
        Err(io::Error::last_os_error())
    }
}

fn writer_from_path(path: OutputType) -> Result<Box<dyn Write + Send>> {
    match path {
        OutputType::RegularFile(path) => {
            let file = File::create(path)?;
            let writer = BufWriter::with_capacity(BUFFER_SIZE, file);
            Ok(Box::new(writer))
        }
        OutputType::StdIO => {
            let writer = BufWriter::with_capacity(BUFFER_SIZE, stdout());
            Ok(Box::new(writer))
        }
        OutputType::NamedPipe(path) => {
            unimplemented!("not yet")
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
    outdir: OutputType,
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
        for i in 0..4 {
            let path = build_path_name(
                OutputType::RegularFile(&outdir),
                prefix,
                compression,
                format,
                i,
            );
            let writer = writer_from_path(OutputType::RegularFile(&path))?;
            let writer = compression_passthrough(writer, compression, c_threads)?;
            writers.push(writer);
        }
        Ok(writers)
    } else {
        let mut writers = vec![];
        let writer = writer_from_path(OutputType::StdIO)?;
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
