use std::fs::File;
use std::io::{stdout, BufWriter, Write};

use anyhow::{bail, Result};

use super::BUFFER_SIZE;

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

pub fn build_writers(outdir: Option<&str>) -> Result<Vec<Box<dyn Write + Send>>> {
    if let Some(outdir) = outdir {
        // create directory if it doesn't exist
        if !std::path::Path::new(outdir).exists() {
            std::fs::create_dir(outdir)?;
        }

        let mut writers = vec![];
        for i in 0..4 {
            let path = format!("{}/{}.fastq", outdir, i);
            let writer = writer_from_path(Some(&path))?;
            writers.push(writer);
        }
        Ok(writers)
    } else {
        let mut writers = vec![];
        let writer = writer_from_path(None)?;
        writers.push(writer);
        Ok(writers)
    }
}

pub fn build_local_buffers<T>(global_writer: &[T]) -> Vec<Vec<u8>> {
    let num_buffers = global_writer.len();
    let buffers = vec![vec![0; BUFFER_SIZE]; num_buffers];
    buffers
}

pub fn write_to_buffer_set(
    buffers: &mut [Vec<u8>],
    row_id: i64,
    seg_id: usize,
    seq: &[u8],
    qual: &[u8],
) -> Result<()> {
    if buffers.len() == 1 {
        // Interleaved output - single output handle
        let buffer = &mut buffers[0];
        write_to_buffer(buffer, row_id, seg_id, seq, qual)?;
    } else {
        if seg_id >= buffers.len() {
            bail!(
                "Provided Segment ID: {} is above the expected 4-segment counts",
                seg_id
            );
        }
        // Multiple output handles
        let buffer = &mut buffers[seg_id];
        write_to_buffer(buffer, row_id, seg_id, seq, qual)?;
    }

    Ok(())
}

fn write_to_buffer(
    buffer: &mut Vec<u8>,
    row_id: i64,
    seg_id: usize,
    seq: &[u8],
    qual: &[u8],
) -> Result<()> {
    writeln!(buffer, "@{}:{}", row_id, seg_id)?;
    buffer.extend_from_slice(seq);
    writeln!(buffer, "\n+")?;
    buffer.extend_from_slice(qual);
    writeln!(buffer)?;
    Ok(())
}
