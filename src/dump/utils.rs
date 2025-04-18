use std::io::Write;

use anyhow::{bail, Result};
use ncbi_vdb::Segment;

use crate::cli::OutputFormat;

pub fn write_segment_to_buffer_set(
    buffers: &mut [Vec<u8>],
    segment: &Segment<'_>,
    format: OutputFormat,
) -> Result<()> {
    if buffers.len() == 1 {
        // Interleaved output - single output handle
        let buffer = &mut buffers[0];
        match format {
            OutputFormat::Fasta => write_fasta(buffer, segment),
            OutputFormat::Fastq => write_fastq(buffer, segment),
        }
    } else {
        if segment.sid() >= buffers.len() {
            bail!(
                "Provided Segment ID: {} is above the expected 4-segment counts",
                segment.sid()
            );
        }
        let buffer = &mut buffers[segment.sid()];
        match format {
            OutputFormat::Fasta => write_fasta(buffer, segment),
            OutputFormat::Fastq => write_fastq(buffer, segment),
        }
    }
}

pub fn write_fastq<W: Write>(wtr: &mut W, segment: &Segment<'_>) -> Result<()> {
    writeln!(wtr, "@{}.{}", segment.rid(), segment.sid())?;
    wtr.write_all(segment.seq())?;
    writeln!(wtr, "\n+")?;
    wtr.write_all(segment.qual())?;
    writeln!(wtr)?;
    Ok(())
}

pub fn write_fasta<W: Write>(wtr: &mut W, segment: &Segment<'_>) -> Result<()> {
    writeln!(wtr, ">{}.{}", segment.rid(), segment.sid())?;
    wtr.write_all(segment.seq())?;
    writeln!(wtr)?;
    Ok(())
}
