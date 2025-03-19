use std::path::Path;

use anyhow::Result;
use ncbi_vdb::{SegmentType, SraReader};

use crate::{cli::DescribeOptions, prefetch::identify_url};

mod stats;
use stats::DescribeStats;

fn calculate_average_quality(qual: &[u8]) -> f64 {
    // PHRED33 has an ASCII offset of 33
    const PHRED33_OFFSET: u8 = 33;

    if qual.is_empty() {
        return 0.0;
    }

    let total_score: u32 = qual.iter().map(|byte| (byte - PHRED33_OFFSET) as u32).sum();

    total_score as f64 / qual.len() as f64
}

pub fn describe(sra_file: &str, opts: DescribeOptions) -> Result<()> {
    let sra_file = if !Path::new(sra_file).exists() {
        eprintln!("Identifying SRA data URL for Accession: {}", sra_file);
        let url = identify_url(sra_file)?;
        eprintln!("Streaming SRA records from URL: {}", url);
        url
    } else {
        sra_file.to_string()
    };
    let reader = SraReader::new(&sra_file)?;
    let num_spots = reader.stop();

    let l_bound = opts.skip.max(1);
    let r_bound = (l_bound + opts.limit).min(num_spots as usize);

    let mut num_segments = 0;
    let mut segment_types = Vec::new();
    let mut segment_lengths = Vec::new();
    let mut segment_qualities = Vec::new();
    for record in reader.into_range_iter(l_bound as i64, r_bound as u64)? {
        let record = record?;
        for segment in record.into_iter() {
            // calculate mean quality
            let mean_quality = calculate_average_quality(segment.qual());

            // update segment stats
            num_segments = num_segments.max(segment.sid());

            if segment_types.len() <= segment.sid() {
                segment_types.resize(segment.sid() + 1, SegmentType::Technical);
                segment_lengths.resize(segment.sid() + 1, Vec::new());
                segment_qualities.resize(segment.sid() + 1, Vec::new());
            }
            segment_types[segment.sid()] = segment.ty();
            segment_lengths[segment.sid()].push(segment.len() as f64);
            segment_qualities[segment.sid()].push(mean_quality);
        }
    }

    let stats = DescribeStats::new(
        segment_types,
        segment_lengths,
        segment_qualities,
        r_bound - l_bound,
        l_bound,
        r_bound,
        num_spots as usize,
    );
    stats.pprint(&mut std::io::stdout())?;

    Ok(())
}
