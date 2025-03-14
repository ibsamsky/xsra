use std::io::Write;

use anyhow::Result;
use ncbi_vdb::SegmentType;
use serde::{Serialize, Serializer};

#[derive(Debug)]
pub struct SegmentTypeWrapper(SegmentType);
impl std::fmt::Display for SegmentTypeWrapper {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.0 {
            SegmentType::Technical => write!(f, "Technical"),
            SegmentType::Biological => write!(f, "Biological"),
        }
    }
}
impl Serialize for SegmentTypeWrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        // Use the Display implementation we already have to convert to a string
        serializer.serialize_str(&self.to_string())
    }
}
impl From<SegmentType> for SegmentTypeWrapper {
    fn from(ty: SegmentType) -> Self {
        Self(ty)
    }
}

#[derive(Debug, Serialize)]
pub struct SegmentStats {
    sid: usize,
    segment_type: SegmentTypeWrapper,
    mean_length: f64,
    mean_quality: f64,
}
impl SegmentStats {
    pub fn new(sid: usize, segment_type: SegmentType, mean_length: f64, mean_quality: f64) -> Self {
        Self {
            sid,
            segment_type: segment_type.into(),
            mean_length,
            mean_quality,
        }
    }
}

#[derive(Debug, Serialize)]
pub struct DescribeStats {
    total_spots: usize,
    processed_spots: usize,
    spot_range: (usize, usize),
    stats: Vec<SegmentStats>,
}
impl DescribeStats {
    pub fn new(
        segment_types: Vec<SegmentType>,
        segment_lengths: Vec<Vec<f64>>,
        segment_qualities: Vec<Vec<f64>>,
        processed_spots: usize,
        first_spot: usize,
        last_spot: usize,
        total_spots: usize,
    ) -> Self {
        let num_segments = segment_types.len();
        let average_segment_lengths = Self::average(segment_lengths);
        let average_segment_qualities = Self::average(segment_qualities);
        let stats = (0..num_segments)
            .map(|idx| {
                SegmentStats::new(
                    idx + 1,
                    segment_types[idx],
                    average_segment_lengths[idx],
                    average_segment_qualities[idx],
                )
            })
            .collect();
        Self {
            total_spots,
            processed_spots,
            spot_range: (first_spot, last_spot),
            stats,
        }
    }

    fn average(vec: Vec<Vec<f64>>) -> Vec<f64> {
        vec.iter()
            .map(|lens| {
                if lens.is_empty() {
                    0.0
                } else {
                    lens.iter().sum::<f64>() / lens.len() as f64
                }
            })
            .collect()
    }

    pub fn pprint<W: Write>(&self, wtr: &mut W) -> Result<()> {
        serde_json::to_writer_pretty(wtr, self)?;
        Ok(())
    }
}
