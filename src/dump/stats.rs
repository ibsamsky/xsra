use std::io::Result;
use std::io::Write;
use std::ops::Add;

#[derive(Debug, Clone)]
pub struct ProcessStatistics {
    pub num_spots: u64,
    pub num_reads: u64,
    /// Number of written reads per segment
    pub reads_per_segment: Vec<u64>,
    /// Number of reads filtered by size by segment
    pub filter_size: Vec<u64>,
    /// Number of reads filtered by biological/technical type by segment
    pub filter_type: Vec<u64>,
}
impl Default for ProcessStatistics {
    fn default() -> Self {
        Self {
            num_spots: 0,
            num_reads: 0,
            reads_per_segment: vec![0; 4],
            filter_size: vec![0; 4],
            filter_type: vec![0; 4],
        }
    }
}
impl Add for ProcessStatistics {
    type Output = Self;

    fn add(mut self, other: Self) -> Self {
        let num_spots = self.num_spots + other.num_spots;
        let num_reads = self.num_reads + other.num_reads;

        // Resize vectors to match the longest one
        if self.reads_per_segment.len() < other.reads_per_segment.len() {
            self.reads_per_segment
                .resize(other.reads_per_segment.len(), 0);
        }
        if self.filter_size.len() < other.filter_size.len() {
            self.filter_size.resize(other.filter_size.len(), 0);
        }
        if self.filter_type.len() < other.filter_type.len() {
            self.filter_type.resize(other.filter_type.len(), 0);
        }

        // Sum vectors
        let reads_per_segment = self
            .reads_per_segment
            .iter()
            .zip(other.reads_per_segment.iter())
            .map(|(a, b)| a + b)
            .collect();
        let filter_size = self
            .filter_size
            .iter()
            .zip(other.filter_size.iter())
            .map(|(a, b)| a + b)
            .collect();
        let filter_type = self
            .filter_type
            .iter()
            .zip(other.filter_type.iter())
            .map(|(a, b)| a + b)
            .collect();

        ProcessStatistics {
            num_spots,
            num_reads,
            reads_per_segment,
            filter_size,
            filter_type,
        }
    }
}
impl ProcessStatistics {
    pub fn inc_spots(&mut self) {
        self.num_spots += 1;
    }
    pub fn inc_reads(&mut self, seg_id: usize) {
        self.num_reads += 1;
        if seg_id >= self.reads_per_segment.len() {
            self.reads_per_segment.resize(seg_id + 1, 0);
        }
        self.reads_per_segment[seg_id] += 1;
    }
    pub fn inc_filter_size(&mut self, seg_id: usize) {
        if seg_id >= self.filter_size.len() {
            self.filter_size.resize(seg_id + 1, 0);
        }
        self.filter_size[seg_id] += 1;
    }
    pub fn inc_filter_type(&mut self, seg_id: usize) {
        if seg_id >= self.filter_type.len() {
            self.filter_type.resize(seg_id + 1, 0);
        }
        self.filter_type[seg_id] += 1;
    }
    pub fn pprint<W: Write>(&self, wtr: &mut W) -> Result<()> {
        writeln!(wtr, "Number of spots processed: {}", self.num_spots)?;
        writeln!(wtr, "Number of reads written: {}", self.num_reads)?;

        if sum_slice(&self.reads_per_segment) > 0 {
            writeln!(wtr, "Reads written per segment:")?;
            for (i, &count) in self.reads_per_segment.iter().enumerate() {
                writeln!(wtr, "  Segment {}: {}", i, count)?;
            }
        }
        if sum_slice(&self.filter_size) > 0 {
            writeln!(wtr, "Filtered reads by size:")?;
            for (i, &count) in self.filter_size.iter().enumerate() {
                writeln!(wtr, "  Segment {}: {}", i, count)?;
            }
        }
        if sum_slice(&self.filter_type) > 0 {
            writeln!(wtr, "Filtered reads by type:")?;
            for (i, &count) in self.filter_type.iter().enumerate() {
                writeln!(wtr, "  Segment {}: {}", i, count)?;
            }
        }
        Ok(())
    }
}

fn sum_slice(vec: &[u64]) -> u64 {
    vec.iter().sum()
}
