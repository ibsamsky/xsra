use std::io::Write;
use std::sync::Arc;
use std::thread;
use std::{ffi::CString, ops::Add};

use anyhow::{bail, Result};
use parking_lot::Mutex;

use crate::output::{build_local_buffers, build_writers, write_to_buffer_set};

use super::{open_table, BUFFER_SIZE, RECORD_CAPACITY};
use xsra::{
    is_column_present, SafeKDirectory, SafeVCursor, SafeVDBManager, SafeVSchema, SafeVTable,
    VCursorAddColumn, VCursorCellDataDirect, VCursorOpen, VDBManagerMakeSchema,
    VTableCreateCachedCursorRead,
};

/// Column indices used when reading data from a table
#[derive(Debug)]
pub struct ColumnIndices {
    pub seq: u32,
    pub qual: u32,
    pub read_start: u32,
    pub read_len: u32,
    pub read_type: u32,
}

#[derive(Debug, Clone, Default)]
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
}

pub fn launch_threads(
    sra_file: &str,
    num_threads: usize,
    first_row_id: i64,
    row_count: u64,
    min_read_len: u32,
    skip_technical: bool,
    split_files: bool,
    outdir: &str,
) -> Result<()> {
    let total_rows = row_count as usize;
    let chunk_size = total_rows / num_threads;
    let remainder = total_rows % num_threads;

    // Shared writer for output
    let writers = if split_files {
        build_writers(Some(outdir))?
    } else {
        build_writers(None)?
    };
    let shared_writers = Arc::new(Mutex::new(writers));

    let mut handles = vec![];
    for i in 0..num_threads {
        let sra_file = sra_file.to_string();
        let writer = Arc::clone(&shared_writers);
        let start = first_row_id + (i * chunk_size) as i64;
        let end = if i == num_threads - 1 {
            start + (chunk_size + remainder) as i64
        } else {
            start + chunk_size as i64
        };
        handles.push(thread::spawn(move || {
            match thread_work(&sra_file, start, end, writer, min_read_len, skip_technical) {
                Ok(stats) => stats,
                Err(e) => {
                    eprintln!("Thread error: {}", e);
                    ProcessStatistics::default()
                }
            }
        }));
    }

    // Collect statistics
    let mut stats = ProcessStatistics::default();
    for handle in handles {
        let thread_stat = handle.join().unwrap();
        stats = stats + thread_stat;
    }

    // Final flush
    let mut writer = shared_writers.lock();
    for w in writer.iter_mut() {
        w.flush()?;
    }

    // Print statistics
    eprintln!("{:#?}", stats);

    Ok(())
}

fn thread_work(
    sra_file: &str,
    start_row: i64,
    end_row: i64,
    writer: Arc<Mutex<Vec<Box<dyn Write + Send>>>>,
    min_read_len: u32,
    skip_technical: bool,
) -> Result<ProcessStatistics> {
    let dir = match SafeKDirectory::new() {
        Ok(dir) => dir,
        Err(rc) => bail!(format!("KDirectoryNativeDir failed: {}", rc)),
    };
    let mgr = match SafeVDBManager::new(&dir) {
        Ok(mgr) => mgr,
        Err(rc) => bail!(format!("VDBManagerMakeRead failed: {}", rc)),
    };
    let schema = {
        let mut schema_ptr = std::ptr::null_mut();
        let rc = unsafe { VDBManagerMakeSchema(mgr.as_ptr(), &mut schema_ptr) };
        if rc != 0 {
            bail!("VDBManagerMakeSchema failed: {}", rc);
        }
        SafeVSchema(schema_ptr)
    };

    let table = open_table(&mgr, &schema, sra_file)?;

    let cursor = {
        let mut cursor_ptr = std::ptr::null();
        let rc =
            unsafe { VTableCreateCachedCursorRead(table.as_ptr(), &mut cursor_ptr, BUFFER_SIZE) };
        if rc != 0 {
            bail!("VTableCreateCachedCursorRead failed: {}", rc);
        }
        SafeVCursor(cursor_ptr)
    };

    let indices = add_columns(&table, &cursor)?;

    unsafe {
        let rc = VCursorOpen(cursor.as_ptr());
        if rc != 0 {
            bail!("VCursorOpen failed: {}", rc);
        }
    }

    process_range(
        &cursor,
        &indices,
        start_row,
        end_row,
        &writer,
        min_read_len,
        skip_technical,
    )
}

fn add_columns(table: &SafeVTable, cursor: &SafeVCursor) -> Result<ColumnIndices> {
    let mut indices = ColumnIndices {
        seq: 0,
        qual: 0,
        read_start: 0,
        read_len: 0,
        read_type: 0,
    };

    add_cursor_column(table, cursor, &mut indices.seq, "READ", None)?;
    add_cursor_column(
        table,
        cursor,
        &mut indices.qual,
        "QUALITY",
        Some("(INSDC:quality:text:phred_33)QUALITY"),
    )?;
    add_cursor_column(
        table,
        cursor,
        &mut indices.read_start,
        "READ_START",
        Some("(INSDC:coord:zero)READ_START"),
    )?;
    add_cursor_column(
        table,
        cursor,
        &mut indices.read_len,
        "READ_LEN",
        Some("(INSDC:coord:len)READ_LEN"),
    )?;
    add_cursor_column(
        table,
        cursor,
        &mut indices.read_type,
        "READ_TYPE",
        Some("(INSDC:SRA:xread_type)READ_TYPE"),
    )?;

    Ok(indices)
}

fn add_cursor_column(
    table: &SafeVTable,
    cursor: &SafeVCursor,
    index: &mut u32,
    col_name: &str,
    alt_name: Option<&str>,
) -> Result<()> {
    match is_column_present(table, col_name) {
        Ok(true) => {
            let c_col_name = if let Some(alt_name) = alt_name {
                CString::new(alt_name)?
            } else {
                CString::new(col_name)?
            };
            let rc = unsafe { VCursorAddColumn(cursor.as_ptr(), index, c_col_name.as_ptr()) };
            if rc != 0 {
                bail!("VCursorAddColumn({}) failed: {}", col_name, rc);
            }
            Ok(())
        }
        Ok(false) => bail!("Required column '{}' not found", col_name),
        Err(e) => bail!(e),
    }
}

fn process_range(
    cursor: &SafeVCursor,
    indices: &ColumnIndices,
    start_row: i64,
    end_row: i64,
    writer: &Arc<Mutex<Vec<Box<dyn Write + Send>>>>,
    min_read_len: u32,
    skip_technical: bool,
) -> Result<ProcessStatistics> {
    let mut stats = ProcessStatistics::default();
    let mut local_buffers = build_local_buffers(&writer.lock());
    let mut counts = vec![0; local_buffers.len()];
    let mut num_spots = 0;
    for row_id in start_row..end_row {
        // Get sequence data
        let (seq, qual, read_starts, read_lens, read_types) =
            unsafe { get_sequence_data(cursor, row_id, indices)? };

        // Prepare local buffer to minimize lock contention
        for (seg_id, (&start, &len)) in read_starts.iter().zip(read_lens.iter()).enumerate() {
            if skip_technical && read_types[seg_id] == 0 {
                stats.inc_filter_type(seg_id);
                continue;
            }

            if len < min_read_len {
                stats.inc_filter_size(seg_id);
                continue;
            }
            let end = start as usize + len as usize;
            let seq = &seq[start as usize..end];
            let qual = &qual[start as usize..end];

            write_to_buffer_set(&mut local_buffers, row_id, seg_id, seq, qual)?;
            if counts.len() == 1 {
                counts[0] += 1;
            } else {
                counts[seg_id] += 1;
            }

            stats.inc_reads(seg_id);
        }

        if num_spots % RECORD_CAPACITY == 0 {
            // Lock the writer until all buffers are written
            let mut writer = writer.lock();

            // Write buffer to shared writer
            for ((global_buf, local_buf), local_count) in writer
                .iter_mut()
                .zip(local_buffers.iter_mut())
                .zip(counts.iter_mut())
            {
                if *local_count > 0 {
                    global_buf.write_all(local_buf)?;
                }
                local_buf.clear();
                *local_count = 0;
            }
        }

        num_spots += 1;
        stats.inc_spots();
    }

    // Write remaining buffer to shared writer
    let mut writer = writer.lock();
    for (i, buffer) in local_buffers.iter_mut().enumerate() {
        writer[i].write_all(buffer)?;
        buffer.clear();
    }

    Ok(stats)
}

unsafe fn get_sequence_data<'a>(
    cursor: &'a SafeVCursor,
    row_id: i64,
    indices: &ColumnIndices,
) -> Result<(&'a [u8], &'a [u8], &'a [u32], &'a [u32], &'a [u8])> {
    // Initialize row length and number of reads
    let mut row_len = 0;
    let mut num_reads = 0;

    let mut seq_data = std::ptr::null();
    let rc = VCursorCellDataDirect(
        cursor.as_ptr(),
        row_id,
        indices.seq,
        std::ptr::null_mut(),
        &mut seq_data as *mut *const _,
        std::ptr::null_mut(),
        &mut row_len,
    );
    if rc != 0 {
        bail!("Failed to get sequence data for row {}: {}", row_id, rc);
    }

    let mut qual_data = std::ptr::null();
    let rc = VCursorCellDataDirect(
        cursor.as_ptr(),
        row_id,
        indices.qual,
        std::ptr::null_mut(),
        &mut qual_data as *mut *const _,
        std::ptr::null_mut(),
        &mut row_len,
    );
    if rc != 0 {
        bail!("Failed to get quality data for row {}: {}", row_id, rc);
    }

    let mut read_start_data = std::ptr::null();
    let rc = VCursorCellDataDirect(
        cursor.as_ptr(),
        row_id,
        indices.read_start,
        std::ptr::null_mut(),
        &mut read_start_data as *mut *const _,
        std::ptr::null_mut(),
        &mut num_reads,
    );
    if rc != 0 {
        bail!("Failed to get read start data for row {}: {}", row_id, rc);
    }

    let mut read_len_data = std::ptr::null();
    let rc = VCursorCellDataDirect(
        cursor.as_ptr(),
        row_id,
        indices.read_len,
        std::ptr::null_mut(),
        &mut read_len_data as *mut *const _,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    if rc != 0 {
        bail!("Failed to get read length data for row {}: {}", row_id, rc);
    }

    let mut read_type_data = std::ptr::null();
    let rc = VCursorCellDataDirect(
        cursor.as_ptr(),
        row_id,
        indices.read_type,
        std::ptr::null_mut(),
        &mut read_type_data as *mut *const _,
        std::ptr::null_mut(),
        std::ptr::null_mut(),
    );
    if rc != 0 {
        bail!("Failed to get read type data for row {}: {}", row_id, rc);
    }

    let seq_slice = std::slice::from_raw_parts(seq_data as *const u8, row_len as usize);
    let qual_slice = std::slice::from_raw_parts(qual_data as *const u8, row_len as usize);
    let read_starts = std::slice::from_raw_parts(read_start_data as *const u32, num_reads as usize);
    let read_lens = std::slice::from_raw_parts(read_len_data as *const u32, num_reads as usize);
    let read_types = std::slice::from_raw_parts(read_type_data as *const u8, num_reads as usize);

    Ok((seq_slice, qual_slice, read_starts, read_lens, read_types))
}
