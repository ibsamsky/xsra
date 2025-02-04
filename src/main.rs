use std::env;
use std::ffi::CString;
use std::io::{self, Write};
use std::sync::Arc;
use std::thread;

use anyhow::{bail, Result};
use parking_lot::Mutex;

use sra_rs::{
    is_column_present, SafeKDirectory, SafeVCursor, SafeVDBManager, SafeVSchema, SafeVTable,
    VCursorAddColumn, VCursorCellDataDirect, VCursorIdRange, VCursorOpen, VDBManagerMakeSchema,
    VTableCreateCachedCursorRead,
};

const BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer
const LOCAL_BUFFER_SIZE: usize = 10 * 1024; // 10kB buffer

#[derive(Debug)]
struct ColumnIndices {
    seq: u32,
    qual: u32,
    read_start: u32,
    read_len: u32,
    read_type: u32,
}

fn main() -> Result<()> {
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <SRA file>", args[0]);
        return Ok(());
    }
    let sra_file = args[1].clone();

    // Initialize VDB components in main thread to determine row count
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

    let table = open_table(&mgr, &schema, &sra_file)?;

    // Create temporary cursor to get row range
    let (first_row_id, row_count) = {
        let temp_cursor = {
            let mut cursor_ptr = std::ptr::null();
            let rc = unsafe {
                VTableCreateCachedCursorRead(table.as_ptr(), &mut cursor_ptr, BUFFER_SIZE)
            };
            if rc != 0 {
                bail!("VTableCreateCachedCursorRead failed: {}", rc);
            }
            SafeVCursor(cursor_ptr)
        };

        let mut seq_idx = 0;
        let col_name = CString::new("READ")?;
        unsafe {
            let rc = VCursorAddColumn(temp_cursor.as_ptr(), &mut seq_idx, col_name.as_ptr());
            if rc != 0 {
                bail!("VCursorAddColumn(READ) failed: {}", rc);
            }
            let rc = VCursorOpen(temp_cursor.as_ptr());
            if rc != 0 {
                bail!("VCursorOpen failed: {}", rc);
            }
        }

        let mut first_row_id = 0;
        let mut row_count = 0;
        unsafe {
            let rc = VCursorIdRange(
                temp_cursor.as_ptr(),
                seq_idx,
                &mut first_row_id,
                &mut row_count,
            );
            if rc != 0 {
                bail!("VCursorIdRange failed: {}", rc);
            }
        }

        (first_row_id, row_count)
    };

    // Determine number of threads and split work
    let num_threads = num_cpus::get();
    let total_rows = row_count as usize;
    let chunk_size = total_rows / num_threads;
    let remainder = total_rows % num_threads;

    // Shared writer for output
    let writer = io::BufWriter::with_capacity(BUFFER_SIZE, io::stdout());
    let shared_writer = Arc::new(Mutex::new(writer));

    let mut handles = vec![];

    for i in 0..num_threads {
        let sra_file = sra_file.clone();
        let writer = Arc::clone(&shared_writer);
        let start = first_row_id + (i * chunk_size) as i64;
        let end = if i == num_threads - 1 {
            start + (chunk_size + remainder) as i64
        } else {
            start + chunk_size as i64
        };
        handles.push(thread::spawn(move || {
            if let Err(e) = thread_work(&sra_file, start, end, writer) {
                eprintln!("Thread error: {}", e);
            }
        }));
    }

    for handle in handles {
        handle.join().unwrap();
    }

    // Final flush
    let mut writer = shared_writer.lock();
    writer.flush()?;

    Ok(())
}

fn thread_work(
    sra_file: &str,
    start_row: i64,
    end_row: i64,
    writer: Arc<Mutex<io::BufWriter<io::Stdout>>>,
) -> Result<()> {
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

    process_range(&cursor, &indices, start_row, end_row, &writer)
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

fn open_table(mgr: &SafeVDBManager, schema: &SafeVSchema, sra_file: &str) -> Result<SafeVTable> {
    match mgr.open_database(schema, sra_file) {
        Ok(Some(db)) => db
            .open_table("SEQUENCE")
            .map_err(|e| anyhow::anyhow!("VDatabaseOpenTableRead failed: {}", e)),
        Ok(None) => match mgr.open_table(schema, sra_file) {
            Ok(Some(table)) => Ok(table),
            Ok(None) => bail!("Failed to open input as either database or table"),
            Err(e) => bail!("VDBManagerOpenTableRead failed: {}", e),
        },
        Err(e) => bail!(e),
    }
}

fn process_range(
    cursor: &SafeVCursor,
    indices: &ColumnIndices,
    start_row: i64,
    end_row: i64,
    writer: &Arc<Mutex<io::BufWriter<io::Stdout>>>,
) -> Result<()> {
    let mut local_buffer = Vec::with_capacity(LOCAL_BUFFER_SIZE);
    for row_id in start_row..end_row {
        let mut row_len = 0;
        let mut num_reads = 0;

        // Get sequence data
        let (seq, qual, read_starts, read_lens) = unsafe {
            let mut seq_data = std::ptr::null();
            let rc = VCursorCellDataDirect(
                cursor.as_ptr(),
                row_id,
                indices.seq,
                std::ptr::null_mut(),
                &mut seq_data as *mut *const _ as *mut *const std::ffi::c_void,
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
                &mut qual_data as *mut *const _ as *mut *const std::ffi::c_void,
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
                &mut read_start_data as *mut *const _ as *mut *const std::ffi::c_void,
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
                &mut read_len_data as *mut *const _ as *mut *const std::ffi::c_void,
                std::ptr::null_mut(),
                std::ptr::null_mut(),
            );
            if rc != 0 {
                bail!("Failed to get read length data for row {}: {}", row_id, rc);
            }

            let seq_slice = std::slice::from_raw_parts(seq_data as *const u8, row_len as usize);
            let qual_slice = std::slice::from_raw_parts(qual_data as *const u8, row_len as usize);
            let read_starts =
                std::slice::from_raw_parts(read_start_data as *const u32, num_reads as usize);
            let read_lens =
                std::slice::from_raw_parts(read_len_data as *const u32, num_reads as usize);

            (seq_slice, qual_slice, read_starts, read_lens)
        };

        // Prepare local buffer to minimize lock contention
        for (i, (&start, &len)) in read_starts.iter().zip(read_lens.iter()).enumerate() {
            if len == 0 {
                continue;
            }
            let end = start as usize + len as usize;
            let seq = &seq[start as usize..end];
            let qual = &qual[start as usize..end];

            writeln!(local_buffer, "@{}.{}", row_id, i)?;
            local_buffer.extend_from_slice(seq);
            writeln!(local_buffer)?;
            writeln!(local_buffer, "+")?;
            local_buffer.extend_from_slice(qual);
            writeln!(local_buffer)?;
        }

        if local_buffer.len() > LOCAL_BUFFER_SIZE {
            // Write buffer to shared writer
            let mut writer = writer.lock();
            writer.write_all(&local_buffer)?;
            local_buffer.clear();
        }
    }

    if !local_buffer.is_empty() {
        // Write remaining buffer to shared writer
        let mut writer = writer.lock();
        writer.write_all(&local_buffer)?;
        local_buffer.clear();
    }

    Ok(())
}
