// main.rs
use std::env;
use std::ffi::CString;
use std::io::{self, Write};

use anyhow::{bail, Result};

use sra_rs::{
    is_column_present, SafeKDirectory, SafeVCursor, SafeVDBManager, SafeVSchema, SafeVTable,
    VCursorAddColumn, VCursorCellDataDirect, VCursorIdRange, VCursorOpen, VDBManagerMakeSchema,
    VDBManagerOpenTableRead, VTableCreateCachedCursorRead,
};

const BUFFER_SIZE: usize = 1024 * 1024; // 1MB buffer

#[derive(Debug)]
struct ColumnIndices {
    seq: u32,
    qual: u32,
    read_start: u32,
    read_len: u32,
    read_type: u32,
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command line arguments
    let args: Vec<String> = env::args().collect();
    if args.len() != 2 {
        eprintln!("Usage: {} <SRA file>", args[0]);
        return Ok(());
    }

    let sra_file = &args[1];

    // Set up buffered stdout
    let stdout = io::stdout();
    let mut writer = io::BufWriter::with_capacity(BUFFER_SIZE, stdout.lock());

    // Initialize VDB components
    let dir = SafeKDirectory::new().map_err(|rc| format!("KDirectoryNativeDir failed: {}", rc))?;

    let mgr =
        SafeVDBManager::new(&dir).map_err(|rc| format!("VDBManagerMakeRead failed: {}", rc))?;

    // Create schema
    let schema = unsafe {
        let mut schema = std::ptr::null_mut();
        let rc = VDBManagerMakeSchema(mgr.as_ptr(), &mut schema);
        if rc != 0 {
            return Err(format!("VDBManagerMakeSchema failed: {}", rc).into());
        }
        SafeVSchema(schema)
    };

    // Open table
    let table = unsafe {
        let mut table = std::ptr::null();
        let path = CString::new(sra_file.as_str())?;
        let rc = VDBManagerOpenTableRead(mgr.as_ptr(), &mut table, schema.as_ptr(), path.as_ptr());
        if rc != 0 {
            return Err(format!("VDBManagerOpenTableRead failed: {}", rc).into());
        }
        SafeVTable(table)
    };

    // Create cursor
    let cursor = unsafe {
        let mut cursor = std::ptr::null();
        let rc = VTableCreateCachedCursorRead(table.as_ptr(), &mut cursor, BUFFER_SIZE);
        if rc != 0 {
            return Err(format!("VTableCreateCursorRead failed: {}", rc).into());
        }
        SafeVCursor(cursor)
    };

    // Add required columns and get their indices
    let indices = add_columns(&table, &cursor)?;

    // Open cursor
    unsafe {
        let rc = VCursorOpen(cursor.as_ptr());
        if rc != 0 {
            return Err(format!("VCursorOpen failed: {}", rc).into());
        }
    }

    // Process rows
    process_rows(&cursor, &indices, &mut writer)?;

    Ok(())
}

fn add_columns(table: &SafeVTable, cursor: &SafeVCursor) -> Result<ColumnIndices> {
    let mut indices = ColumnIndices {
        seq: 0,
        qual: 0,
        read_start: 0,
        read_len: 0,
        read_type: 0,
    };

    unsafe {
        // Add READ column
        match is_column_present(table, "READ") {
            Ok(true) => {
                let col_name = CString::new("READ")?;
                let rc = VCursorAddColumn(cursor.as_ptr(), &mut indices.seq, col_name.as_ptr());
                if rc != 0 {
                    bail!("VCursorAddColumn(READ) failed: {}", rc);
                }
            }
            Ok(false) => bail!("Required column 'READ' not found"),
            Err(e) => bail!(e),
        }

        // Add QUALITY column
        match is_column_present(table, "QUALITY") {
            Ok(true) => {
                // let col_name = CString::new("QUALITY")?;
                let col_name = CString::new("(INSDC:quality:text:phred_33)QUALITY")?;
                let rc = VCursorAddColumn(cursor.as_ptr(), &mut indices.qual, col_name.as_ptr());
                if rc != 0 {
                    bail!("VCursorAddColumn(QUALITY) failed: {}", rc);
                }
            }
            Ok(false) => bail!("Required column 'QUALITY' not found"),
            Err(e) => bail!(e),
        }

        // Add READ_START column
        match is_column_present(table, "READ_START") {
            Ok(true) => {
                let col_name = CString::new("(INSDC:coord:zero)READ_START")?;
                let rc =
                    VCursorAddColumn(cursor.as_ptr(), &mut indices.read_start, col_name.as_ptr());
                if rc != 0 {
                    bail!("VCursorAddColumn(READ_START) failed: {}", rc);
                }
            }
            Ok(false) => bail!("Required column 'READ_START' not found"),
            Err(e) => bail!(e),
        }

        // Add READ_LEN column
        match is_column_present(table, "READ_LEN") {
            Ok(true) => {
                let col_name = CString::new("(INSDC:coord:len)READ_LEN")?;
                let rc =
                    VCursorAddColumn(cursor.as_ptr(), &mut indices.read_len, col_name.as_ptr());
                if rc != 0 {
                    bail!("VCursorAddColumn(READ_LEN) failed: {}", rc);
                }
            }
            Ok(false) => bail!("Required column 'READ_LEN' not found"),
            Err(e) => bail!(e),
        }

        // Add READ_TYPE column (optional)
        match is_column_present(table, "READ_TYPE") {
            Ok(true) => {
                let col_name = CString::new("(INSDC:SRA:xread_type)READ_TYPE")?;
                let rc =
                    VCursorAddColumn(cursor.as_ptr(), &mut indices.read_type, col_name.as_ptr());
                if rc != 0 {
                    eprintln!("Warning: Failed to add READ_TYPE column: {}", rc);
                }
            }
            Ok(false) => {
                eprintln!("Warning: Optional column 'READ_TYPE' not found");
            }
            Err(e) => bail!(e),
        }
    }

    Ok(indices)
}

fn process_rows(
    cursor: &SafeVCursor,
    indices: &ColumnIndices,
    writer: &mut impl Write,
) -> Result<(), Box<dyn std::error::Error>> {
    unsafe {
        // Get row range
        let mut first_row_id = 0;
        let mut row_count = 0;
        let rc = VCursorIdRange(
            cursor.as_ptr(),
            indices.seq,
            &mut first_row_id,
            &mut row_count,
        );
        if rc != 0 {
            return Err(format!("VCursorIdRange failed: {}", rc).into());
        }

        // Process each row
        for row_id in first_row_id..first_row_id + row_count as i64 {
            let mut row_len = 0;
            let mut num_reads = 0;

            // Get sequence data
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
                eprintln!("Failed to get sequence data for row {}: {}", row_id, rc);
                continue;
            }

            // Get quality data
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
                eprintln!("Failed to get quality data for row {}: {}", row_id, rc);
                continue;
            }

            // Get read start positions
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
                eprintln!("Failed to get read start data for row {}: {}", row_id, rc);
                continue;
            }

            // Get read lengths
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
                eprintln!("Failed to get read length data for row {}: {}", row_id, rc);
                continue;
            }

            // Cast raw pointers to slices
            let seq = std::slice::from_raw_parts(seq_data as *const u8, row_len as usize);
            let qual = std::slice::from_raw_parts(qual_data as *const u8, row_len as usize);
            let read_starts =
                std::slice::from_raw_parts(read_start_data as *const u32, num_reads as usize);
            let read_lens =
                std::slice::from_raw_parts(read_len_data as *const u32, num_reads as usize);

            // Process each read in the spot
            for (i, (&start, &length)) in read_starts.iter().zip(read_lens.iter()).enumerate() {
                if length > 0 {
                    // Write FASTQ format output
                    writeln!(writer, "@{}.{}", row_id, i)?;
                    writer.write_all(&seq[start as usize..(start + length) as usize])?;
                    writeln!(writer)?;
                    writeln!(writer, "+")?;
                    writer.write_all(&qual[start as usize..(start + length) as usize])?;
                    writeln!(writer)?;
                }
            }
        }
    }

    Ok(())
}
