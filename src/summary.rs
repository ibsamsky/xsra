use std::ffi::CString;

use anyhow::{bail, Result};

use super::{open_table, BUFFER_SIZE};
use xsra::{
    SafeKDirectory, SafeVCursor, SafeVDBManager, SafeVSchema, VCursorAddColumn, VCursorIdRange,
    VCursorOpen, VDBManagerMakeSchema, VTableCreateCachedCursorRead,
};

pub fn get_num_spots(sra_file: &str) -> Result<(i64, u64)> {
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

    let table = open_table(&mgr, &schema, sra_file)?;

    // Create temporary cursor to get row range
    let temp_cursor = {
        let mut cursor_ptr = std::ptr::null();
        let rc =
            unsafe { VTableCreateCachedCursorRead(table.as_ptr(), &mut cursor_ptr, BUFFER_SIZE) };
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

    Ok((first_row_id, row_count))
}
