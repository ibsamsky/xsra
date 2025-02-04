// lib.rs
use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_void};

/// Opaque structs for the VDB API
#[repr(C)]
pub struct KDirectory {
    _private: [u8; 0],
}

#[repr(C)]
pub struct VDBManager {
    _private: [u8; 0],
}

#[repr(C)]
pub struct VSchema {
    _private: [u8; 0],
}

#[repr(C)]
pub struct VDatabase {
    _private: [u8; 0],
}

#[repr(C)]
pub struct VTable {
    _private: [u8; 0],
}

#[repr(C)]
pub struct VCursor {
    _private: [u8; 0],
}

#[repr(C)]
pub struct KNamelist {
    _private: [u8; 0],
}

// Type aliases for common types used in the VDB API
#[allow(non_camel_case_types)]
pub type rc_t = i32;

#[link(name = "ncbi-vdb")]
extern "C" {
    #[link_name = "KDirectoryNativeDir_v1"]
    pub fn KDirectoryNativeDir(dir: *mut *mut KDirectory) -> rc_t;
    pub fn VDBManagerMakeRead(mgr: *mut *const VDBManager, dir: *mut KDirectory) -> rc_t;
    pub fn VDBManagerMakeSchema(mgr: *const VDBManager, schema: *mut *mut VSchema) -> rc_t;
    pub fn VDBManagerOpenDBRead(
        mgr: *const VDBManager,
        db: *mut *const VDatabase,
        schema: *mut VSchema,
        path: *const c_char,
        ...
    ) -> rc_t;
    pub fn VDBManagerOpenTableRead(
        mgr: *const VDBManager,
        tbl: *mut *const VTable,
        schema: *mut VSchema,
        path: *const c_char,
        ...
    ) -> rc_t;
    pub fn VDatabaseOpenTableRead(
        db: *const VDatabase,
        tbl: *mut *const VTable,
        name: *const c_char,
    ) -> rc_t;
    pub fn VTableCreateCachedCursorRead(
        tbl: *const VTable,
        cursor: *mut *const VCursor,
        capacity: usize,
    ) -> rc_t;
    pub fn VTableListCol(tbl: *const VTable, columns: *mut *mut KNamelist) -> rc_t;
    pub fn VCursorAddColumn(cursor: *const VCursor, idx: *mut u32, name: *const c_char) -> rc_t;
    pub fn VCursorOpen(cursor: *const VCursor) -> rc_t;
    pub fn VCursorIdRange(
        cursor: *const VCursor,
        idx: u32,
        first: *mut i64,
        count: *mut u64,
    ) -> rc_t;
    pub fn VCursorCellDataDirect(
        cursor: *const VCursor,
        row_id: i64,
        column_idx: u32,
        elem_bits: *mut u32,
        data: *mut *const c_void,
        bit_offset: *mut u32,
        row_len: *mut u32,
    ) -> rc_t;

    // Release functions
    #[link_name = "KDirectoryRelease_v1"]
    pub fn KDirectoryRelease(self_: *mut KDirectory) -> rc_t;
    pub fn VDBManagerRelease(self_: *const VDBManager) -> rc_t;
    pub fn VSchemaRelease(self_: *mut VSchema) -> rc_t;
    pub fn VDatabaseRelease(self_: *const VDatabase) -> rc_t;
    pub fn VTableRelease(self_: *const VTable) -> rc_t;
    pub fn VCursorRelease(self_: *const VCursor) -> rc_t;
    pub fn KNamelistRelease(self_: *mut KNamelist) -> rc_t;

    // KNamelist functions
    pub fn KNamelistCount(list: *const KNamelist, count: *mut u32) -> rc_t;
    pub fn KNamelistGet(list: *const KNamelist, idx: u32, name: *mut *const c_char) -> rc_t;
}

// Safe wrapper structs
pub struct SafeKDirectory(pub(crate) *mut KDirectory);
pub struct SafeVDBManager(pub(crate) *const VDBManager);
pub struct SafeVSchema(pub *mut VSchema);
pub struct SafeVDatabase(pub *const VDatabase);
pub struct SafeVTable(pub *const VTable);
pub struct SafeVCursor(pub *const VCursor);

// Add methods to safely access the inner pointers
impl SafeKDirectory {
    pub fn as_ptr(&self) -> *mut KDirectory {
        self.0
    }
}

impl SafeVDBManager {
    pub fn as_ptr(&self) -> *const VDBManager {
        self.0
    }
    pub fn open_database(
        &self,
        schema: &SafeVSchema,
        path: &str,
    ) -> Result<Option<SafeVDatabase>, rc_t> {
        let mut db = std::ptr::null();
        let path = CString::new(path).unwrap();
        let rc = unsafe { VDBManagerOpenDBRead(self.0, &mut db, schema.as_ptr(), path.as_ptr()) };
        if rc != 0 {
            return Ok(None); // Not a database
        }
        Ok(Some(SafeVDatabase(db)))
    }

    pub fn open_table(&self, schema: &SafeVSchema, path: &str) -> Result<Option<SafeVTable>, rc_t> {
        let mut table = std::ptr::null();
        let path = CString::new(path).unwrap();
        let rc =
            unsafe { VDBManagerOpenTableRead(self.0, &mut table, schema.as_ptr(), path.as_ptr()) };
        if rc != 0 {
            return Ok(None); // Not a table
        }
        Ok(Some(SafeVTable(table)))
    }
}

impl SafeVSchema {
    pub fn as_ptr(&self) -> *mut VSchema {
        self.0
    }
}

impl SafeVDatabase {
    pub fn as_ptr(&self) -> *const VDatabase {
        self.0
    }
    pub fn open_table(&self, name: &str) -> Result<SafeVTable, rc_t> {
        let mut table = std::ptr::null();
        let name = CString::new(name).unwrap();
        let rc = unsafe { VDatabaseOpenTableRead(self.0, &mut table, name.as_ptr()) };
        if rc != 0 {
            return Err(rc);
        }
        Ok(SafeVTable(table))
    }
}

impl SafeVTable {
    pub fn as_ptr(&self) -> *const VTable {
        self.0
    }
}

impl SafeVCursor {
    pub fn as_ptr(&self) -> *const VCursor {
        self.0
    }
}

// Implement Drop for safe release of resources
impl Drop for SafeKDirectory {
    fn drop(&mut self) {
        unsafe { KDirectoryRelease(self.0) };
    }
}

impl Drop for SafeVDBManager {
    fn drop(&mut self) {
        unsafe { VDBManagerRelease(self.0) };
    }
}

impl Drop for SafeVSchema {
    fn drop(&mut self) {
        unsafe { VSchemaRelease(self.0) };
    }
}

impl Drop for SafeVDatabase {
    fn drop(&mut self) {
        unsafe { VDatabaseRelease(self.0) };
    }
}

impl Drop for SafeVTable {
    fn drop(&mut self) {
        unsafe { VTableRelease(self.0) };
    }
}

impl Drop for SafeVCursor {
    fn drop(&mut self) {
        unsafe { VCursorRelease(self.0) };
    }
}

// Safe wrapper functions
impl SafeKDirectory {
    pub fn new() -> Result<Self, rc_t> {
        let mut dir = std::ptr::null_mut();
        let rc = unsafe { KDirectoryNativeDir(&mut dir) };
        if rc != 0 {
            return Err(rc);
        }
        Ok(SafeKDirectory(dir))
    }
}

impl SafeVDBManager {
    pub fn new(dir: &SafeKDirectory) -> Result<Self, rc_t> {
        let mut mgr = std::ptr::null();
        let rc = unsafe { VDBManagerMakeRead(&mut mgr, dir.0) };
        if rc != 0 {
            return Err(rc);
        }
        Ok(SafeVDBManager(mgr))
    }
}

// Helper function to check if a column exists in a table
pub fn is_column_present(tbl: &SafeVTable, col_name: &str) -> Result<bool, rc_t> {
    let mut columns = std::ptr::null_mut();
    let rc = unsafe { VTableListCol(tbl.0, &mut columns) };
    if rc != 0 {
        return Err(rc);
    }

    let mut count = 0;
    let rc = unsafe { KNamelistCount(columns, &mut count) };
    if rc != 0 {
        unsafe { KNamelistRelease(columns) };
        return Err(rc);
    }

    let mut present = false;
    for i in 0..count {
        let mut name_ptr = std::ptr::null();
        let rc = unsafe { KNamelistGet(columns, i, &mut name_ptr) };
        if rc != 0 {
            unsafe { KNamelistRelease(columns) };
            return Err(rc);
        }

        let name = unsafe { CStr::from_ptr(name_ptr) }.to_str().unwrap();
        if name == col_name {
            present = true;
            break;
        }
    }

    unsafe { KNamelistRelease(columns) };
    Ok(present)
}
