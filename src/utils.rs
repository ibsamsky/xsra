use anyhow::{bail, Result};

use xsra::{SafeVDBManager, SafeVSchema, SafeVTable};

pub fn open_table(
    mgr: &SafeVDBManager,
    schema: &SafeVSchema,
    sra_file: &str,
) -> Result<SafeVTable> {
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
