use anyhow::Result;
use ncbi_vdb::SraReader;

pub fn get_num_records(path: &str) -> Result<u64> {
    let reader = SraReader::new(path)?;
    Ok(reader.stop())
}
