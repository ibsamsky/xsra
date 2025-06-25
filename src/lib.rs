// Library interface for xsra
// This allows integration tests to access the internal modules

// Re-export constants needed by modules
pub const BUFFER_SIZE: usize = 1024 * 1024;
pub const RECORD_CAPACITY: usize = 1024;

pub mod cli;
pub mod describe;
pub mod dump;
pub mod output;
pub mod prefetch;
pub mod recode;
pub mod utils;
