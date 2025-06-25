use anyhow::Result;
use std::fs;
use std::path::Path;
use std::sync::Once;
use xsra::cli::{AccessionOptions, MultiInputOptions, Provider};
use xsra::prefetch::prefetch;

static INIT: Once = Once::new();

/// Test fixture files
pub struct TestFixtures {
    pub data_dir: String,
    pub small_variable_sra: String, // Small SRA with variable-length segments (for fast VBINSEQ testing)
    pub small_fixed_sra: String, // Small SRA with fixed-length segments (for fast BINSEQ testing)
    pub corrupt_sra: String,
    pub invalid_sra: String,
}

impl TestFixtures {
    pub fn new() -> Self {
        let base_dir = "tests/fixtures".to_string();
        let data_dir = format!("{}/data", base_dir);
        Self {
            data_dir: data_dir.clone(),
            small_variable_sra: format!("{}/small-variable.sra", data_dir), // SRR5150787 (~1.7MB, variable)
            small_fixed_sra: format!("{}/small-fixed.sra", data_dir), // SRR1574235 (~17MB, fixed)
            corrupt_sra: format!("{}/corrupt.sra", data_dir),
            invalid_sra: format!("{}/invalid.sra", data_dir),
        }
    }

    /// Ensure all test fixtures exist, downloading or creating them if needed.
    /// This method is thread-safe and will only run the setup once, even if called
    /// from multiple test files simultaneously.
    pub fn ensure_fixtures() -> Result<TestFixtures> {
        let fixtures = TestFixtures::new();
        let mut result = Ok(());

        INIT.call_once(|| {
            if let Err(e) = fixtures.setup_fixtures() {
                result = Err(e);
            }
        });

        result?;
        Ok(fixtures)
    }

    /// Internal method that does the actual fixture setup work
    fn setup_fixtures(&self) -> Result<()> {
        println!("🔧 Setting up test fixtures...");

        // Create fixtures data directory if it doesn't exist
        fs::create_dir_all(&self.data_dir)?;

        // Download small variable-length SRA file if it doesn't exist
        if !Path::new(&self.small_variable_sra).exists() {
            println!("📥 Downloading small variable-length SRA file fixture...");
            self.download_small_variable_sra()?;
        }

        // Download small fixed-length SRA file if it doesn't exist
        if !Path::new(&self.small_fixed_sra).exists() {
            println!("📥 Downloading small fixed-length SRA file fixture...");
            self.download_small_fixed_sra()?;
        }

        // Create corrupt SRA file if it doesn't exist
        if !Path::new(&self.corrupt_sra).exists() {
            println!("🔨 Creating corrupt SRA file fixture...");
            self.create_corrupt_sra()?;
        }

        // Create invalid SRA file if it doesn't exist
        if !Path::new(&self.invalid_sra).exists() {
            println!("📝 Creating invalid SRA file fixture...");
            self.create_invalid_sra()?;
        }

        println!("✅ Test fixtures setup complete!");
        Ok(())
    }

    /// Download a small SRA file with variable-length segments for fast VBINSEQ testing
    fn download_small_variable_sra(&self) -> Result<()> {
        // Use tokio runtime to handle async prefetch call
        let rt = tokio::runtime::Runtime::new()?;

        let input = MultiInputOptions {
            accessions: vec!["SRR5150787".to_string()], // Very small test dataset (~1.7MB)
            options: AccessionOptions {
                full_quality: false, // Prefer lite version
                lite_only: false,    // Allow fallback to full if lite not available
                provider: Provider::Https,
                retry_limit: 3,
                retry_delay: 1000,
                gcp_project_id: None,
            },
        };

        // Download to fixtures data directory
        rt.block_on(prefetch(&input, Some(&self.data_dir)))?;

        // Find the downloaded file and rename it to small-variable.sra
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.file_name().map_or(false, |name| {
                name.to_string_lossy().starts_with("SRR5150787")
            }) {
                fs::rename(&path, &self.small_variable_sra)?;
                break;
            }
        }

        println!(
            "Downloaded small variable-length SRA fixture: {}",
            self.small_variable_sra
        );
        Ok(())
    }

    /// Download a small SRA file with fixed-length segments for fast BINSEQ testing
    fn download_small_fixed_sra(&self) -> Result<()> {
        // Use tokio runtime to handle async prefetch call
        let rt = tokio::runtime::Runtime::new()?;

        let input = MultiInputOptions {
            accessions: vec!["SRR1574235".to_string()], // Small ChIP-seq dataset with fixed-length reads (~17MB)
            options: AccessionOptions {
                full_quality: false, // Prefer lite version
                lite_only: false,    // Allow fallback to full if lite not available
                provider: Provider::Https,
                retry_limit: 3,
                retry_delay: 1000,
                gcp_project_id: None,
            },
        };

        // Download to fixtures data directory
        rt.block_on(prefetch(&input, Some(&self.data_dir)))?;

        // Find the downloaded file and rename it to small-fixed.sra
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.file_name().map_or(false, |name| {
                name.to_string_lossy().starts_with("SRR1574235")
            }) {
                fs::rename(&path, &self.small_fixed_sra)?;
                break;
            }
        }

        println!(
            "Downloaded small fixed-length SRA fixture: {}",
            self.small_fixed_sra
        );
        Ok(())
    }

    /// Create a corrupt SRA file by truncating a valid one
    fn create_corrupt_sra(&self) -> Result<()> {
        // First ensure we have a large variable SRA file
        if !Path::new(&self.small_variable_sra).exists() {
            self.download_small_variable_sra()?;
        }

        // Read the large variable file and truncate it to create corruption
        let valid_data = fs::read(&self.small_variable_sra)?;
        let corrupt_size = valid_data.len() / 2; // Truncate to half size

        // Write the truncated (corrupt) version
        fs::write(&self.corrupt_sra, &valid_data[..corrupt_size])?;

        println!(
            "Created corrupt SRA fixture: {} ({} bytes, truncated from {})",
            self.corrupt_sra,
            corrupt_size,
            valid_data.len()
        );
        Ok(())
    }

    /// Create an invalid SRA file (not actually SRA format)
    fn create_invalid_sra(&self) -> Result<()> {
        // Create a text file with .sra extension - not actually SRA format
        let invalid_content = r#"
This is not a valid SRA file.
"#;

        fs::write(&self.invalid_sra, invalid_content)?;

        println!("Created invalid SRA fixture: {}", self.invalid_sra);
        Ok(())
    }
}
