use anyhow::Result;
use std::fs;
use std::path::Path;
use std::sync::Once;
use xsra::cli::{AccessionOptions, MultiInputOptions, Provider};
use xsra::prefetch::prefetch;

static INIT: Once = Once::new();

/// Test fixture files
pub struct TestFixtures {
    pub base_dir: String,
    pub data_dir: String,
    pub valid_sra: String,
    pub corrupt_sra: String,
    pub invalid_sra: String,
}

impl TestFixtures {
    pub fn new() -> Self {
        let base_dir = "tests/fixtures".to_string();
        let data_dir = format!("{}/data", base_dir);
        Self {
            base_dir,
            data_dir: data_dir.clone(),
            valid_sra: format!("{}/valid.sra", data_dir),
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

        // Download valid SRA file if it doesn't exist
        if !Path::new(&self.valid_sra).exists() {
            println!("📥 Downloading valid SRA file fixture...");
            self.download_valid_sra()?;
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

    /// Download a small, valid SRA file for testing
    fn download_valid_sra(&self) -> Result<()> {
        // Use tokio runtime to handle async prefetch call
        let rt = tokio::runtime::Runtime::new()?;

        let input = MultiInputOptions {
            accessions: vec!["SRR390728".to_string()], // Small test dataset (~76MB)
            options: AccessionOptions {
                full_quality: false, // Use lite version
                lite_only: true,
                provider: Provider::Https,
                retry_limit: 3,
                retry_delay: 1000,
                gcp_project_id: None,
            },
        };

        // Download to fixtures data directory
        rt.block_on(prefetch(&input, Some(&self.data_dir)))?;

        // Find the downloaded file and rename it to valid.sra
        for entry in fs::read_dir(&self.data_dir)? {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "sra") {
                fs::rename(&path, &self.valid_sra)?;
                break;
            }
        }

        println!("Downloaded valid SRA fixture: {}", self.valid_sra);
        Ok(())
    }

    /// Create a corrupt SRA file by truncating a valid one
    fn create_corrupt_sra(&self) -> Result<()> {
        // First ensure we have a valid SRA file
        if !Path::new(&self.valid_sra).exists() {
            self.download_valid_sra()?;
        }

        // Read the valid file and truncate it to create corruption
        let valid_data = fs::read(&self.valid_sra)?;
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
