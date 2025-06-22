use anyhow::Result;
use std::fs;
use tempfile::TempDir;
use xsra::cli::{AccessionOptions, MultiInputOptions, Provider};
use xsra::prefetch::prefetch;

/// Integration tests for prefetch

#[test]
#[ignore = "requires network access"]
fn test_prefetch_multiple_accessions_https() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    let input = MultiInputOptions {
        // Use small, real accessions and lite only to keep the test fast
        accessions: vec!["SRR390728".to_string(), "SRR390729".to_string()],
        options: AccessionOptions {
            full_quality: false,
            lite_only: true,
            provider: Provider::Https,
            retry_limit: 3,
            retry_delay: 1000,
            gcp_project_id: None,
        },
    };

    prefetch(&input, Some(output_path.to_str().unwrap()))?;

    // We don't know the exact filenames (.sra or .lite.sra), so we just count them.
    let files: Vec<_> = fs::read_dir(output_path)?.filter_map(Result::ok).collect();
    assert_eq!(
        files.len(),
        2,
        "Expected two files to be downloaded for two accessions"
    );

    // Verify both files are not empty
    for file in files {
        assert!(
            file.metadata()?.len() > 0,
            "Downloaded file {:?} is empty",
            file.file_name()
        );
    }
    Ok(())
}

#[test]
#[ignore = "requires network access"]
fn test_prefetch_invalid_accession_https() -> Result<()> {
    let temp_dir = TempDir::new()?;
    let output_path = temp_dir.path();

    let input = MultiInputOptions {
        accessions: vec!["INVALID_ACCESSION_12345".to_string()],
        options: AccessionOptions {
            full_quality: false,
            lite_only: false,
            provider: Provider::Https,
            retry_limit: 1,
            retry_delay: 100,
            gcp_project_id: None,
        },
    };

    let result = prefetch(&input, Some(output_path.to_str().unwrap()));

    assert!(
        result.is_err(),
        "Expected prefetch to fail with invalid accession"
    );
    Ok(())
}

#[test]
#[ignore = "requires network access"]
fn test_prefetch_full_vs_lite_quality() -> Result<()> {
    let temp_dir_lite = TempDir::new()?;
    let output_path_lite = temp_dir_lite.path();

    // First, download lite version
    let input_lite = MultiInputOptions {
        accessions: vec!["SRR390728".to_string()],
        options: AccessionOptions {
            full_quality: false,
            lite_only: true,
            provider: Provider::Https,
            retry_limit: 3,
            retry_delay: 1000,
            gcp_project_id: None,
        },
    };
    prefetch(&input_lite, Some(output_path_lite.to_str().unwrap()))?;
    let lite_file_path = fs::read_dir(output_path_lite)?.next().unwrap()?.path();
    let lite_size = fs::metadata(&lite_file_path)?.len();

    // Then, download full version
    let temp_dir_full = TempDir::new()?;
    let output_path_full = temp_dir_full.path();
    let input_full = MultiInputOptions {
        accessions: vec!["SRR390728".to_string()],
        options: AccessionOptions {
            full_quality: true,
            lite_only: false,
            provider: Provider::Https,
            retry_limit: 3,
            retry_delay: 1000,
            gcp_project_id: None,
        },
    };
    prefetch(&input_full, Some(output_path_full.to_str().unwrap()))?;
    let full_file_path = fs::read_dir(output_path_full)?.next().unwrap()?.path();
    let full_size = fs::metadata(&full_file_path)?.len();

    assert!(
        full_size > lite_size,
        "Expected full quality file ({}) to be larger than lite quality file ({})",
        full_size,
        lite_size
    );
    Ok(())
}
