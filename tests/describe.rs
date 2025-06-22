use anyhow::Result;
use xsra::describe::describe_inner;

mod fixtures;
use fixtures::TestFixtures;

/// Integration tests for describe

#[test]
fn test_describe_with_valid_sra_fixture() -> Result<()> {
    let fixtures = TestFixtures::ensure_fixtures()?;

    // Test describe with valid SRA file
    let stats = describe_inner(&fixtures.small_variable_sra, 0, 100)?;

    // Verify we got meaningful results using available methods
    let segment_lengths = stats.segment_lengths();
    assert!(
        segment_lengths.len() > 0,
        "No segments found in valid SRA file"
    );
    assert!(
        segment_lengths.iter().any(|&len| len > 0.0),
        "No meaningful segment lengths found"
    );

    // Test that we can serialize the stats (exercises the pprint functionality)
    let mut output = Vec::new();
    stats.pprint(&mut output)?;
    assert!(
        output.len() > 0,
        "Stats serialization produced empty output"
    );

    Ok(())
}

#[test]
fn test_describe_with_invalid_sra_fixture() -> Result<()> {
    let fixtures = TestFixtures::ensure_fixtures()?;

    // Test describe with invalid SRA file - should fail
    let result = describe_inner(&fixtures.invalid_sra, 0, 10);

    assert!(
        result.is_err(),
        "Expected describe to fail with invalid SRA file"
    );

    Ok(())
}

#[test]
fn test_describe_with_corrupt_sra_fixture() -> Result<()> {
    let fixtures = TestFixtures::ensure_fixtures()?;

    // Test describe with corrupt SRA file - should fail
    let result = describe_inner(&fixtures.corrupt_sra, 0, 10);

    assert!(
        result.is_err(),
        "Expected describe to fail with corrupt SRA file"
    );

    Ok(())
}
