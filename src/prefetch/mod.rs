use std::{
    fs::File,
    io::{BufWriter, Write},
    sync::Arc,
    time::Duration,
};

use anyhow::{bail, Result};
use futures::{future::join_all, stream::FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::Client;
use tokio::{sync::Semaphore, time::sleep};

use crate::cli::{AccessionOptions, MultiInputOptions, Provider};

/// Semaphore for rate limiting (NCBI limits to 3 requests per second)
pub const RATE_LIMIT_SEMAPHORE: usize = 3;

/// Checks if the response from NCBI indicates rate limiting
fn is_rate_limited(response: &str) -> bool {
    // Check for the specific JSON rate limit response
    if response.contains("API rate limit exceeded") {
        return true;
    }

    // Check if response is a JSON error with rate limit indicators
    if response.starts_with("{")
        && (response.contains("rate limit") || response.contains("limit exceeded"))
    {
        return true;
    }

    false
}

pub async fn query_entrez(accession: &str) -> Result<String> {
    let query_url = format!(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=sra&id={}&rettype=full",
        accession
    );
    let response = Client::new().get(&query_url).send().await?.text().await?;
    Ok(response)
}

pub fn parse_url(
    accession: &str,
    response: &str,
    full_quality: bool,
    provider: Provider,
) -> Option<String> {
    for line in response.replace(" ", "\n").split("\n") {
        if line.contains("url=")
            && line.contains(accession)
            && !line.contains(".fastq")
            && !line.contains(".gz")
            && line.contains(provider.url_prefix())
        {
            if full_quality && line.contains(".lite") {
                continue;
            }
            if !full_quality && !line.contains(".lite") {
                continue;
            }
            let url = line.replace("url=", "").replace('"', "");
            return Some(url);
        }
    }
    None
}

pub fn parse_url_with_fallback(
    accession: &str,
    response: &str,
    full_quality: bool,
    lite_only: bool,
    provider: Provider,
) -> Option<String> {
    // Try preferred quality type
    if let Some(url) = parse_url(accession, response, full_quality, provider) {
        return Some(url);
    }

    // Fallback from SRA lite to full if needed
    if !lite_only {
        if let Some(url) = parse_url(accession, response, true, provider) {
            eprintln!(
                "Warning: Lite quality not available for {}, falling back to full quality",
                accession
            );
            return Some(url);
        }
    } else {
        eprintln!("Warning: No lite quality found for <{accession}> - not performing fallback because `--lite-only` flag in use")
    }

    None
}

pub async fn identify_url(accession: &str, options: &AccessionOptions) -> Result<String> {
    let mut retry_count = 0;

    loop {
        // Break the loop if we've reached max retries
        if retry_count >= options.retry_limit {
            break;
        }

        let entrez_response = query_entrez(accession).await?;

        // Check if we're being rate limited
        if is_rate_limited(&entrez_response) {
            let delay = options.retry_delay + (retry_count * options.retry_delay);
            eprintln!(
                "Rate limit detected for accession {}, retrying in {}ms (attempt {}/{})",
                accession, delay, retry_count, options.retry_limit
            );

            // Use tokio::time::sleep for asynchronous sleep
            tokio::time::sleep(Duration::from_millis(delay as u64)).await;
            retry_count += 1;
            continue;
        }

        // If we have a valid response, try to parse the URL
        if let Some(url) = parse_url_with_fallback(
            accession,
            &entrez_response,
            options.full_quality,
            options.lite_only,
            options.provider,
        ) {
            match options.provider {
                Provider::Https | Provider::Gcp => return Ok(url),
                _ => {
                    bail!(
                        "Identified the {}-URL, but cannot currently proceed: {url}",
                        options.provider,
                    );
                }
            }
        } else {
            // If we can't parse a URL, break out of the loop to return the error
            break;
        }
    }

    // If we've exhausted retries or couldn't parse a URL, return an error
    bail!("Unable to identify a download URL for accession: <{accession}> with full_quality={} and provider={}",
        options.full_quality,
        options.provider,
    )
}

// Rate-limited version that processes multiple accessions by calling identify_url
pub async fn identify_urls(
    accessions: &[String],
    options: &AccessionOptions,
) -> Result<Vec<(String, Result<String>)>> {
    let total = accessions.len();
    eprintln!("Identifying URLs for {} accessions...", total);

    // Use a semaphore to limit concurrent requests to 3
    let semaphore = Arc::new(Semaphore::new(RATE_LIMIT_SEMAPHORE));
    let mut tasks = Vec::new();

    for accession in accessions {
        let accession_clone = accession.clone();
        let options_clone = options.clone();
        let sem_clone = Arc::clone(&semaphore);

        // Create a task for each accession that respects the semaphore
        let task = tokio::spawn(async move {
            // Acquire permit from semaphore (blocks when 3 permits are already taken)
            let _permit = sem_clone
                .acquire()
                .await
                .expect("Semaphore should not be closed");
            eprintln!(">> Identifying URL for accession: {}", accession_clone);

            // Execute the request
            let result = identify_url(&accession_clone, &options_clone).await;

            // The permit is automatically released when it goes out of scope
            // Small delay to ensure we don't exceed rate limits when permits are released in bursts
            sleep(Duration::from_millis(50)).await;

            (accession_clone, result)
        });

        tasks.push(task);
    }

    // Wait for all tasks to complete
    let results = join_all(tasks).await;

    // Process results, handling any JoinError from the spawned tasks
    let mut processed_results = Vec::new();
    for result in results {
        match result {
            Ok(res) => processed_results.push(res),
            Err(e) => eprintln!("Task join error: {}", e),
        }
    }

    Ok(processed_results)
}

/// Download a file from a URL asynchronously
async fn download_url(url: String, path: String, pb: ProgressBar) -> Result<()> {
    let filename = url.split('/').next_back().unwrap_or("");
    let client = reqwest::Client::new()
        .get(&url)
        .send()
        .await?
        .error_for_status()?;

    let size = client.content_length().unwrap_or(0);
    pb.set_style(ProgressStyle::default_bar()
        .template(
            "{spinner:.green} [{elapsed_precise}] [{bar:40.cyan/blue}] {bytes}/{total_bytes} ({eta}) {msg}")?
        .progress_chars("#>-"));
    pb.set_length(size);
    pb.set_message(filename.to_string());

    let mut file = File::create(path).map(BufWriter::new)?;
    let mut stream = client.bytes_stream();
    while let Some(item) = stream.next().await {
        let chunk = item?;
        pb.inc(chunk.len() as u64);
        file.write_all(&chunk)?;
    }
    file.flush()?;
    pb.finish();
    Ok(())
}

/// Download a file from a GCP URL using gsutil
async fn download_url_gcp(
    url: String,
    path: String,
    project_id: String,
    pb: ProgressBar,
) -> Result<()> {
    let filename = url.split('/').next_back().unwrap_or("");
    pb.set_message(format!("GCP: {}", filename));

    // Set indeterminate progress style - we'll let gsutil show its own progress
    pb.set_style(ProgressStyle::default_spinner().template("{spinner:.green} {msg}")?);

    // Prepare the gsutil command
    let mut cmd = std::process::Command::new("gsutil");
    cmd.arg("-u")
        .arg(project_id)
        .arg("cp")
        .arg(&url)
        .arg(&path)
        // Use inherit to show gsutil's own progress bar in the terminal
        .stdout(std::process::Stdio::inherit())
        .stderr(std::process::Stdio::inherit());

    // Execute the command and wait for it to complete
    let status = cmd.spawn()?.wait()?;

    if !status.success() {
        pb.finish_with_message(format!("Failed to download {}", filename));
        bail!("gsutil command failed with exit code: {}", status);
    }

    pb.finish_with_message(format!("Downloaded {} successfully", filename));
    Ok(())
}

pub async fn prefetch(input: &MultiInputOptions, output_dir: Option<&str>) -> Result<()> {
    let accessions = input.accession_set();

    if accessions.is_empty() {
        bail!("No accessions provided");
    }

    // For a single accession
    if accessions.len() == 1 {
        let url = identify_url(&accessions[0], &input.options).await?;
        let path = match output_dir {
            Some(dir) => format!("{}/{}.sra", dir, &accessions[0]),
            None => format!("{}.sra", &accessions[0]),
        };

        let pb = ProgressBar::new(0);

        return match input.options.provider {
            Provider::Https => download_url(url, path, pb).await,
            Provider::Gcp => {
                let project_id = match &input.options.gcp_project_id {
                    Some(id) => id.to_string(),
                    None => bail!("GCP project ID is required for GCP downloads"),
                };
                download_url_gcp(url, path, project_id, pb).await
            }
            _ => bail!("Unsupported provider: {:?}", input.options.provider),
        };
    }

    // For multiple accessions
    // Step 1: Identify URLs with rate limiting
    let url_results = identify_urls(accessions, &input.options).await?;

    // Step 2: Download files concurrently
    let mp = MultiProgress::new();

    // For HTTPS downloads, we can use FuturesUnordered for full concurrency
    let mut https_downloads = FuturesUnordered::new();

    // For GCP downloads, we'll use a separate Vec since gsutil has its own concurrency management
    let mut gcp_downloads = Vec::new();

    for (accession, url_result) in url_results {
        match url_result {
            Ok(url) => {
                let path = match output_dir {
                    Some(dir) => format!("{}/{}.sra", dir, accession),
                    None => format!("{}.sra", accession),
                };

                let pb = mp.add(ProgressBar::new(0));
                pb.set_message(format!("Downloading {}", accession));

                match input.options.provider {
                    Provider::Https => {
                        https_downloads.push(download_url(url, path, pb));
                    }
                    Provider::Gcp => {
                        let project_id = match &input.options.gcp_project_id {
                            Some(id) => id.to_string(),
                            None => {
                                eprintln!(
                                    "Error for accession {}: GCP project ID is required",
                                    accession
                                );
                                continue;
                            }
                        };
                        // We'll collect GCP downloads and process them separately
                        gcp_downloads.push((url, path, project_id, pb));
                    }
                    _ => {
                        eprintln!(
                            "Error for accession {}: Unsupported provider: {:?}",
                            accession, input.options.provider
                        );
                        continue;
                    }
                }
            }
            Err(e) => {
                eprintln!("Error for accession {}: {}", accession, e);
            }
        }
    }

    // Process HTTPS downloads concurrently
    while let Some(result) = https_downloads.next().await {
        if let Err(e) = result {
            eprintln!("Download error: {}", e);
        }
    }

    // Process GCP downloads - since gsutil has its own concurrency management,
    // we'll run them sequentially to avoid overwhelming the terminal output
    for (url, path, project_id, pb) in gcp_downloads {
        if let Err(e) = download_url_gcp(url, path, project_id, pb).await {
            eprintln!("GCP download error: {}", e);
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_is_rate_limited_table_driven() {
        struct TestCase {
            name: &'static str,
            response: &'static str,
            expected: bool,
        }

        let test_cases = vec![
            TestCase {
                name: "detects json API rate limit exceeded",
                response: r#"{"error": "API rate limit exceeded"}"#,
                expected: true,
            },
            TestCase {
                name: "detects json rate limit exceeded",
                response: r#"{"message": "rate limit exceeded"}"#,
                expected: true,
            },
            TestCase {
                name: "detects json limit exceeded",
                response: r#"{"error": "limit exceeded"}"#,
                expected: true,
            },
            TestCase {
                name: "detects plain text rate limit",
                response: "API rate limit exceeded",
                expected: true,
            },
            TestCase {
                name: "ignores normal responses",
                response: "normal response",
                expected: false,
            },
            TestCase {
                name: "ignores empty responses",
                response: "",
                expected: false,
            },
            TestCase {
                name: "ignores success json",
                response: r#"{"status": "success", "data": "some data"}"#,
                expected: false,
            },
            TestCase {
                name: "ignores malformed json starting with brace",
                response: "{not valid json",
                expected: false,
            },
            TestCase {
                name: "ignores false positives",
                response: "user rating limit is 5 stars",
                expected: false,
            },
        ];

        for test_case in test_cases {
            let result = is_rate_limited(test_case.response);
            assert_eq!(
                result, test_case.expected,
                "Test case '{}' failed",
                test_case.name
            );
        }
    }

    #[test]
    fn test_provider_url_prefix_table_driven() {
        let test_cases = vec![
            (Provider::Https, "https://"),
            (Provider::Gcp, "gs://"),
            (Provider::Aws, "s3://"),
        ];

        for (provider, expected_prefix) in test_cases {
            assert_eq!(
                provider.url_prefix(),
                expected_prefix,
                "Provider {:?} should have prefix '{}'",
                provider,
                expected_prefix
            );
        }
    }

    #[test]
    fn test_parse_url_table_driven() {
        struct TestCase {
            name: &'static str,
            response: &'static str,
            accession: &'static str,
            full_quality: bool,
            provider: Provider,
            expected: Option<&'static str>,
        }

        let test_cases = vec![
            TestCase {
                name: "prefers lite when full_quality=false",
                response: r#"
                    url="https://example.com/SRR123456.sra"
                    url="https://example.com/SRR123456.lite.sra"
                "#,
                accession: "SRR123456",
                full_quality: false,
                provider: Provider::Https,
                expected: Some("https://example.com/SRR123456.lite.sra"),
            },
            TestCase {
                name: "prefers full when full_quality=true",
                response: r#"
                    url="https://example.com/SRR123456.sra"
                    url="https://example.com/SRR123456.lite.sra"
                "#,
                accession: "SRR123456",
                full_quality: true,
                provider: Provider::Https,
                expected: Some("https://example.com/SRR123456.sra"),
            },
            TestCase {
                name: "filters wrong accession",
                response: r#"url="https://example.com/SRR999999.sra""#,
                accession: "SRR123456",
                full_quality: true,
                provider: Provider::Https,
                expected: None,
            },
            TestCase {
                name: "filters fastq files",
                response: r#"
                    url="https://example.com/SRR123456.fastq"
                    url="https://example.com/SRR123456.sra"
                "#,
                accession: "SRR123456",
                full_quality: true,
                provider: Provider::Https,
                expected: Some("https://example.com/SRR123456.sra"),
            },
            TestCase {
                name: "filters gz files",
                response: r#"
                    url="https://example.com/SRR123456.sra.gz"
                    url="https://example.com/SRR123456.sra"
                "#,
                accession: "SRR123456",
                full_quality: true,
                provider: Provider::Https,
                expected: Some("https://example.com/SRR123456.sra"),
            },
            TestCase {
                name: "respects GCP provider",
                response: r#"
                    url="https://sra-pub-run-odp.s3.amazonaws.com/sra/SRR123456/SRR123456.sra"
                    url="gs://sra-pub-run-gs/sra/SRR123456/SRR123456.sra"
                "#,
                accession: "SRR123456",
                full_quality: true,
                provider: Provider::Gcp,
                expected: Some("gs://sra-pub-run-gs/sra/SRR123456/SRR123456.sra"),
            },
            TestCase {
                name: "respects AWS provider",
                response: r#"
                    url="https://example.com/SRR123456.sra"
                    url="s3://sra-pub-src-odp/sra/SRR123456/SRR123456.sra"
                "#,
                accession: "SRR123456",
                full_quality: true,
                provider: Provider::Aws,
                expected: Some("s3://sra-pub-src-odp/sra/SRR123456/SRR123456.sra"),
            },
            TestCase {
                name: "returns none for no urls",
                response: "no urls here",
                accession: "SRR123456",
                full_quality: true,
                provider: Provider::Https,
                expected: None,
            },
            TestCase {
                name: "handles quotes properly",
                response: r#"url="https://example.com/SRR123456.sra""#,
                accession: "SRR123456",
                full_quality: true,
                provider: Provider::Https,
                expected: Some("https://example.com/SRR123456.sra"),
            },
        ];

        for test_case in test_cases {
            let result = parse_url(
                test_case.accession,
                test_case.response,
                test_case.full_quality,
                test_case.provider,
            );

            assert_eq!(
                result.as_deref(),
                test_case.expected,
                "Test case '{}' failed",
                test_case.name
            );
        }
    }

    #[test]
    fn test_parse_url_with_fallback_table_driven() {
        struct TestCase {
            name: &'static str,
            response: &'static str,
            full_quality: bool,
            lite_only: bool,
            expected_contains: Option<&'static str>, // What the result should contain
            should_succeed: bool,
        }

        let test_cases = vec![
            TestCase {
                name: "both available, prefers lite when full_quality=false",
                response: r#"
                    url="https://example.com/SRR123456.sra"
                    url="https://example.com/SRR123456.lite.sra"
                "#,
                full_quality: false,
                lite_only: false,
                expected_contains: Some(".lite."),
                should_succeed: true,
            },
            TestCase {
                name: "both available, prefers full when full_quality=true",
                response: r#"
                    url="https://example.com/SRR123456.sra"
                    url="https://example.com/SRR123456.lite.sra"
                "#,
                full_quality: true,
                lite_only: false,
                expected_contains: Some("SRR123456.sra"),
                should_succeed: true,
            },
            TestCase {
                name: "only full available, fallback works when lite_only=false",
                response: r#"url="https://example.com/SRR123456.sra""#,
                full_quality: false,
                lite_only: false,
                expected_contains: Some("SRR123456.sra"),
                should_succeed: true,
            },
            TestCase {
                name: "only full available, no fallback when lite_only=true",
                response: r#"url="https://example.com/SRR123456.sra""#,
                full_quality: false,
                lite_only: true,
                expected_contains: None,
                should_succeed: false,
            },
            TestCase {
                name: "only lite available, works with lite_only=true",
                response: r#"url="https://example.com/SRR123456.lite.sra""#,
                full_quality: false,
                lite_only: true,
                expected_contains: Some(".lite."),
                should_succeed: true,
            },
            TestCase {
                name: "only lite available, works with lite_only=false",
                response: r#"url="https://example.com/SRR123456.lite.sra""#,
                full_quality: false,
                lite_only: false,
                expected_contains: Some(".lite."),
                should_succeed: true,
            },
            TestCase {
                name: "no URLs available",
                response: "no urls here",
                full_quality: false,
                lite_only: false,
                expected_contains: None,
                should_succeed: false,
            },
        ];

        for test_case in test_cases {
            let result = parse_url_with_fallback(
                "SRR123456",
                test_case.response,
                test_case.full_quality,
                test_case.lite_only,
                Provider::Https,
            );

            if test_case.should_succeed {
                assert!(
                    result.is_some(),
                    "Test case '{}' should succeed but returned None",
                    test_case.name
                );

                if let Some(expected_substring) = test_case.expected_contains {
                    assert!(
                        result.unwrap().contains(expected_substring),
                        "Test case '{}' result should contain '{}'",
                        test_case.name,
                        expected_substring
                    );
                }
            } else {
                assert!(
                    result.is_none(),
                    "Test case '{}' should fail but returned Some",
                    test_case.name
                );
            }
        }
    }
}
