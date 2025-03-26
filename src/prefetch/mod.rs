use std::{
    fs::File,
    io::{BufWriter, Write},
    time::{Duration, Instant},
};

use anyhow::{bail, Result};
use futures::{stream::FuturesUnordered, StreamExt};
use indicatif::{MultiProgress, ProgressBar, ProgressStyle};
use reqwest::blocking::Client;
use tokio::time::sleep;

use crate::cli::{AccessionOptions, MultiInputOptions, Provider};

/// Rate limit in milliseconds between requests
pub const RATE_LIMIT_MS: u64 = 400;

pub fn query_entrez(accession: &str) -> Result<String> {
    let query_url = format!(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=sra&id={}&rettype=full",
        accession
    );
    let response = Client::new().get(&query_url).send()?.text()?;
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

pub fn identify_url(accession: &str, options: AccessionOptions) -> Result<String> {
    let entrez_response = query_entrez(accession)?;
    if let Some(url) = parse_url(
        accession,
        &entrez_response,
        options.full_quality,
        options.provider,
    ) {
        match options.provider {
            Provider::Https => Ok(url),
            _ => {
                bail!(
                    "Identified the {}-URL, but cannot currently proceed: {url}",
                    options.provider,
                );
            }
        }
    } else {
        bail!("Unable to identify a download URL for accession: <{accession}> with full_quality={} and provider={}",
            options.full_quality,
            options.provider,
        )
    }
}

// Rate-limited version that processes multiple accessions by calling identify_url
pub async fn identify_urls(
    accessions: &[String],
    options: AccessionOptions,
) -> Result<Vec<(String, Result<String>)>> {
    eprintln!("Identifying URLs for {} accessions...", accessions.len());

    let mut results = Vec::new();
    let mut last_request = Instant::now();

    for accession in accessions {
        eprintln!(">> Identifying URL for accession: {}", accession);
        // Rate limiting: ensure at least some time between requests (3 per second)
        let elapsed = last_request.elapsed();
        if elapsed < Duration::from_millis(RATE_LIMIT_MS) {
            sleep(Duration::from_millis(RATE_LIMIT_MS) - elapsed).await;
        }

        last_request = Instant::now();
        let result = identify_url(accession, options);
        results.push((accession.clone(), result));
    }

    Ok(results)
}

/// Download a file from a URL asynchronously
async fn download_url(url: String, path: String, pb: ProgressBar) -> Result<()> {
    let filename = url.split('/').last().unwrap_or("");
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

pub fn prefetch(input: &MultiInputOptions, output_dir: Option<&str>) -> Result<()> {
    let accessions = input.accession_set();

    if accessions.is_empty() {
        bail!("No accessions provided");
    }

    // For a single accession, use the original blocking approach
    if accessions.len() == 1 {
        let url = identify_url(&accessions[0], input.options)?;
        let path = match output_dir {
            Some(dir) => format!("{}/{}.sra", dir, &accessions[0]),
            None => format!("{}.sra", &accessions[0]),
        };

        let runtime = tokio::runtime::Runtime::new()?;
        runtime.block_on(async {
            let pb = ProgressBar::new(0);
            download_url(url, path, pb).await
        })?;

        return Ok(());
    }

    // For multiple accessions, use the rate-limited approach
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        // Step 1: Identify URLs with rate limiting
        let url_results = identify_urls(accessions, input.options).await?;

        // Step 2: Download files concurrently
        let mp = MultiProgress::new();
        let mut downloads = FuturesUnordered::new();

        for (accession, url_result) in url_results {
            match url_result {
                Ok(url) => {
                    let path = match output_dir {
                        Some(dir) => format!("{}/{}.sra", dir, accession),
                        None => format!("{}.sra", accession),
                    };

                    let pb = mp.add(ProgressBar::new(0));
                    pb.set_message(format!("Downloading {}", accession));

                    downloads.push(download_url(url, path, pb));
                }
                Err(e) => {
                    eprintln!("Error for accession {}: {}", accession, e);
                }
            }
        }

        // Process all downloads concurrently
        while let Some(result) = downloads.next().await {
            if let Err(e) = result {
                eprintln!("Download error: {}", e);
            }
        }

        Ok(())
    })
}
