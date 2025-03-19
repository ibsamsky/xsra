use std::{
    fs::File,
    io::{BufWriter, Write},
};

use anyhow::{bail, Result};
use futures::StreamExt;
use indicatif::{ProgressBar, ProgressStyle};
use reqwest::blocking::Client;

pub fn query_entrez(accession: &str) -> Result<String> {
    let query_url = format!(
        "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/efetch.fcgi?db=sra&id={}&rettype=runinfo",
        accession
    );
    let response = Client::new().get(&query_url).send()?.text()?;
    Ok(response)
}

pub fn parse_url(accession: &str, response: &str) -> Option<String> {
    for line in response.split('\n') {
        if line.contains("<download_path>") && line.contains(accession) {
            let url = line
                .replace("<download_path>", "")
                .replace("</download_path>", "");
            return Some(url);
        }
    }
    None
}

pub fn identify_url(accession: &str) -> Result<String> {
    let entrez_response = query_entrez(accession)?;
    if let Some(url) = parse_url(accession, &entrez_response) {
        Ok(url)
    } else {
        bail!("Unable to identify a download URL for accession: {accession}")
    }
}

/// Download a file from a URL asynchronously
async fn download_url(url: &str, path: &str, pb: ProgressBar) -> Result<()> {
    let filename = url.split('/').last().unwrap_or("");
    let client = reqwest::Client::new()
        .get(url)
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

pub fn prefetch(accession: &str, output: Option<&str>) -> Result<()> {
    let url = identify_url(accession)?;
    let path = output
        .map(String::from)
        .unwrap_or_else(|| format!("{}.sra", accession));
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let pb = ProgressBar::new(0);
        download_url(&url, &path, pb).await
    })?;
    Ok(())
}
