# xsra

A tool to extract FASTQ records from an SRA archive.

## Overview

The NCBI Sequence Read Archive (SRA) is a repository of raw sequencing data.
The file format used by the SRA is a complicated binary database format that isn't directly readable by most bioinformatics tools.
This tool makes use of the `ncbi_vdb` c-library to interact with the SRA archive with safe abstractions.
This means the core functionality of the tool wraps unsafe C, but the majority of the code related to multi-threading, error-handlings, and overall execution is written in Rust.

This tool is designed to be a fast and more convenient replacement for the `fastq-dump` and `fasterq-dump` tools provided by the NCBI.
However, it is not a complete feature-for-feature replacement, and some functionality may be missing.

## Features

- Multi-threaded extraction of FASTQ records
- Optional compression of output files
  - gzip
  - bgzip
  - zstd
- Minimum read length filtering
- Technical / biological read segement selection
- Spot subsetting

## Limitations

- May not support every possible SRA archive layout
- Does not support all the options provided by `fastq-dump` or `fasterq-dump`
- Will not output sequence identifiers in the same format as `fastq-dump` or `fasterq-dump`
- Spot ordering is not guaranteed to be the same as the SRA archive
  - Read segments are in order to keep paired-end reads together, but the order of spots is dependent on the order of completion of the threads.
- Installation bundles a static `ncbi-vdb` and not all architectures are supported out of the box.
  - Currently only: ["linux-x86_64", "macos-aarch64"] are supported

## Usage

`xsra` expects to be run on an on-disk SRA archive.
If you have an SRA accession number, you can download the archive using the `prefetch` tool provided by the `sra-tools` package.

```bash
# Write all records to stdout
xsra <ACCESSION>.sra

# Split records into multiple files (will create an output directory and write files there)
xsra <ACCESSION>.sra -s

# Split records into multiple files and compress them (gzip)
xsra <ACCESSION>.sra -s -cg

# Split records into multiple files, compress them (zstd), and filter out reads shorter than 11bp
xsra <ACCESSION>.sra -s -cz -L 11

# Write all records to stdout but only use 4 threads and compress the output (bgzip)
xsra <ACCESSION>.sra -T4 -cb

# Write only the first 100 spots to stdout
xsra <ACCESSION>.sra -l 100
```

## Installation

```
# Clone repo
git clone https://github.com/arcinstitute/xsra
cd xsra

# Build and install
cargo install --path .

# Check that the installation was successful
xsra --help
```

## License

MIT

## Contributing

Please feel free to open an issue or pull request if you have any suggestions or improvements.
