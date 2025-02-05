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

### `ncbi-vdb` is required

This makes use of the `ncbi-vdb` c-library, which is not included in this repository.
You *will* need to install this library on your system before you can build this tool.

Specifically you will require the dynamic lib `libncbi-vdb.so` to be available on your system.
This is typically installed by the `sra-tools` package, which can be installed via `apt-get`, `brew`, or `conda`.

You can check if you have the library installed by running the following command:

```bash
# If you see any output from this you should be good to go
ldconfig -p | grep libncbi-vdb
```

If you are building from source, you can follow the instructions provided by [sra-tools](https://github.com/ncbi/sra-tools/wiki/Building-from-source-:--configure-options-explained)
These instructions will guide you through cloning all 3 repositories and building them sequentially.

Once you locate the `libncbi-vdb.so` file, you can provide an environment variable to the build script to link against it.

```bash
# Clone the repository
git clone https://github.com/noamteyssier/xsra
cd xsra

# If you have the library installed in a standard location you can just build
cargo install --path .

# If installing from source you may find the library in a path like this
# ~/ncbi-outdir/ncbi-vdb/linux/gcc/x86_64/rel/lib/libncbi-vdb.so
# You can provide this path to the build script like so
export NCBI_VDB_PATH=$HOME/ncbi-outdir/ncbi-vdb/linux/gcc/x86_64/rel/lib
cargo install --path .
```

## License

MIT

## Contributing

Please feel free to open an issue or pull request if you have any suggestions or improvements.
