mod stats;
mod utils;

use std::io::Write;
use std::path::Path;
use std::sync::Arc;

use anyhow::Result;
//use futures::channel::mpsc::Receiver;
//use futures::SinkExt;
use ncbi_vdb_sys::SraReader;
use parking_lot::Mutex;

use crate::cli::{DumpOutput, FilterOptions, InputOptions, OutputFormat};
use crate::output::{build_local_buffers, build_path_name, build_writers, OutputFileType};
use crate::prefetch::identify_url;
use crate::RECORD_CAPACITY;

use crate::utils::get_num_records;
use stats::ProcessStatistics;
use std::sync::mpsc::{sync_channel, Receiver, SyncSender};
use std::thread;
use utils::write_segment_to_buffer_set;

fn launch_threads<W: Write + Send + 'static>(
    path: &str,
    num_threads: u64,
    records_per_thread: u64,
    remainder: u64,
    shared_writers: Arc<Mutex<Vec<W>>>,
    filter_opts: FilterOptions,
    format: OutputFormat,
) -> Result<ProcessStatistics> {
    // Segments included in the output
    let segment_set = if filter_opts.include.is_empty() {
        None
    } else {
        // checking a small vector should be faster than a HashSet
        let set: Vec<usize> = filter_opts.include.clone();
        Some(set)
    };

    let mut handles = Vec::new();
    for i in 0..num_threads {
        let writer = Arc::clone(&shared_writers);
        let segment_set = segment_set.clone();
        let start = (i * records_per_thread) + 1;
        let stop = if i == num_threads - 1 {
            start + records_per_thread + remainder - 1
        } else {
            start + records_per_thread - 1
        };
        let path = path.to_string();

        let handle = std::thread::spawn(move || -> Result<ProcessStatistics> {
            let reader = SraReader::new(&path)?;

            // Initialize local buffers and counters
            let mut stats = ProcessStatistics::default();
            let mut local_buffers = build_local_buffers(&writer.lock());
            let mut counts = vec![0; local_buffers.len()];

            // Iterate over record spots and write to buffers
            for (idx, record) in reader.into_range_iter(start as i64, stop)?.enumerate() {
                let record = record?;

                // Iterate over segments in the record
                for segment in record.into_iter() {
                    // Skip segment if outside of set
                    if let Some(ref set) = segment_set {
                        if !set.contains(&segment.sid()) {
                            continue;
                        }
                    }

                    // Skip technical segments if required
                    if filter_opts.skip_technical && segment.is_technical() {
                        // Increment filter statistics
                        stats.inc_filter_type(segment.sid());
                        continue;
                    }

                    // Skip reads if they are under the minimum read length
                    if segment.len() < filter_opts.min_read_len {
                        // Increment filter statistics
                        stats.inc_filter_size(segment.sid());
                        continue;
                    }

                    // Write the segment to the record set
                    write_segment_to_buffer_set(&mut local_buffers, &segment, format)?;

                    if counts.len() == 1 {
                        counts[0] += 1;
                    } else {
                        counts[segment.sid()] += 1;
                    }

                    // Increment read statistics
                    stats.inc_reads(segment.sid());
                }

                // Handle buffer writes at specific intervals
                if idx % RECORD_CAPACITY == 0 {
                    // Lock the writer until all buffers are written
                    let mut writer = writer.lock();

                    // Write all buffers to the writer
                    for ((global_buf, local_buf), local_count) in writer
                        .iter_mut()
                        .zip(local_buffers.iter_mut())
                        .zip(counts.iter_mut())
                    {
                        if *local_count == 0 {
                            continue;
                        }
                        global_buf.write_all(local_buf)?;
                        local_buf.clear();
                        *local_count = 0;
                    }
                }

                // Increment record statistics
                stats.inc_spots();
            }

            // Write remaining buffers to shared writer
            let mut writer = writer.lock();
            for (i, buffer) in local_buffers.iter_mut().enumerate() {
                writer[i].write_all(buffer)?;
                buffer.clear();
            }

            // Return thread-specific statistics
            Ok(stats)
        });

        // Collect all thread handles
        handles.push(handle);
    }

    // Collect all statistics
    let mut stats = ProcessStatistics::default();
    for handle in handles {
        let thread_stats = handle.join().expect("Thread panicked")?;
        stats = stats + thread_stats;
    }

    // Flush all writers
    let mut writer = shared_writers.lock();
    for buffer in writer.iter_mut() {
        buffer.flush()?;
    }

    Ok(stats)
}

fn launch_threads_fifo<W: Write + Send + 'static>(
    path: &str,
    num_threads: u64,
    records_per_thread: u64,
    remainder: u64,
    shared_writers: Arc<Mutex<Vec<W>>>,
    filter_opts: FilterOptions,
    format: OutputFormat,
) -> Result<ProcessStatistics> {
    // Segments included in the output
    let segment_set = if filter_opts.include.is_empty() {
        None
    } else {
        // checking a small vector should be faster than a HashSet
        let set: Vec<usize> = filter_opts.include.clone();
        Some(set)
    };

    // note that right now we will launch async writer threads per output segment
    // this can mess with the total thread count, so we should think about the best
    // way to deal with this.
    let (s0, r0): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(4 * num_threads as usize);
    let (s1, r1): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(4 * num_threads as usize);
    let (s2, r2): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(4 * num_threads as usize);
    let (s3, r3): (SyncSender<Vec<u8>>, Receiver<Vec<u8>>) = sync_channel(4 * num_threads as usize);

    let receivers = vec![r0, r1, r2, r3];
    let senders = Arc::new(Mutex::new(vec![s0, s1, s2, s3]));
    let mut writers = shared_writers.lock();
    let stats_res = thread::scope(|sc| {
        let mut handles = Vec::new();

        let wslice = Arc::new(vec![0; writers.len()]);
        let receiver_threads = receivers
            .into_iter()
            .zip(writers.iter_mut())
            .map(|(r, w)| {
                sc.spawn(|| {
                    for buf in r {
                        w.write_all(buf.as_slice())?;
                    }
                    // flush writers at the end
                    w.flush()?;
                    Ok(())
                })
            })
            .collect::<Vec<std::thread::ScopedJoinHandle<anyhow::Result<()>>>>();

        for i in 0..num_threads {
            let segment_set = segment_set.clone();
            let senders = Arc::clone(&senders);

            let start = (i * records_per_thread) + 1;
            let stop = if i == num_threads - 1 {
                start + records_per_thread + remainder - 1
            } else {
                start + records_per_thread - 1
            };
            let path = path.to_string();
            let wslice = Arc::clone(&wslice);

            let handle = std::thread::spawn(move || -> Result<ProcessStatistics> {
                let reader = SraReader::new(&path)?;

                // Initialize local buffers and counters
                let mut stats = ProcessStatistics::default();
                let mut local_buffers = build_local_buffers(&wslice);
                let mut counts = vec![0; local_buffers.len()];
                let senders = senders.clone();

                // Iterate over record spots and write to buffers
                for (idx, record) in reader.into_range_iter(start as i64, stop)?.enumerate() {
                    let record = record?;

                    // Iterate over segments in the record
                    for segment in record.into_iter() {
                        // Skip segment if outside of set
                        if let Some(ref set) = segment_set {
                            if !set.contains(&segment.sid()) {
                                continue;
                            }
                        }

                        // Skip technical segments if required
                        if filter_opts.skip_technical && segment.is_technical() {
                            // Increment filter statistics
                            stats.inc_filter_type(segment.sid());
                            continue;
                        }

                        // Skip reads if they are under the minimum read length
                        if segment.len() < filter_opts.min_read_len {
                            // Increment filter statistics
                            stats.inc_filter_size(segment.sid());
                            continue;
                        }

                        // Write the segment to the record set
                        write_segment_to_buffer_set(&mut local_buffers, &segment, format)?;

                        if counts.len() == 1 {
                            counts[0] += 1;
                        } else {
                            counts[segment.sid()] += 1;
                        }

                        // Increment read statistics
                        stats.inc_reads(segment.sid());
                    }

                    // Handle buffer writes at specific intervals
                    if idx > 0 && (idx % RECORD_CAPACITY == 0) {
                        // Lock the writer until all buffers are written
                        let mut senders = senders.lock();
                        for ((sender, local_buf), local_count) in senders
                            .iter_mut()
                            .zip(local_buffers.iter_mut())
                            .zip(counts.iter_mut())
                        {
                            if *local_count == 0 {
                                continue;
                            }

                            sender.send(local_buf.clone())?;
                            local_buf.clear();
                            *local_count = 0;
                        }
                    }

                    // Increment record statistics
                    stats.inc_spots();
                }

                // Write remaining buffers to shared writer
                let mut senders = senders.lock();
                for (sender, local_buf) in senders.iter_mut().zip(local_buffers.iter_mut()) {
                    if local_buf.is_empty() {
                        continue;
                    }
                    sender.send(local_buf.clone())?;
                    local_buf.clear();
                }
                drop(senders);
                // Return thread-specific statistics
                Ok(stats)
            });

            // Collect all thread handles
            handles.push(handle);
        }

        // Collect all statistics
        let mut stats = ProcessStatistics::default();
        for handle in handles {
            let thread_stats = handle.join().expect("Thread panicked")?;
            stats = stats + thread_stats;
        }

        drop(senders);
        for rec in receiver_threads {
            rec.join().expect("Thread panicked")?;
        }

        Ok(stats)
    });

    stats_res
}

pub fn dump(
    input: &InputOptions,
    num_threads: u64,
    output_opts: &DumpOutput,
    filter_opts: FilterOptions,
) -> Result<()> {
    let accession = if !Path::new(&input.accession).exists() {
        eprintln!(
            "Identifying SRA data URL for Accession: {}",
            &input.accession
        );
        let url = identify_url(&input.accession, &input.options)?;
        eprintln!("Streaming SRA records from URL: {}", url);
        url
    } else {
        input.accession.to_string()
    };

    let num_records = get_num_records(&accession)?;

    // Adjust the number of records to process if a limit is provided
    let num_records = if let Some(limit) = filter_opts.limit {
        if limit > num_records {
            eprintln!("Warning: Provided spot limit ({}) is greater than the actual number of spots ({}). Will process the full archive.",
                limit, num_records);
        }
        num_records.min(limit)
    } else {
        num_records
    };

    // Calculate records per thread and remainder (final thread)
    let records_per_thread = num_records / num_threads;
    let remainder = num_records % num_threads;

    // Build writers depending on split requirements
    let writers = if output_opts.split {
        build_writers(
            Some((&output_opts.outdir, output_opts.named_pipes)),
            &output_opts.prefix,
            output_opts.compression,
            output_opts.format,
            num_threads as usize,
            &filter_opts,
        )
    } else {
        build_writers(
            None,
            &output_opts.prefix,
            output_opts.compression,
            output_opts.format,
            num_threads as usize,
            &filter_opts,
        )
    }?;
    let shared_writers = Arc::new(Mutex::new(writers));

    let included_segs = filter_opts.include.clone();
    // Launch worker threads
    let stats = if output_opts.named_pipes {
        launch_threads_fifo(
            &accession,
            num_threads,
            records_per_thread,
            remainder,
            shared_writers,
            filter_opts,
            output_opts.format,
        )?
    } else {
        launch_threads(
            &accession,
            num_threads,
            records_per_thread,
            remainder,
            shared_writers,
            filter_opts,
            output_opts.format,
        )?
    };

    // Remove empty files
    if output_opts.split {
        let wrap = |x| {
            if output_opts.named_pipes {
                OutputFileType::NamedPipe(x)
            } else {
                OutputFileType::RegularFile(x)
            }
        };
        stats.reads_per_segment.iter().enumerate().try_for_each(
            |(seg_id, &count)| -> Result<()> {
                // if included_segs was non-empty, so we are applying a filter
                // and this segment was not included, then we didn't create a real
                // file for it.
                if !included_segs.is_empty() && !included_segs.contains(&seg_id) {
                    return Ok(());
                }
                if count == 0 || output_opts.named_pipes {
                    let path = build_path_name(
                        wrap(&output_opts.outdir),
                        &output_opts.prefix,
                        output_opts.compression,
                        output_opts.format,
                        seg_id,
                    );
                    if output_opts.keep_empty {
                        eprintln!("Warning => empty path: {}", path);
                    } else {
                        eprintln!("Removing empty path: {}", path);
                        std::fs::remove_file(path)?;
                    }
                }
                Ok(())
            },
        )?;
    }

    // Print all statistics
    stats.pprint(&mut std::io::stderr())?;

    Ok(())
}
