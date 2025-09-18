use std::{collections::HashMap, fs::File, io::BufWriter, path::Path};

use anyhow::{Context, Result};

// compute the number of elements we can buffer for 50MB of memory usage during LAZ -> XyzRecord conversion
const LAZ_BUFFER_SIZE: usize = 50 * 1024 * 1024 / (size_of::<las::Point>());

fn main() -> Result<()> {
    // Usage: [input folder] [output folder] [tile size]
    let args: Vec<String> = std::env::args().collect();
    if args.len() != 4 {
        eprintln!(
            "Usage: {} [input folder] [output folder] [tile size]",
            args[0]
        );
        std::process::exit(1);
    }

    let input_folder = Path::new(&args[1]);
    let output_folder = Path::new(&args[2]);
    let tile_size: f64 = args[3].parse().context("parse tile size")?;

    let mut headers = Vec::new();

    // Step1: iterate over all input files and load their LAS headers to know their size
    for file in std::fs::read_dir(input_folder)? {
        let file = file?;

        if !file.file_type()?.is_file() {
            continue;
        }

        let path = file.path();

        // only process .las and .laz files
        let Some(ext) = path.extension() else {
            continue;
        };
        if ext != "las" && ext != "laz" {
            continue;
        }

        println!("Reading header for file: {}", path.display());

        let reader = las::Reader::from_path(&path).context("open LAS/LAZ file")?;
        let header = reader.header();

        println!(
            "File: {} has {} points: {:?}",
            path.display(),
            header.number_of_points(),
            header.bounds(),
        );

        headers.push((path.to_owned(), header.clone()));
    }

    let min = headers
        .iter()
        .map(|(_, h)| h.bounds().min)
        .reduce(|a, b| vector_min(&a, &b))
        .context("at least one input file")?;

    let max = headers
        .iter()
        .map(|(_, h)| h.bounds().max)
        .reduce(|a, b| vector_max(&a, &b))
        .context("at least one input file")?;

    println!("Overall bounds: min={:?}, max={:?}", min, max);
    println!(
        "Overall size: x={}, y={}, z={}",
        max.x - min.x,
        max.y - min.y,
        max.z - min.z
    );

    // make sure the files do not overlap (TODO: this is N^2, optimize?)
    let mut overlap_found = false;
    for (i, (_, h1)) in headers.iter().enumerate() {
        for (j, (_, h2)) in headers.iter().enumerate() {
            if i == j {
                continue;
            }

            if bounds_intersect(&h1.bounds(), &h2.bounds()) {
                eprintln!(
                    "Error: Input files {} and {} have overlapping bounds",
                    headers[i].0.display(),
                    headers[j].0.display()
                );
                overlap_found = true;
            }
        }
    }
    anyhow::ensure!(!overlap_found, "overlapping files found");

    // Step2: Create a plan of how to retile and which tiles that need to be read in which order

    // One file can either need to be split to multiple files, or we might need to merge multiple files into one
    // Idea: compute all output files that we will need, then open all of them for writing.
    // Then we can read each input file, and write the points to the appropriate output files.
    // or we can open the files on demand when we need them.

    let options = las::ReaderOptions::default().with_laz_parallelism(las::LazParallelism::Yes);

    // TODO: create the mapping from input to output beforehand. Automatically close files that
    // have been written completely to avoid having too many files open at once.
    // Assume the input files have points "everywhere" in their bounds.
    let mut output_files: HashMap<(i64, i64), las::Writer<BufWriter<File>>> =
        std::collections::HashMap::new();
    for (path, header) in &headers {
        // open the file for reading
        println!("Processing file: {}", path.display());
        let mut reader = las::Reader::with_options(std::fs::File::open(path)?, options)
            .expect("Could not create reader");

        let mut points = Vec::with_capacity(LAZ_BUFFER_SIZE);
        loop {
            points.clear();
            let n = reader.read_points_into(LAZ_BUFFER_SIZE as u64, &mut points)?;

            if n == 0 {
                break;
            }

            // map each point to their "new" tile

            // To reduce the number of hashmap lookups: iterate the points until
            // they no longer fit into the current tile, then do a single lookup and write all
            // points at once.

            let mut i = 0;
            while i < n as usize {
                let mut tile_index = None;
                let mut count = 0;
                for p in &points[i..] {
                    let nx = (p.x / tile_size) as i64;
                    let ny = (p.y / tile_size) as i64;

                    if let Some((tx, ty)) = tile_index {
                        if (nx, ny) != (tx, ty) {
                            // this point is in a different tile, stop here
                            break;
                        }
                    } else {
                        tile_index = Some((nx, ny));
                    }
                    count += 1;
                }

                let (nx, ny) = tile_index.context("at least one point to process")?;
                println!("Count: {count}");

                let writer = output_files.entry((nx, ny)).or_insert_with(|| {
                    // create the output file
                    let tile_path = output_folder.join(format!("tile_{}_{}.laz", nx, ny));
                    std::fs::create_dir_all(output_folder).expect("Could not create output folder");
                    println!("Creating output file: {}", tile_path.display());
                    let mut new_header = header.clone();
                    new_header.clear();
                    las::Writer::from_path(&tile_path, new_header).expect("Could not create writer")
                });

                for p in &points[i..(i + count)] {
                    writer
                        .write_point(p.clone())
                        .context("Could not write point")?;
                }
                i += count;
            }
        }
    }
    drop(output_files); // close all output files

    // Step3: Apply the plan, read the input files and write the output files (in parallel)

    Ok(())
}

fn vector_min(a: &las::Vector<f64>, b: &las::Vector<f64>) -> las::Vector<f64> {
    las::Vector {
        x: a.x.min(b.x),
        y: a.y.min(b.y),
        z: a.z.min(b.z),
    }
}

fn vector_max(a: &las::Vector<f64>, b: &las::Vector<f64>) -> las::Vector<f64> {
    las::Vector {
        x: a.x.max(b.x),
        y: a.y.max(b.y),
        z: a.z.max(b.z),
    }
}

fn bounds_intersect(a: &las::Bounds, b: &las::Bounds) -> bool {
    !(a.min.x > b.max.x
        || a.max.x < b.min.x
        || a.min.y > b.max.y
        || a.max.y < b.min.y
        || a.min.z > b.max.z
        || a.max.z < b.min.z)
}
