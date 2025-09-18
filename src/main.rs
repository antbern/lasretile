use std::{
    collections::{HashMap, HashSet},
    fs::File,
    io::BufWriter,
    path::Path,
};

use anyhow::{Context, Result};

// compute the number of elements we can buffer for 200MB of memory usage during LAZ/LAS reading
const LAZ_BUFFER_SIZE: usize = 200 * 1024 * 1024 / (size_of::<las::Point>());

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

        let reader = las::Reader::from_path(&path)
            .with_context(|| format!("open LAS/LAZ file: {}", path.display()))?;

        let header = reader.header();
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

    let total_points: u64 = headers.iter().map(|(_, h)| h.number_of_points()).sum();

    println!(
        "Found {} input files with a total {}M points.",
        headers.len(),
        total_points / 1_000_000
    );

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

    // Create the mapping from input to output beforehand. Automatically close files that
    // have been written completely to avoid having too many files open at once.
    // Assume the input files have points "everywhere" in their bounds.
    let mut output_files: HashMap<(i32, i32), OutTile> = std::collections::HashMap::new();
    for (i, (_, header)) in headers.iter().enumerate() {
        // since each tile is rectangular, we can compute the range of tiles that this file intersects and make sure they are instantiated
        let bounds = header.bounds();

        let min_x = (bounds.min.x / tile_size) as i32;
        let max_x = (bounds.max.x / tile_size) as i32;
        let min_y = (bounds.min.y / tile_size) as i32;
        let max_y = (bounds.max.y / tile_size) as i32;

        for tx in min_x..=max_x {
            for ty in min_y..=max_y {
                let tile = output_files.entry((tx, ty)).or_insert_with(|| OutTile {
                    tile_index: (tx, ty),
                    input_files: HashSet::new(),
                    writer: None,
                });
                tile.input_files.insert(i);
            }
        }
    }

    println!("Output files to create: {}", output_files.len(),);

    let pb = indicatif::ProgressBar::new(total_points);
    pb.set_style(indicatif::ProgressStyle::with_template("{spinner:.green} [{elapsed_precise}] [{msg}] [{wide_bar:.cyan/blue}] {human_pos}/{human_len} ({percent}%) ({eta})")
        .unwrap()
        .with_key("eta", |state: &indicatif::ProgressState, w: &mut dyn std::fmt::Write| write!(w, "{:.1}s", state.eta().as_secs_f64()).unwrap())
        .progress_chars("#>-"));
    let mut processed_points = 0;
    for (i_file, (path, header)) in headers.iter().enumerate() {
        pb.set_message(format!("{}/{}", i_file + 1, headers.len()));

        // open the file for reading
        let mut reader = las::Reader::with_options(std::fs::File::open(path)?, options)
            .expect("Could not create reader");

        // read LAZ_BUFFER_SIZE points at a time, this allows the reading to happen in parallel
        let mut points = Vec::with_capacity(LAZ_BUFFER_SIZE);
        loop {
            points.clear();
            let n = reader.read_points_into(LAZ_BUFFER_SIZE as u64, &mut points)?;

            if n == 0 {
                break;
            }

            // To reduce the number of hashmap lookups: iterate the points until
            // they no longer fit into the current tile, then do a single lookup and write all
            // points at once.
            let mut i = 0;
            while i < n as usize {
                let mut tile_index = None;
                let mut count = 0;
                for p in &points[i..] {
                    let nx: i32 = (p.x / tile_size) as i32;
                    let ny: i32 = (p.y / tile_size) as i32;

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

                let writer = output_files
                    .get_mut(&(nx, ny))
                    .context("tile should exist")?
                    .get_writer(output_folder, header)
                    .context("Could not get writer")?;

                for p in &points[i..(i + count)] {
                    writer
                        .write_point(p.clone())
                        .context("Could not write point")?;
                }
                i += count;
                processed_points += count as u64;
                pb.set_position(processed_points);
            }
        }

        // finished reading this input file, we should remove it from any output files and close
        // any output files that are now complete

        output_files.retain(|_, tile| {
            // remove the file we just processed from the list
            tile.input_files.remove(&i_file);

            // drop this entry if it has no more input files
            !tile.input_files.is_empty()
        });
    }
    pb.finish_with_message("Done");

    // make sure all output files are closed
    anyhow::ensure!(output_files.is_empty(), "all output files should be closed");

    Ok(())
}

struct OutTile {
    /// the index of this tile
    tile_index: (i32, i32),

    /// The input files that contribute to this tile
    input_files: HashSet<usize>,

    /// The writer to this file, might be None if not opened yet
    writer: Option<las::Writer<BufWriter<File>>>,
}

impl OutTile {
    pub fn get_writer(
        &mut self,
        output_folder: &Path,
        header: &las::Header,
    ) -> Result<&mut las::Writer<BufWriter<File>>> {
        if self.writer.is_none() {
            let tile_path = output_folder.join(format!(
                "tile_{}_{}.laz",
                self.tile_index.0, self.tile_index.1
            ));
            let mut new_header = header.clone();
            new_header.clear();

            let new_writer = las::Writer::from_path(&tile_path, new_header)
                .context("Could not create writer")?;

            let writer = self.writer.insert(new_writer);
            return Ok(writer);
        }
        // we know writer is Some here
        Ok(self.writer.as_mut().expect("unreachable"))
    }
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
