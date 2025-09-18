
# lasretile

`lasretile` is a small Rust tool for re-tiling LAS/LAZ point cloud files into a new tile size. It efficiently splits or merges input files into rectangular tiles, making it easy to reorganize large point cloud datasets for further processing or analysis.

## Features

- Supports both LAS and LAZ formats
- Fast parallel reading and writing
- Automatically detects and prevents overlapping input files
- Progress bar for large datasets
- Simple command-line interface

## Installation

Clone the repository and build with Cargo (target the current CPU for maximum performance):

```fish
git clone https://github.com/antbern/lasretile.git
cd lasretile
RUSTFLAGS="-C target-cpu=native" cargo build --release
```

## Usage

```fish
target/release/lasretile [input folder] [output folder] [tile size]
```

- `input folder`: Directory containing LAS/LAZ files to retile
- `output folder`: Directory where new tiles will be written
- `tile size`: Tile size in the same units as the LAS/LAZ files (e.g., meters)

Example:

```fish
target/release/lasretile ./input_las ./output_tiles 100.0
```

This will read all `.las` and `.laz` files in `./input_las`, and write new tiles of size 100x100 units to `./output_tiles`.

## Output Tile Format

Each output tile is written as a compressed LAZ file (using the same format as the input files, if possible). The tile files are named as:

```
tile_<x>_<y>.laz
```

where `<x>` and `<y>` are the integer tile indices in the X and Y directions, respectively. Each file contains all points from the input files that fall within the corresponding tile bounds. The LAS/LAZ header is updated to reflect the new bounds and point count for each tile.

## How it works

1. Scans all input files and reads their headers to determine bounds and point counts.
2. Checks for overlapping input files and aborts if any are found.
3. Computes the set of output tiles needed.
4. Reads each input file in parallel, writing points to the appropriate output tile file.
5. Closes output files as soon as all contributing input files are processed.

## Requirements

- Rust 1.70+ (edition 2024)
- LAS/LAZ files (with non-overlapping bounds)

## License

MIT

---
For more details, see the source code in `src/main.rs`.
