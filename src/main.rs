#![feature(rustc_private, vec_push_within_capacity)]
extern crate libc;

use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Write},
    os::unix::fs::OpenOptionsExt,
    time::Instant,
};

// Flip this around to see performance differences on different machines
// Don't go too high! You'll start getting pointer overlap problems!
// Increasing reduces memory usage, but also reduces cpu saturation
// Decreasinve increases memory usage, but also increases cpu saturation
// Use 1 to fully saturate the cpu
const PARALLELISM_CONSTANT: usize = 1;

#[derive(Default, Clone, Copy)]
struct Measurement {
    min: f64,
    max: f64,
    sum: f64,
    count: u32,
}

impl Measurement {
    #[inline(always)]
    fn record(&mut self, measurement: f64) {
        if measurement < self.min {
            self.min = measurement;
        }

        if measurement > self.max {
            self.max = measurement;
        }

        self.sum += measurement;
        self.count += 1;
    }

    #[inline(always)]
    fn aggregate(&mut self, other: &Measurement) {
        if other.min < self.min {
            self.min = other.min;
        }

        if other.max > self.max {
            self.max = other.max;
        }

        self.sum += other.sum;
        self.count += other.count;
    }
}

impl From<f64> for Measurement {
    #[inline(always)]
    fn from(measurement: f64) -> Self {
        Self {
            min: measurement,
            max: measurement,
            sum: measurement,
            count: 1,
        }
    }
}

impl std::fmt::Debug for Measurement {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}", self)
    }
}

impl Display for Measurement {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.1}/{:.1}/{:.1}",
            self.min,
            self.max,
            self.sum / self.count as f64
        )
    }
}

#[inline(always)]
fn round_towards_positive(mut n: f64) -> f64 {
    if n < 0.0 {
        // For negative numbers, we adjust the logic to ensure we are "rounding towards positive"
        // We invert the number, perform the rounding, and then invert it back
        n = -((-n * 10.0).ceil() / 10.0);
    } else {
        n = (n * 10.0).ceil() / 10.0;
    }

    n
}

#[inline(always)]
fn parse_line<'a>(line: &'a str) -> (&'a str, f64) {
    for character in line.char_indices().rev() {
        if character.1.eq(&';') {
            let (city, measurement) = line.split_at(character.0);

            return (
                city,
                round_towards_positive(measurement[1..].parse().unwrap()),
            );
        }
    }

    // We know that the input is well formed, so this is unreachable
    unreachable!()
}

#[inline(always)]
fn process_lines(contents: String) -> impl Iterator<Item = (String, Measurement)> {
    let start = Instant::now();
    let contents = contents.leak();
    let mut measurements = HashMap::<&str, Measurement>::with_capacity(10000);

    for line in contents.lines() {
        let (city, measurement) = parse_line(line);

        let Some(item) = measurements.get_mut(city) else {
            measurements.insert(city, Measurement::from(measurement));
            continue;
        };

        item.record(measurement);
    }

    measurements.shrink_to_fit();

    println!(
        "Processed {} lines in {:?}",
        contents.lines().count(),
        start.elapsed()
    );
    measurements.into_iter().map(|(k, v)| (k.to_string(), v))
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    /*
     * Get the number of available cores on the machine
     */
    let available_parallelism = std::thread::available_parallelism().unwrap().get();

    println!(
        "Parallelism: {} with parallelism multiplier: {}",
        available_parallelism, PARALLELISM_CONSTANT
    );

    let available_parallelism = available_parallelism * PARALLELISM_CONSTANT;

    let file = File::options()
        .read(true)
        .mode(libc::O_DIRECT as u32)
        .open("measurements.txt")
        .unwrap();

    /*
     * Tell the compiler to treat the output as a usize
     * This allows us to avoid runtime type conversion.
     */
    let file_size = file.metadata()?.len() as usize;

    let chunk_size = file_size / available_parallelism;

    let mut reader = BufReader::with_capacity(chunk_size, file);

    let handles = (0..available_parallelism)
        .map(|_| {
            let start = Instant::now();
            let mut buf = Vec::with_capacity(chunk_size + 100);
            let bytes = reader.fill_buf().unwrap();
            buf.extend_from_slice(bytes);
            let mut buf = unsafe { String::from_utf8_unchecked(buf) };
            reader.read_line(&mut buf).unwrap();

            println!("Read {} bytes in {:?}", buf.len(), start.elapsed());

            std::thread::spawn(move || process_lines(buf))
        })
        .collect::<Vec<_>>();

    // Perform memory allocation while waiting for the threads to finish
    let mut measurements = HashMap::<String, Measurement>::with_capacity(10000);
    let mut results = Vec::<(String, Measurement)>::with_capacity(10000);

    for handle in handles {
        let result = handle.join().unwrap();

        // While we're waiting for the threads to finish, we can perform the aggregation
        for (city, measurement) in result {
            let Some(item) = measurements.get_mut(&city) else {
                measurements.insert(city, measurement);
                continue;
            };

            item.aggregate(&measurement);
        }
    }

    for measurement in measurements.into_iter() {
        results.push_within_capacity(measurement).unwrap();
    }

    results.sort_by(|a, b| a.0.cmp(&b.0));

    // Create a buffer to write to stdout, this is faster than writing to stdout directly
    let stdout = std::io::stdout();
    let mut handle = stdout.lock();
    let mut writer = BufWriter::new(&mut handle);

    // Write the buffer to stdout
    writer.write(b"{").unwrap();
    for (city, measurement) in results {
        writer
            .write_fmt(format_args!("{}={},", city, measurement))
            .unwrap();
    }
    // Removing the trailing comma
    writer.write(b"\x08}").unwrap();
    writer.flush().unwrap();

    Ok(())
}
