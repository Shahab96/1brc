use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    io::BufRead,
};

// Flip this around to see performance differences on different machines
// Don't go too high! You'll start getting pointer overlap problems!
// Increasing reduces memory usage, but also reduces cpu saturation
// Decreasinve increases memory usage, but also increases cpu saturation
// Use 1 to fully saturate the cpu
const PARALLELISM_CONSTANT: usize = 1;

#[derive(Debug, Default)]
struct Measurement {
    min: f64,
    max: f64,
    sum: f64,
    count: u32,
}

impl Display for Measurement {
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

fn round_towards_positive(mut n: f64) -> f64 {
    n = (n * 10.0).round() / 10.0;

    if n < 0.0 {
        n += 1.0;
    }

    n
}

fn process_lines(contents: String) -> HashMap<String, Measurement> {
    let mut measurements = HashMap::<String, Measurement>::with_capacity(10000);

    contents.lines().for_each(|line| {
        let (city, measurement) = line.split_once(';').unwrap_or_else(|| {
            panic!("Failed to parse line: {}", line);
        });
        let measurement = round_towards_positive(measurement.parse().unwrap_or_else(|e| {
            panic!("Failed to parse measurement: {}, {:?}", measurement, e);
        }));

        let item = measurements.entry(city.to_string()).or_default();
        item.min = item.min.min(measurement);
        item.max = item.max.max(measurement);
        item.sum += measurement;
        item.count += 1;
    });

    measurements
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let available_parallelism = std::thread::available_parallelism().unwrap().get();

    println!(
        "Parallelism: {} with parallelism multiplier: {}",
        available_parallelism, PARALLELISM_CONSTANT
    );

    let available_parallelism = available_parallelism * PARALLELISM_CONSTANT;

    let file = std::fs::File::open("measurements.txt").unwrap();

    // Tell the compiler to treat the output as a usize
    // This allows us to avoid runtime type conversion.
    let file_size = file.metadata()?.len() as usize;

    let chunk_size = file_size / available_parallelism;

    let mut reader = std::io::BufReader::with_capacity(chunk_size, file);

    let handles = (0..available_parallelism)
        .map(|_| {
            let mut buf = Vec::with_capacity(chunk_size);
            let bytes = reader.fill_buf().unwrap();
            buf.extend_from_slice(bytes);

            if !buf.ends_with(&[b'\n']) {
                // Discard the result to prevent branching
                let _ = reader.read_until(b'\n', &mut buf);
            }

            println!("Read {} bytes", buf.len());

            let buf = String::from_utf8(buf).unwrap();

            std::thread::spawn(move || process_lines(buf))
        })
        .collect::<Vec<_>>();

    // Perform memory allocation while waiting for the threads to finish
    let mut measurements = HashMap::<String, Measurement>::with_capacity(10000);
    let mut results = Vec::<(String, Measurement)>::with_capacity(10000);

    for handle in handles {
        let result = handle.join().unwrap();

        for (city, measurement) in result {
            let item = measurements.entry(city).or_default();
            item.min = measurement.min.min(measurement.min);
            item.max = measurement.max.max(measurement.max);
            item.sum += measurement.sum;
            item.count += measurement.count;
        }
    }

    for (city, measurement) in measurements {
        results.push((city, measurement));
    }

    results.sort_by(|a, b| a.0.cmp(&b.0));

    print!("{{");
    results.iter().for_each(|(city, measurement)| {
        print!("{}={},", city, measurement);
    });
    // Removing the trailing comma
    print!("\x08}}");

    Ok(())
}
