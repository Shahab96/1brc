use std::{collections::HashMap, fmt::Display, io::Read};

/*
 * Allowing this crate because it's a compile time constant and is required
 * for testing across different machines.
 */
const ONE_BILLION: usize = 1000000000;

#[derive(Debug, Default)]
struct Measurement {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

impl Display for Measurement {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:.1}/{:.1}/{:.1}",
            self.min,
            self.max,
            self.sum / self.count as f32
        )
    }
}

fn round_towards_positive(mut n: f32) -> f32 {
    n = (n * 10f32).round() / 10f32;

    if n < 0f32 {
        n += 1f32;
    }

    format!("{:.1}", n).parse().unwrap()
}

fn process_lines<'a>(contents: &'a str) -> HashMap<&'a str, Measurement> {
    let mut measurements = HashMap::<&str, Measurement>::with_capacity(10000);

    contents.lines().for_each(|line| {
        let (city, measurement) = line.split_once(';').unwrap_or_else(|| {
            panic!("Failed to parse line: {}", line);
        });
        let measurement = round_towards_positive(measurement.parse().unwrap_or_else(|e| {
            panic!("Failed to parse measurement: {}, {:?}", measurement, e);
        }));

        let item = measurements.entry(city).or_default();
        item.min = item.min.min(measurement);
        item.max = item.max.max(measurement);
        item.sum += measurement;
        item.count += 1;
    });

    measurements
}

fn main() -> std::io::Result<()> {
    let cores = std::process::Command::new("nproc")
        .output()
        .expect("Failed to get number of cores")
        .stdout;

    let cores = String::from_utf8(cores)
        .expect("Failed to parse number of cores")
        .trim()
        .parse::<usize>()
        .expect("Failed to parse number of cores");

    println!("{} cores", cores);

    let file = std::fs::File::open("measurements.txt");
    let mut reader = std::io::BufReader::new(file?);
    let mut contents = String::with_capacity(ONE_BILLION);
    let _ = reader.read_to_string(&mut contents);
    let contents: &'static str = contents.leak();
    let chunk_size = contents.len() / cores;
    let mut measurements = HashMap::<&str, Measurement>::with_capacity(10000);

    let mut ptr = 0;
    let mut end = chunk_size;
    println!("Beginning processing");

    let start = std::time::Instant::now();
    let handles = (0..cores)
        .map(|_| {
            if end > contents.len() {
                println!("EOF");
                end = contents.len();
            }

            let newline = contents[ptr..end].rfind('\n').unwrap() + ptr;
            let slice = &contents[ptr..newline];

            ptr = newline + 1;
            end = ptr + chunk_size;

            std::thread::spawn(move || process_lines(slice))
        })
        .collect::<Vec<_>>();

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

    let mut results = Vec::<(&str, Measurement)>::with_capacity(10000);

    for (city, measurement) in measurements {
        results.push((city, measurement));
    }

    results.sort_by(|a, b| a.0.cmp(b.0));

    print!("{{");
    results.iter().for_each(|(city, measurement)| {
        print!("{}={},", city, measurement);
    });
    // Removing the trailing comma
    print!("\x08}}");

    println!("Processing took: {:?}", start.elapsed());

    Ok(())
}
