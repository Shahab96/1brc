use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    io::BufRead,
};

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
    let available_parallelism: usize = std::thread::available_parallelism()?.into();

    println!("Parallelism: {}", available_parallelism);

    let mut measurements = HashMap::<String, Measurement>::with_capacity(10000);
    let file = std::fs::File::open("measurements.txt")?;
    let file_size: usize = file.metadata()?.len().try_into()?;
    let chunk_size = file_size / available_parallelism;
    let mut reader = std::io::BufReader::with_capacity(chunk_size, file);

    let handles = (0..available_parallelism)
        .map(|_| {
            let mut buf = Vec::with_capacity(chunk_size);
            let bytes = reader.fill_buf().expect("Failed to read into buffer");
            buf.extend_from_slice(bytes);

            if !buf.ends_with(&[b'\n']) {
                reader
                    .read_until(b'\n', &mut buf)
                    .expect("Failed to read until newline");
            }

            println!("Read {} bytes", buf.len());

            let buf = String::from_utf8(buf).expect("Failed to convert buffer to string");

            std::thread::spawn(move || process_lines(buf))
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

    let mut results = Vec::<(String, Measurement)>::with_capacity(10000);

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
