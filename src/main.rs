use std::collections::HashMap;
use std::fmt::Display;
use std::fs::File;
use std::io::{BufRead, BufReader};

fn round_towards_positive(mut n: f32) -> f32 {
    n = (n * 10f32).round() / 10f32;

    if n < 0f32 {
        n += 1f32;
    }

    format!("{:.1}", n).parse().unwrap()
}

#[derive(Default)]
struct Measurement {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

impl Measurement {
    fn add(&mut self, temp: f32) {
        self.min = self.min.min(temp);
        self.max = self.max.max(temp);
        self.sum += temp;
        self.count += 1;
    }
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

fn main() -> std::io::Result<()> {
    let file = File::open("measurements.txt")?;
    let mut reader = BufReader::new(file);
    let mut buffer = String::with_capacity(64);
    let mut measurements: HashMap<String, Measurement> = HashMap::with_capacity(10000);

    // Read the first two lines and discard them we don't want the legal stuff to be part of the
    // measurements
    reader.read_line(&mut buffer)?;
    buffer.clear();
    reader.read_line(&mut buffer)?;
    buffer.clear();

    let start = std::time::Instant::now();
    for line in reader.lines() {
        let line = line?;
        let (city, temp) = line.split_once(';').unwrap();
        let temp: f32 = round_towards_positive(
            temp.parse()
                .or_else(|_| -> Result<f32, ()> {
                    eprintln!("Failed to parse line: {}", line);
                    panic!();
                })
                .unwrap(),
        );

        measurements
            .entry(city.to_string())
            .or_insert_with(Measurement::default)
            .add(temp);
    }

    println!("Took: {:?}", start.elapsed());

    Ok(())
}
