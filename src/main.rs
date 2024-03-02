use std::{
    borrow::Cow,
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

#[derive(Debug, Default, Clone, Copy)]
struct Measurement {
    min: f64,
    max: f64,
    sum: f64,
    count: u32,
}

impl Measurement {
    #[inline(always)]
    fn record(&mut self, measurement: f64) {
        self.min = self.min.min(measurement);
        self.max = self.max.max(measurement);
        self.sum += measurement;
        self.count += 1;
    }

    #[inline(always)]
    fn aggregate(&mut self, other: &Measurement) {
        self.min = self.min.min(other.min);
        self.max = self.max.max(other.max);
        self.sum += other.sum;
        self.count += other.count;
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
    n = (n * 10.0).round() / 10.0;

    if n < 0.0 {
        n += 1.0;
    }

    n
}

#[inline(always)]
fn process_lines(contents: Vec<u8>, start: Instant) -> HashMap<&'static str, Measurement> {
    println!("Read {} bytes in {:?}", contents.len(), start.elapsed());
    let contents = std::str::from_utf8(contents.leak()).unwrap();
    let measurement_template = Cow::Owned(Measurement::default());
    let mut measurements = HashMap::<&'static str, Measurement>::with_capacity(10000);

    for line in contents.lines() {
        let (city, measurement) = line.split_once(';').unwrap_or_else(|| {
            panic!("Failed to parse line: {}", line);
        });
        let measurement = round_towards_positive(measurement.parse().unwrap_or_else(|e| {
            panic!("Failed to parse measurement: {}, {:?}", measurement, e);
        }));

        let Some(item) = measurements.get_mut(city) else {
            measurements.insert(city, *measurement_template);
            continue;
        };

        item.record(measurement);
    }

    println!(
        "Thread {} processed in {:?}",
        std::thread::current().name().unwrap(),
        start.elapsed()
    );
    measurements
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
        .mode(16384)
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
        .map(|thread_count| {
            let start = std::time::Instant::now();
            let mut buf = Vec::with_capacity(chunk_size + 100);
            let bytes = reader.fill_buf().unwrap();
            buf.extend_from_slice(bytes);

            if buf.last() != Some(&b'\n') {
                reader.read_until(b'\n', &mut buf).unwrap();
            }

            std::thread::Builder::new()
                .name(format!("worker-{}", thread_count))
                .spawn(move || process_lines(buf, start))
                .unwrap()
        })
        .collect::<Vec<_>>();

    // Perform memory allocation while waiting for the threads to finish
    let mut measurements = HashMap::<&str, Measurement>::with_capacity(10000);

    // Put it on the stack!
    let mut results = [("", Measurement::default()); 10000];

    for handle in handles {
        let result = handle.join().unwrap();

        // While we're waiting for the threads to finish, we can perform the aggregation
        for (city, measurement) in result {
            measurements
                .entry(city)
                .and_modify(|m| m.aggregate(&measurement))
                .or_insert(measurement);
        }
    }

    for (idx, (city, measurement)) in measurements.iter().enumerate() {
        results[idx] = (city, *measurement);
    }

    results.sort_by(|a, b| a.0.cmp(b.0));

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
