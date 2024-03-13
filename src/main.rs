use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    fs::File,
    io::{BufWriter, Read, Seek, SeekFrom, Write},
    time::Instant,
};

#[derive(Default)]
struct Measurement {
    min: f32,
    max: f32,
    sum: f32,
    count: u32,
}

impl Measurement {
    #[inline(always)]
    fn record(&mut self, measurement: f32) {
        /*
         * This may seem ridiculous, as we can just do something like
         * ```
         * self.min.min(other.min)
         * ```
         *
         * However, this is a performance optimization. When we use the min/max functions,
         * we are calling a function, which means that we are pushing a new frame onto the stack.
         * We are also returning a value, which would be assigned to the existing value.
         *
         * The min and max values will only change infrequently, so we want to avoid performing
         * a memcpy when the values haven't changed.
         */
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

impl From<f32> for Measurement {
    #[inline(always)]
    fn from(measurement: f32) -> Self {
        Self {
            min: measurement,
            max: measurement,
            sum: measurement,
            count: 1,
        }
    }
}

impl Display for Measurement {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{:.1}",
            self.min,
            self.max,
            self.sum / self.count as f32
        )
    }
}

#[inline(always)]
fn round_towards_positive(mut n: f32) -> f32 {
    n *= 10.0;
    if n < 0.0 {
        // For negative numbers we round up, for rounding towards positive
        n = n.ceil();
    } else {
        n = n.round();
    }

    n / 10.0
}

#[inline(always)]
// We're manually implementing the search for our delimiter because we know that the measurements
// are always to 1 decimal place. This means we can search from the end of the string and skip
// the last 4 bytes, as they will be the minimum possible measurement eg (0.0).
fn split_line<'a>(line: &'a str) -> (&'a str, &'a str) {
    let bytes = line.as_bytes();
    let mut i = bytes.len() - 4;

    while bytes[i] != b';' {
        i -= 1;
    }

    (&line[..i], &line[i + 1..])
}

#[inline(always)]
fn parse_line<'a>(line: &'a str) -> (&'a str, f32) {
    // We know that the measurements are all to 1 decimal place. This means that
    // if we search from the end of the string we will find the ; significantly faster.
    let (city, measurement) = split_line(line);

    (city, round_towards_positive(measurement.parse().unwrap()))
}

#[inline(always)]
fn process_lines<'a>(contents: &'a str) -> impl Iterator<Item = (&'a str, Measurement)> {
    let mut measurements = HashMap::<&str, Measurement>::with_capacity(10000);
    let mut line_count = 0u32;
    let start = Instant::now();

    for line in contents.lines() {
        let (city, measurement) = parse_line(line);

        let Some(item) = measurements.get_mut(city) else {
            measurements.insert(city, Measurement::from(measurement));
            continue;
        };

        item.record(measurement);

        line_count += 1;
    }

    let end = start.elapsed();

    println!(
        "Processed {} lines in {:?}, averaging {:?} per line",
        line_count,
        end,
        end / line_count
    );

    measurements.into_iter()
}

#[inline(always)]
fn memory_map(available_parallelism: usize) -> Vec<(i64, i64)> {
    let mut file = File::open("measurements.txt").unwrap();

    /*
     * Tell the compiler to treat the output as a usize
     * This allows us to avoid runtime type conversion.
     */
    let file_size = file.metadata().unwrap().len() as usize;
    let chunk_size = file_size / available_parallelism;
    let start = Instant::now();

    let temp = &mut [0u8; 100];
    let mut beginning = 0u64;
    let mut mmap = Vec::with_capacity(available_parallelism);

    /*
     * We're going to seek the file chunk by chunk.
     * At the end of each seek we read the next 100 bytes, find the first newline and
     * treat it's index as the end of the chunk.
     *
     * This lets us produce a list of tuples that represent the start and end of each chunk
     * without having to perform full file reads.
     */
    for _ in 0..available_parallelism {
        file.seek(SeekFrom::Start(beginning + chunk_size as u64))
            .unwrap();
        file.read(temp.as_mut_slice()).unwrap();
        let temp = unsafe { std::str::from_utf8_unchecked(temp) };
        let newline = temp.find('\n').unwrap() as u64;
        let end = newline + beginning + chunk_size as u64;
        file.seek(SeekFrom::Start(end)).unwrap();

        mmap.push((beginning as i64, end as i64));
        beginning = end + 1;
    }

    println!(
        "Memory mapped {} chunks in {:?}",
        mmap.len(),
        start.elapsed()
    );

    mmap
}

#[inline(always)]
fn process_mapped_lines<'a>(start: i64, end: i64) -> impl Iterator<Item = (&'a str, Measurement)> {
    let chunk_size = (end - start) as usize;
    let mut buf = Vec::with_capacity(chunk_size);
    let beginning = Instant::now();

    let mut file = File::options().read(true).open("measurements.txt").unwrap();
    file.seek(SeekFrom::Start(start as u64)).unwrap();
    let mut take = file.take(chunk_size as u64);

    take.read_to_end(&mut buf).unwrap();

    let buf = buf.leak();

    // We know that the input is all valid utf8, so we can use unsafe to avoid the overhead of checking.
    let buf = unsafe { std::str::from_utf8_unchecked(buf) };

    println!("Read {} bytes in {:?}", buf.len(), beginning.elapsed());

    process_lines(buf)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    /*
     * Get the number of available cores on the machine
     */
    let available_parallelism = std::thread::available_parallelism().unwrap().get();

    println!("Parallelism: {}", available_parallelism);

    // Commenting this out while testing the memory map. Using a parallelism constant of 1 is
    // required for testing to ensure that we don't end up with pointer overlap.
    // let available_parallelism = available_parallelism * PARALLELISM_CONSTANT;

    let mmap = memory_map(available_parallelism);

    let handles = (0..available_parallelism)
        .map(|thread_count| {
            /*
             * Now that we have a map of the file, we can spawn threads to read the file
             * chunk by chunk in parallel.
             */
            let (start, end) = mmap[thread_count];

            std::thread::spawn(move || process_mapped_lines(start, end))
        })
        .collect::<Vec<_>>();

    // Perform memory allocation while waiting for the threads to finish
    let mut measurements = HashMap::<&str, Measurement>::with_capacity(10000);

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

    let mut results = Vec::from_iter(measurements.iter());
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
