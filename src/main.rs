use std::{
    collections::HashMap,
    fmt::{Display, Formatter},
    fs::File,
    io::{BufRead, BufReader, BufWriter, Read, Seek, SeekFrom, Write},
    time::Instant,
};

/* Flip this around to see performance differences on different machines
 * Don't go too high! You'll start getting pointer overlap problems!
 * Use 1 to evenly distribute the data across your cores.
 *
 * This is a constant, so the compiler will optimize it out
 * However, know that as you decrease this number, you will have threads that finish and
 * simply spin down. So as you continue to process your chunks a lot of cpu time is wasted.
 * It's useful to increase this and check what the best value is for your machine.
 *
 * As you increase this value, the chunks will be smaller, and the threads will finish faster.
 * However since you are using more threads than the number of cores you have, it is very likely
 * that one of your cores will simply grab a thread from the cpu scheduler and dedicate itself to that.
 * This means that you will overall waste less cpu time, however if you end up processing faster
 * than you read, then you will still be bottlenecked by IO.
 */
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

impl Display for Measurement {
    #[inline(always)]
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}/{}/{:.1}",
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
        n = (n * 10.0).ceil();
    } else {
        n = (n * 10.0).round();
    }

    n / 10.0
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
fn process_lines<'a>(contents: String) -> impl Iterator<Item = (&'a str, Measurement)> {
    let contents = contents.leak();
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

    println!("Processed {} lines in {:?}", line_count, start.elapsed());
    measurements.into_iter()
}

#[inline(always)]
fn memory_map(available_parallelism: usize) -> Vec<(i64, i64)> {
    // Open the file with Direct I/O. Windows not supported.
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
        let temp = std::str::from_utf8(temp).unwrap();
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
    let beginning = Instant::now();
    let mut file = File::options().read(true).open("measurements.txt").unwrap();
    file.seek(SeekFrom::Start(start as u64)).unwrap();
    let mut take = file.take(chunk_size as u64);
    let mut buf = Vec::with_capacity(chunk_size);
    take.read_to_end(&mut buf).unwrap();

    // We know that the input is all valid utf8, so we can use unsafe to avoid the overhead of checking.
    let buf = unsafe { String::from_utf8_unchecked(buf) };

    println!("Read {} bytes in {:?}", buf.len(), beginning.elapsed());

    process_lines(buf)
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
