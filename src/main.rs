use std::fs::File;
use std::io::{BufRead, Write};
use std::io::BufReader;
use std::vec::Vec;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::fs;
use clap::{AppSettings, Clap};

#[derive(Clap)]
#[clap(version = "1.0", author = "Pavel Ajtkulov")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(short, long)]
    input: String,
    #[clap(short, long)]
    output: String
}

fn main() {
    let opts: Opts = Opts::parse();

    read_line(opts.input.as_str(), opts.output.as_str());

    fn read_line(input_file: &str, output_file: &str) -> Result<(), std::io::Error> {
        let file = File::open(&input_file)?;

        let mut reader: BufReader<File> = BufReader::new(file);
        let mut line = String::new();

        let mut size: usize = 0;
        let mut file_idx: usize = 0;
        let mut done: bool = false;

        let mut vec: Vec<String> = Vec::new();

        loop {
            loop {
                match reader.read_line(&mut line) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            done = true;
                            break;
                        }

                        size = size + line.len();
                        vec.push(line.to_string());

                        line.clear();
                    }
                    Err(err) => {
                        return Err(err);
                    }
                };

                if size > 5000000 {
                    vec.sort();
                    writeToFile(format!("{}_{}.tmp", input_file, file_idx), vec.to_vec());
                    size = 0;
                    file_idx = file_idx + 1;
                    vec.clear();
                    break;
                }
            }

            if done {
                vec.sort();
                writeToFile(format!("{}_{}.tmp", input_file, file_idx).to_string(), vec.to_vec());
                break;
            }
        }

        let mut open_files: Vec<BufReader<File>> = Vec::new();
        let mut done_states: Vec<bool> = Vec::new();
        let mut counts: Vec<i32> = Vec::new();

        let mut heap: BinaryHeap<Reverse<(String, usize)>> = BinaryHeap::new();

        for idx in 0..file_idx + 1 {
            let fileName = format!("{}_{}.tmp", input_file, idx);
            let file = File::open(&fileName)?;
            open_files.push(BufReader::new(file));
            done_states.push(false);
            counts.push(0);
        }

        for idx in 0..file_idx + 1 {
            read(&mut counts, &mut open_files, &mut done_states, &mut heap, idx);
        }

        let mut ff = File::create(output_file)?;

        read(&mut counts, &mut open_files, &mut done_states, &mut heap, 0);

        loop {
            if heap.is_empty() {
                break;
            }

            match heap.pop() {
                None => {
                    break;
                }
                Some(q1) => {
                    let q = q1.0;
                    ff.write(q.0.as_bytes());
                    counts[q.1] = counts[q.1] - 1;
                    if counts[q.1] == 0 {
                        read(&mut counts, &mut open_files, &mut done_states, &mut heap, q.1);
                    }
                }
            }
        }

        for idx in 0..file_idx + 1 {
            let file_name = format!("{}_{}.tmp", input_file, idx);

            fs::remove_file(file_name)?;
        }

        Ok(())
    }
}

fn read(counts: &mut Vec<i32>, files: &mut Vec<BufReader<File>>, done_states: &mut Vec<bool>, heap: &mut BinaryHeap<Reverse<(String, usize)>>, idx: usize) {
    let mut line = String::new();

    if !done_states[idx] && counts[idx] == 0 {
        loop {
            match files[idx].read_line(&mut line) {
                Ok(bytes_read) => {
                    if bytes_read == 0 {
                        done_states[idx] = true;
                        break;
                    }

                    let pair = (line.to_string(), idx);
                    heap.push(Reverse(pair));
                    counts[idx] = counts[idx] + 1;

                    if counts[idx] > 100 {
                        break;
                    }
                    line.clear();
                }
                Err(err) => {
                    break;
                }
            };
        }
    }
}

fn writeToFile(file_name: String, vec: Vec<String>) -> Result<(), std::io::Error> {
    let mut ff = File::create(file_name)?;

    for l in &vec {
        ff.write(l.as_bytes());
    }

    Ok(())
}
