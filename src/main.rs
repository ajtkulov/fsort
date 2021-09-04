use std::fs::File;
use std::io::{BufRead, Write};
use std::io::BufReader;
use std::vec::Vec;
use std::collections::BinaryHeap;
use std::cmp::Reverse;
use std::fs;
use clap::{AppSettings, Clap};
use std::cmp;
use std::thread;
use std::thread::JoinHandle;

#[derive(Clap)]
#[clap(version = "1.0", author = "Pavel Ajtkulov")]
#[clap(setting = AppSettings::ColoredHelp)]
struct Opts {
    #[clap(short, long)]
    input: String,
    #[clap(short, long)]
    output: String,
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

        let threshold = cmp::max((fs::metadata(input_file)?.len() / 100) as usize, 1 << 26);

        let mut vecc: Vec<Vec<String>> = Vec::new();
        let mut spawn: Vec<JoinHandle<()>> = Vec::new();
        vecc.push(Vec::new());

        loop {
            loop {
                match reader.read_line(&mut line) {
                    Ok(bytes_read) => {
                        if bytes_read == 0 {
                            done = true;
                            break;
                        }

                        size = size + line.len();
                        vecc[file_idx].push(line.to_string());

                        line.clear();
                    }
                    Err(err) => {
                        return Err(err);
                    }
                };

                if size > threshold {
                    vecc.push(Vec::new());
                    let tmp_file_name = format!("{}_{}.tmp", input_file, file_idx);
                    let mut inner_vec = vecc[file_idx].to_vec();
                    spawn.push(thread::spawn(move || {
                        inner_vec.sort();
                        write_to_file(tmp_file_name, &mut inner_vec.to_vec());
                        inner_vec.clear();
                    }));
                    size = 0;
                    file_idx = file_idx + 1;
                    break;
                }
            }

            if done {
                let mut inner_vec = vecc[file_idx].to_vec();
                let tmp_file_name = format!("{}_{}.tmp", input_file, file_idx);

                spawn.push(thread::spawn(move || {
                    inner_vec.sort();
                    write_to_file(tmp_file_name, &mut inner_vec.to_vec());
                }));

                break;
            }
        }

        for h in spawn {
            h.join();
        }

        if file_idx == 0 {
            fs::rename(format!("{}_{}.tmp", input_file, 0), output_file);
        } else {
            let mut open_files: Vec<BufReader<File>> = Vec::new();
            let mut done_states: Vec<bool> = Vec::new();
            let mut counts: Vec<i32> = Vec::new();

            let mut heap: BinaryHeap<Reverse<(String, usize)>> = BinaryHeap::new();

            for idx in 0..file_idx + 1 {
                let file_name = format!("{}_{}.tmp", input_file, idx);
                let file = File::open(&file_name)?;
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

                    if counts[idx] > 5000 {
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

fn write_to_file(file_name: String, vec: &mut Vec<String>) -> Result<(), std::io::Error> {
    let mut ff = File::create(file_name)?;

    for l in vec {
        ff.write(l.as_bytes());
    }

    Ok(())
}
