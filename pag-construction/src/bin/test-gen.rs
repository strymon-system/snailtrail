// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate clap;
extern crate logformat;

use clap::{Arg, App};

use std::path::Path;
use std::fs::File;
use std::io::{self, BufWriter, Write};
use logformat::{LogRecord, ActivityType, EventType};

type Timestamp = u64; // Unix time in nanoseconds
type Nanoseconds = u64;

type LogFileWriter = LogWriter<BufWriter<File>>;

#[derive(Debug)]
struct LogWriter<W: Write> {
    writer: W,
    sequence_no: u64,
    activity_cnt: u64,
    buffer: Vec<LogRecord>,
}

impl<W: Write> LogWriter<W> {
    fn create<P: AsRef<Path>>(path: P) -> io::Result<LogFileWriter> {
        File::create(path).map(BufWriter::new).map(LogWriter::new)
    }

    fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            writer: writer,
            sequence_no: 0,
            activity_cnt: 0,
            buffer: Vec::new(),
        }
    }

    fn communication(&mut self,
                     sender: u32,
                     send_ts: Timestamp,
                     receiver: u32,
                     recv_ts: Timestamp,
                     activity: ActivityType) {
        if sender == receiver {
            println!("omitting self message of type {:?}", activity);
            return;
        }

        let tx = LogRecord {
            timestamp: send_ts,
            local_worker: sender,
            activity_type: activity,
            event_type: EventType::Sent,
            correlator_id: Some(self.sequence_no),
            remote_worker: Some(receiver),
            operator_id: None,
        };

        let rx = LogRecord {
            timestamp: recv_ts,
            local_worker: receiver,
            activity_type: activity,
            event_type: EventType::Received,
            correlator_id: Some(self.sequence_no),
            remote_worker: Some(sender),
            operator_id: None,
        };

        self.sequence_no += 1;
        self.buffer.push(tx);
        self.buffer.push(rx);
    }

    pub fn activity(&mut self,
                    worker: u32,
                    start: Timestamp,
                    stop: Timestamp,
                    activity: ActivityType,
                    operator: u32) {

        let start_event = LogRecord {
            timestamp: start,
            local_worker: worker,
            activity_type: activity,
            event_type: EventType::Start,
            correlator_id: None,
            remote_worker: None,
            operator_id: Some(operator),
        };

        let end_event = LogRecord {
            timestamp: stop,
            local_worker: worker,
            activity_type: activity,
            event_type: EventType::End,
            correlator_id: None,
            remote_worker: None,
            operator_id: Some(operator),
        };

        self.activity_cnt += 1;
        self.buffer.push(start_event);
        self.buffer.push(end_event);
    }

    fn flush(&mut self) -> io::Result<()> {
        self.buffer.sort_by_key(|rec| rec.timestamp);
        for record in self.buffer.drain(..) {
            record.write(&mut self.writer)?;
        }
        Ok(())
    }

    fn print_stats(&self) {
        println!("emitted {} activities and {} flow events",
                 self.activity_cnt,
                 self.sequence_no);
    }
}

impl<W: Write> Drop for LogWriter<W> {
    fn drop(&mut self) {
        self.flush().expect("failed to write log file");
    }
}

struct PagState {
    clock: Timestamp,
    phase: u64,
    conf: Config,
    writer: LogFileWriter,
}

impl PagState {
    fn create<P: AsRef<Path>>(config: Config, offset: Timestamp, output: P) -> io::Result<Self> {
        assert!(config.num_phases >= 1, "must have at least one phase");
        assert!(config.num_workers >= 1, "must have at least one worker");

        Ok(PagState {
               phase: 0,
               clock: offset + 1_000_000_000_000_000,
               conf: config,
               writer: LogFileWriter::create(output)?,
           })
    }

    fn generate_pyramid_phase(&mut self) -> io::Result<()> {
        let num_slots = 4 * (self.conf.num_workers as u64 - 1) + 1;
        let slot_len = self.conf.phase_len / num_slots;

        let phase_start = self.clock;
        let phase_end = phase_start + self.conf.phase_len - 1;

        for w in 1..self.conf.num_workers {
            let start = phase_start + (2 * w as u64 - 1) * slot_len;
            let end = phase_end - (2 * w as u64 - 1) * slot_len;

            let main_start = start + slot_len;
            let main_end = end - slot_len;

            if let Some(prefix) = self.conf.activity_prefix {
                self.writer.activity(w, phase_start, main_start, prefix, w);
            }

            // incoming message from previous worker
            if let Some(ty) = self.conf.send_edge {
                self.writer.communication(w - 1, start, w, main_start, ty);
            }

            // don't emit the infix activity on the last worker
            if w < self.conf.num_workers - 1 {
                self.writer
                    .activity(w, main_start, main_start + slot_len, self.conf.activity, w);
                if let Some(infix) = self.conf.activity_infix {
                    self.writer
                        .activity(w, main_start + slot_len, main_end - slot_len, infix, w);
                }
                self.writer
                    .activity(w, main_end - slot_len, main_end, self.conf.activity, w);
            } else {
                // just do a single activity from start to end
                self.writer
                    .activity(w, main_start, main_end, self.conf.activity, w);
            }

            // outgoing message to previous worker
            if let Some(ty) = self.conf.recv_edge {
                self.writer.communication(w, main_end, w - 1, end, ty);
            }

            if let Some(suffix) = self.conf.activity_suffix {
                self.writer.activity(w, main_end, phase_end, suffix, w);
            }
        }

        // worker 0 is a special case
        if self.conf.num_workers > 1 {
            self.writer
                .activity(0,
                          phase_start,
                          phase_start + slot_len,
                          self.conf.activity,
                          0);
            if let Some(infix) = self.conf.activity_infix {
                self.writer
                    .activity(0, phase_start + slot_len, phase_end - slot_len, infix, 0);
            }
            self.writer
                .activity(0, phase_end - slot_len, phase_end, self.conf.activity, 0);
        } else {
            // just do a single activity from start to end
            self.writer
                .activity(0, phase_start, phase_end, self.conf.activity, 0);
        }

        self.phase += 1;
        self.clock = phase_end + 1;

        self.writer.flush()
    }
}

struct Config {
    num_workers: u32,
    num_phases: u64,
    phase_len: Nanoseconds,
    activity: ActivityType,
    activity_prefix: Option<ActivityType>,
    activity_infix: Option<ActivityType>,
    activity_suffix: Option<ActivityType>,
    send_edge: Option<ActivityType>,
    recv_edge: Option<ActivityType>,
}

fn parse_activity(activity: &str) -> Option<ActivityType> {
    match activity {
        "Input" => Some(ActivityType::Input),
        "Buffer" => Some(ActivityType::Buffer),
        "Scheduling" => Some(ActivityType::Scheduling),
        "Processing" => Some(ActivityType::Processing),
        "BarrierProcessing" => Some(ActivityType::BarrierProcessing),
        "Serialization" => Some(ActivityType::Serialization),
        "Deserialization" => Some(ActivityType::Deserialization),
        "FaultTolerance" => Some(ActivityType::FaultTolerance),
        "Unknown" => Some(ActivityType::Unknown),
        "Waiting" => Some(ActivityType::Waiting),
        "None" => None,
        activity => panic!("invalid or unknown activity: {:?}", activity),
    }
}

fn parse_edge(edge: Option<&str>) -> Option<ActivityType> {
    match edge {
        Some("data") | None => Some(ActivityType::DataMessage),
        Some("control") => Some(ActivityType::ControlMessage),
        Some("none") => None,
        Some(edge) => panic!("invalid edge type: {:?}", edge),
    }
}

fn main() {
    let matches = App::new("test-gen")
        .about("Generate msgpack traces for testing")
        .arg(Arg::with_name("OUTPUT")
                 .help("Log file to write")
                 .index(1)
                 .required(true))
        .arg(Arg::with_name("num-workers")
                 .help("Number of workers in the pag")
                 .short("k")
                 .long("num-workers")
                 .takes_value(true)
                 .required(false))
        .arg(Arg::with_name("num-phases")
                 .help("Number of repetations of the pattern")
                 .short("r")
                 .long("num-phases")
                 .takes_value(true)
                 .required(false))
        .arg(Arg::with_name("phase-duration")
                 .help("Duration in milliseconds of each phase")
                 .short("d")
                 .long("phase-duration")
                 .takes_value(true)
                 .required(false))
        .arg(Arg::with_name("activity")
                 .long("activity")
                 .takes_value(true)
                 .required(false))
        .arg(Arg::with_name("activity-prefix")
                 .long("activity-prefix")
                 .takes_value(true)
                 .required(false))
        .arg(Arg::with_name("activity-infix")
                 .long("activity-infix")
                 .takes_value(true)
                 .required(false))
        .arg(Arg::with_name("activity-suffix")
                 .long("activity-suffix")
                 .takes_value(true)
                 .required(false))
        .arg(Arg::with_name("offset")
                 .long("offset")
                 .takes_value(true)
                 .help("Initial clock offset in milliseconds")
                 .required(false))
        .arg(Arg::with_name("send-edge")
                 .long("send-edge")
                 .takes_value(true)
                 .possible_values(&["data", "control", "none"])
                 .required(false))
        .arg(Arg::with_name("recv-edge")
                 .long("recv-edge")
                 .takes_value(true)
                 .possible_values(&["data", "control", "none"])
                 .required(false))
        .get_matches();

    let num_workers = matches
        .value_of("num-workers")
        .map(|m| m.parse::<u32>().expect("num-workers must be u32"))
        .unwrap_or(10);

    let num_phases = matches
        .value_of("num-phases")
        .map(|m| m.parse::<u64>().expect("num-repetitions must be u64"))
        .unwrap_or(1);

    let phase_len = matches
        .value_of("phase-duration")
        .map(|m| m.parse::<u64>().expect("phase-duration must be u64"))
        .unwrap_or(10) * 1_000_000; // ms -> ns

    let offset = matches
        .value_of("clock-offset")
        .map(|m| m.parse::<u64>().expect("clock-offset must be u64"))
        .unwrap_or(0) * 1_000_000; // ms -> ns

    let activity = matches
        .value_of("activity")
        .or(Some("Processing"))
        .and_then(parse_activity)
        .expect("main activity cannot be None");

    let activity_prefix = matches
        .value_of("activity-prefix")
        .or(Some("Waiting"))
        .and_then(parse_activity);

    let activity_infix = matches
        .value_of("activity-infix")
        .or(Some("Processing"))
        .and_then(parse_activity);

    let activity_suffix = matches
        .value_of("activity-suffix")
        .or(Some("Waiting"))
        .and_then(parse_activity);

    let send_edge = parse_edge(matches.value_of("send-edge"));
    let recv_edge = parse_edge(matches.value_of("recv-edge"));

    let config = Config {
        num_workers,
        num_phases,
        phase_len,
        send_edge,
        recv_edge,
        activity,
        activity_prefix,
        activity_infix,
        activity_suffix,
    };

    let log_path = matches.value_of("OUTPUT").unwrap();

    let mut pag = PagState::create(config, offset, log_path)
        .expect("failed to create output log file");

    for _ in 0..num_phases {
        pag.generate_pyramid_phase().unwrap();
    }

    pag.writer.print_stats();
}
