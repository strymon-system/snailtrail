// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate env_logger;
extern crate json;
#[macro_use]
extern crate log;
extern crate logformat;
extern crate getopts;
extern crate regex;

use std::env;
use std::process;
use std::fmt;
use std::cmp;
use std::fs::File;
use std::io::{self, Read, BufWriter, Write};

use std::collections::HashMap;

use json::JsonValue;
use logformat::{LogRecord, ActivityType, EventType};
use getopts::Options;
use regex::Regex;

#[derive(Debug)]
enum Error {
    Io(io::Error),
    Json(json::Error),
}

impl From<io::Error> for Error {
    fn from(io: io::Error) -> Self {
        Error::Io(io)
    }
}

impl From<json::Error> for Error {
    fn from(json: json::Error) -> Self {
        Error::Json(json)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref io) => io.fmt(f),
            Error::Json(ref json) => json.fmt(f),
        }
    }
}

type Pid = u32;
type Tid = u32;

type WorkerId = u32;
type OperatorId = u32;


type Nanoseconds = u64;
type Timestamp = u64; // Unix time in nanoseconds

#[derive(Debug)]
struct NodeStats {
    op: String,
    start: Timestamp,
    duration: Nanoseconds,
    output: String,
    input: Vec<String>,
    tid: Tid,
}

impl NodeStats {
    fn parse(event: &JsonValue) -> Self {
        let tid = event["tid"].as_u32().expect("missing 'tid'");
        let ts = event["ts"].as_u64().expect("missing 'ts'") * 1_000; // us -> ns
        let dur = event["dur"].as_u64().expect("missing 'dur'") * 1_000; // us -> ns
        let ref args = event["args"];

        // the op also seems to be duplicated in event["name"]
        let op = args["op"].as_str().expect("missing 'op'").to_string();
        let output = args["name"].as_str().expect("missing 'name'").to_string();

        // a bit annoying, but we have to iterate over the inputs manually
        let num_inputs = args.len() - 2; // number or args minus 'op' and 'name'
        let mut inputs = Vec::new();
        for n in 0..num_inputs {
            let i = format!("input{}", n);
            let input = args[i].as_str().expect("missing input").to_string();
            inputs.push(input);
        }

        NodeStats {
            output: output,
            op: op,
            start: ts,
            duration: dur,
            input: inputs,
            tid: tid,
        }
    }
}

#[derive(Debug)]
struct DeviceStats {
    // statistics for activities run on this device
    node_stats: Vec<NodeStats>,
    // number of thread used in this step
    num_threads: usize,
}

impl DeviceStats {
    fn new() -> Self {
        DeviceStats {
            node_stats: Vec::new(),
            num_threads: 0,
        }
    }
}

#[derive(Debug)]
struct StepStats {
    // maps the PID to device names
    device_names: HashMap<Pid, String>,
    // worker list for all devices
    device_stats: HashMap<String, DeviceStats>,
    // value of earliest timestamp in this step
    first_ts: Timestamp,
    // value of latest timestamp in this step
    last_ts: Timestamp,
}

impl StepStats {
    fn new() -> Self {
        StepStats {
            device_names: HashMap::new(),
            device_stats: HashMap::new(),
            first_ts: Timestamp::max_value(),
            last_ts: Timestamp::min_value(),
        }
    }

    fn read_json<R: Read>(mut reader: R) -> Result<Self, Error> {
        let mut contents = String::new();
        reader.read_to_string(&mut contents)?;
        let json = json::parse(&contents)?;

        let mut step = StepStats::new();
        // timeline.py in tensorflow creates a single object, containing
        // a "traceEvents" property, which is an array of metadata events
        // followed by the actual events
        for event in json["traceEvents"].members() {
            step.parse_event(event)?;
        }

        // ensure sorted activitites so we can proper schedule them later
        for dev_stats in step.device_stats.values_mut() {
            dev_stats.node_stats.sort_by_key(|stat| stat.start);
        }

        Ok(step)
    }

    fn parse_event(&mut self, event: &JsonValue) -> Result<(), Error> {
        let phase = event["ph"].as_str().expect("missing phase");

        match phase {
            "M" => {
                // metadata event
                assert_eq!(event["name"],
                           "process_name",
                           "assumed metadata events only to be used for PID names");

                let mut device_name = event["args"]["name"].as_str().unwrap().to_string();
                let pid = event["pid"].as_u32().unwrap();

                // timeline.py seems to create a PID for the compute activities
                // and for the tensors, we do not care about the tensors
                if device_name.ends_with(" Compute") {
                    let name_len = device_name.len() - " Compute".len();
                    device_name.truncate(name_len);
                    self.device_names.insert(pid, device_name.clone());
                    self.device_stats.insert(device_name, DeviceStats::new());
                } else {
                    info!("ignoring PID for: {:?}", device_name);
                }
            }
            "X" => {
                // duration event
                assert_eq!(event["cat"],
                           "Op",
                           "only duration events of operations supported");

                let pid = event["pid"].as_u32().unwrap();
                let device = &self.device_names[&pid];
                let activity = NodeStats::parse(event);

                let dev_stats = self.device_stats.get_mut(device).expect("invalid device");

                // push activity to node stats
                dev_stats.num_threads = cmp::max(dev_stats.num_threads, activity.tid as usize + 1);
                self.first_ts = cmp::min(self.first_ts, activity.start);
                self.last_ts = cmp::max(self.last_ts, activity.start + activity.duration);
                dev_stats.node_stats.push(activity);
            }
            "s" | "t" => {
                // we infer dependency edges from the args
                debug!("ignoring flow event: {:?}", event);
            }
            "N" | "O" | "D" => {
                debug!("ignoring object event: {:?}", event);
            }
            "C" => {
                debug!("ignoring counter event: {:?}", event);
            }
            _ => error!("unexpected event phase: {:?}", event),
        };

        Ok(())
    }
}

#[derive(Debug)]
struct Thread {
    /// globally unique worker id for this thread
    worker_id: WorkerId,
    /// list of activities that were run on this thread (sorted by timestamp)
    schedule: Vec<NodeStats>,
}

impl Thread {
    fn busy_until(&self) -> Timestamp {
        self.schedule
            .last()
            .map(|a| a.start + a.duration)
            .unwrap_or(0)
    }
}

struct TensorFlow {
    // schedule for each device
    device: HashMap<String, Vec<Thread>>,
    /// for each output, store the worker and time it was created on
    outputs: HashMap<String, (WorkerId, Timestamp)>,
    /// generated operator ids
    operators: HashMap<String, OperatorId>,
    /// flag to keep the thread mapping of the original input
    preserve_tid: bool,
    /// flag to keep the timestamps of the original input
    preserve_timestamps: bool,
    /// globally unique worker id generator
    next_worker_id: u32,
    /// regex to strip output ports
    output_port: Regex,
    /// suffix used for duplicated names
    duplication_suffix: Regex,
}

impl TensorFlow {
    fn new(operators: HashMap<String, OperatorId>) -> Self {
        TensorFlow {
            device: HashMap::new(),
            outputs: HashMap::new(),
            operators: operators,
            preserve_tid: true,
            preserve_timestamps: false,
            next_worker_id: 0,
            output_port: Regex::new(":[0-9]+$").unwrap(),
            duplication_suffix: Regex::new("_[0-9]+($|/)").unwrap(),
        }
    }

    fn generate_worker_id(&mut self) -> WorkerId {
        let w = self.next_worker_id;
        self.next_worker_id += 1;
        w
    }

    fn create_threads(&mut self, dev_name: &str, required: usize) {
        let available = self.device.get(dev_name).map(Vec::len).unwrap_or(0);
        if available >= required {
            return;
        }

        let missing = required - available;

        let new_threads: Vec<Thread> = (0..missing)
            .map(|_| {
                     Thread {
                         worker_id: self.generate_worker_id(),
                         schedule: Vec::new(),
                     }
                 })
            .collect();

        self.device
            .entry(dev_name.to_owned())
            .or_insert_with(Vec::new)
            .extend(new_threads);
    }

    fn schedule_step(&mut self, step: StepStats) {
        assert!(step.device_stats.len() == 1,
                "currently only supports single device");

        let window_size = step.last_ts - step.first_ts;
        // starting at 0 tends to be buggy, and we add +1 since snailtrail
        // inserts a bogus node at window-1
        let window_offset = window_size * 1000;

        for (dev_name, dev_stats) in step.device_stats {
            self.create_threads(&dev_name, dev_stats.num_threads);
            let threads = self.device.get_mut(&dev_name).unwrap();

            for mut activity in dev_stats.node_stats {
                info!("scheduling: {:?}", activity);
                assert!(!self.outputs.contains_key(&activity.output));

                if !self.preserve_timestamps {
                    // re-align timestamp so the first activity starts right at the window border
                    activity.start = activity.start - step.first_ts + window_offset;
                }

                let start_time = activity.start;
                let end_time = start_time + activity.duration;
                let output = activity.output.clone();

                // create operator id if necessary
                assert!(self.operators.contains_key(&activity.op),
                        "Unkown operator {:?}, please add to operators.json",
                        activity.op);

                let thread = if self.preserve_tid {
                    &mut threads[activity.tid as usize]
                } else {
                    // find MRU thread
                    threads
                        .iter_mut()
                        .filter(|thr| thr.busy_until() < start_time)
                        .max_by_key(|thr| thr.busy_until())
                        .expect("unable to find idle thread?!")
                };
                info!("selected worker {}", thread.worker_id);
                thread.schedule.push(activity);

                // bookkeeping of output
                self.outputs.insert(output, (thread.worker_id, end_time));
            }
        }

        println!("Step length (in seconds): {:.9}",
                 (window_size as f64 + 1.0) / 1e9);
    }

    fn potential_duplicates(&self, input: &str) -> Vec<String> {
        // for found match, create a string which does not contain the match
        self.duplication_suffix
            .captures_iter(input)
            .map(|cap| {
                     let m = cap.get(0).unwrap();
                     let delimn = cap.get(1).unwrap().as_str();
                     let end = m.end() - delimn.len();
                     // create a string which does not contain the _1 bit
                     [&input[..m.start()], &input[end..]].join("")
                 })
            .collect()
    }

    fn emit_dependency<W: Write>(&self,
                                 input: &str,
                                 activity: &NodeStats,
                                 worker: WorkerId,
                                 writer: &mut LogWriter<W>)
                                 -> Result<(), Error> {
        // control dependencies start with a caret, just strip it
        if input.starts_with("^") {
            if let Some((parent, send)) = self.lookup(&input[1..]) {
                writer.control(parent, send, worker, activity.start);
            } else {
                warn!("unable to resolve control dependency: {}", input);
            }
        } else if let Some((parent, send)) = self.lookup(input) {
            writer.data(parent, send, worker, activity.start);
        } else {
            let potential_outputs: Vec<_> = self.potential_duplicates(input)
                .iter()
                .filter_map(|input| self.lookup(input))
                .collect();

            if potential_outputs.len() > 1 {
                warn!("too many candiates for input {}: {:?}",
                      input,
                      potential_outputs);
            } else if let Some(&(parent, send)) = potential_outputs.first() {
                writer.data(parent, send, worker, activity.start);
            } else {
                warn!("unable to resolve input: {}", input);
            }
        }
        Ok(())
    }

    fn lookup(&self, input: &str) -> Option<(WorkerId, Timestamp)> {
        // we have to strip output ports of the form :1, :2, ... etc
        let input = self.output_port.replace(input, "");
        self.outputs.get(&*input).cloned()
    }

    fn write_msgpack<W: Write>(&self, writer: W) -> Result<(), Error> {
        let mut writer = LogWriter::new(writer);

        for (_device, threads) in self.device.iter() {
            for thread in threads.iter() {
                let worker = thread.worker_id;
                for activity in thread.schedule.iter() {
                    info!("writing {:?}", activity);
                    // emit dependencies to parent
                    for input in activity.input.iter() {
                        self.emit_dependency(input, &activity, worker, &mut writer)?;
                    }
                    // TODO(swicki): deal with recv/send nodes
                    let operator_id = self.operators[&activity.op];
                    writer.activity(worker,
                                    activity.start,
                                    activity.duration,
                                    ActivityType::Processing,
                                    operator_id);
                }
            }
        }

        writer.flush()?;
        writer.print_stats();
        Ok(())
    }
}

#[derive(Debug)]
struct LogWriter<W: Write> {
    writer: W,
    sequence_no: u64,
    activity_cnt: u64,
    buffer: Vec<LogRecord>,
}

impl<W: Write> LogWriter<W> {
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
            info!("omitting self message of type {:?}", activity);
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

    pub fn data(&mut self, sender: u32, send_ts: Timestamp, receiver: u32, recv_ts: Timestamp) {
        self.communication(sender,
                           send_ts,
                           receiver,
                           recv_ts,
                           ActivityType::DataMessage)
    }

    pub fn control(&mut self, sender: u32, send_ts: Timestamp, receiver: u32, recv_ts: Timestamp) {
        self.communication(sender,
                           send_ts,
                           receiver,
                           recv_ts,
                           ActivityType::ControlMessage)
    }

    pub fn activity(&mut self,
                    worker: u32,
                    start: Timestamp,
                    duration: Nanoseconds,
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
            timestamp: start + duration,
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
        drop(self.flush());
    }
}

fn print_usage(program: &str, opts: Options) -> ! {
    let brief = format!("Usage: {} [options] -o OUTPUT INPUT...", program);
    print!("{}", opts.usage(&brief));
    process::exit(0)
}

fn read_operators(filename: &str) -> Result<HashMap<String, OperatorId>, Error> {
    let mut reader = File::open(filename)?;
    let mut contents = String::new();
    reader.read_to_string(&mut contents)?;
    let json = json::parse(&contents)?;

    let mut operators = HashMap::new();
    for (name, id) in json.entries() {
        let id = id.as_u32().unwrap();
        operators.insert(name.to_string(), id);
    }

    Ok(operators)
}

fn convert(output: &str, input: &[String], operators: &str) -> Result<(), Error> {
    let operators = read_operators(operators)?;

    let mut tf = TensorFlow::new(operators);
    assert!(input.len() == 1, "currently only supports single step");
    for json in input {
        let input = File::open(json)?;
        let step = StepStats::read_json(input)?;
        tf.schedule_step(step);
    }

    let writer = BufWriter::new(File::create(output)?);
    tf.write_msgpack(writer)
}

fn main() {
    env_logger::init().expect("failed to initalize logger");

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optflag("h", "help", "print this help menu");
    opts.optopt("j", "operators", "name of the operators json", "JSON");
    opts.reqopt("o", "output", "name of output file", "FILE");
    let matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => {
            println!("error: {}", f.to_string());
            print_usage(&program, opts);
        }
    };

    if matches.opt_present("h") || matches.free.is_empty() {
        print_usage(&program, opts);
    }

    let operators = matches
        .opt_str("j")
        .unwrap_or(String::from("operators.json"));

    let output = matches
        .opt_str("o")
        .unwrap_or_else(|| print_usage(&program, opts));
    let input = matches.free;

    convert(&output, &input, &operators).expect("failed to convert TensorFlow logs");
}
