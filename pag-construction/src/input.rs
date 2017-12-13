// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::fs::File;
use std::io::BufReader;
use std::collections::{HashSet, HashMap};
use logformat::{CorrelatorId, EventType, LogRecord, LogReadError, Timestamp, Worker};

/// Read and decode all log records from a log file and give them as input in a single epoch.  In a
/// real computation we'd read input in the background and allow the computation to progress by
/// continually making steps.
pub fn read_sorted_trace_from_file_and_cut_messages(log_path: &str,
                                                    message_delay: Option<u64>)
                                                    -> Vec<LogRecord> {
    let file = File::open(log_path).expect("Unable to open input file");
    let mut reader = BufReader::with_capacity(1 << 22, file);
    let mut input_records = Vec::new();
    loop {
        match LogRecord::read(&mut reader) {
            Ok(rec) => {
                input_records.push(rec);
            }
            Err(LogReadError::Eof) => {
                break;
            }
            Err(LogReadError::DecodeError(msg)) => {
                eprintln!("could not decode record: {:?}", msg);
            }
        };
    }

    // If `message_delay` is `Some`, clip messages to the contained value if longer
    if let Some(message_delay) = message_delay {
        let mut send_stash: HashMap<(Worker, Worker, Option<CorrelatorId>), Timestamp> =
            HashMap::new();

        // Find all sends
        for rec in &input_records {
            if rec.event_type == EventType::Sent {
                send_stash
                    .insert((rec.local_worker, rec.remote_worker.unwrap(), rec.correlator_id),
                            rec.timestamp);
            }
        }

        // Match with receives
        for rec in &mut input_records {
            if rec.event_type == EventType::Received {
                let key = (rec.remote_worker.unwrap(), rec.local_worker, rec.correlator_id);
                if let Some(timestamp) = send_stash.remove(&key) {
                    if (rec.timestamp as i64 - timestamp as i64).abs() as u64 > message_delay {
                        let new_timestamp = timestamp + message_delay;
                        rec.timestamp = new_timestamp;
                    }
                }
            }
        }
    }

    // Timely requires that time increases monotonically
    input_records.sort_by_key(|rec| rec.timestamp);
    input_records
}

/// Return the ids of the workers found in the trace, sorted.
pub fn workers_in_trace(records: &[LogRecord]) -> Vec<Worker> {
    use rayon::prelude::*;

    let mut workers: Vec<_> = records
        .par_iter()
        .map(|x| {
                 let mut hs = HashSet::new();
                 hs.insert(x.local_worker);
                 hs
             })
        .reduce_with(|a, b| a.union(&b).cloned().collect())
        .unwrap_or_default()
        .into_iter()
        .collect();
    workers.sort();
    workers
}

fn default_hash<T>(obj: &T) -> u64
    where T: ::std::hash::Hash
{
    use std::hash::Hasher;
    use std::collections::hash_map::DefaultHasher;

    let mut hasher = DefaultHasher::new();
    obj.hash(&mut hasher);
    hasher.finish()
}

pub fn infer_correlator_ids(_workers: &[u32], records: &mut [LogRecord]) {
    // use rayon::prelude::*;

    let mut worker_in_progress = HashMap::new();
    for record in records
            .iter_mut()
            .filter(|r| r.activity_type.is_worker_local()) {
        let activity_in_progress = worker_in_progress
            .entry(record.local_worker)
            .or_insert_with(HashMap::new);
        let in_progress = activity_in_progress
            .entry(record.activity_type)
            .or_insert_with(Vec::new);
        match record.event_type {
            EventType::Start => {
                if record.correlator_id.is_none() {
                    let new_id = default_hash(record);
                    record.correlator_id = Some(new_id);
                    in_progress.push(new_id);
                }
            }
            EventType::End => {
                if record.correlator_id.is_none() {
                    let start_record_id = in_progress.pop().expect("end without start");
                    record.correlator_id = Some(start_record_id);
                }
            }
            EventType::Bogus => (),
            _ => panic!("unexpected event type {:?}", record),
        }
    }
}
