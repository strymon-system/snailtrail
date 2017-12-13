// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#![allow(unused_imports)]

extern crate pag_construction;
extern crate logformat;
extern crate clap;
extern crate rayon;

use std::collections::HashMap;
use logformat::{LogRecord, EventType, ActivityType};

use clap::{App, Arg};

fn main() {
    let matches = App::new("verify")
        .about("Verify msgpack trace")
        .arg(Arg::with_name("INPUT")
                 .help("Log file to read")
                 .index(1)
                 .required(true))
        .arg(Arg::with_name("message-delay")
                 .help("Sets a constant message dely")
                 .long("message-delay")
                 .takes_value(true)
                 .required(false))
        .get_matches();

    let message_delay = matches
        .value_of("message-delay")
        .map(|m| m.parse::<u64>().expect("message-delay must be u64"));

    let log_path = matches.value_of("INPUT").unwrap();
    let records =
        pag_construction::input::read_sorted_trace_from_file_and_cut_messages(log_path,
                                                                              message_delay);

    use rayon::prelude::*;

    let workers = pag_construction::input::workers_in_trace(&records);
    eprintln!("workers: {:?}", workers);

    let mut valid = true;
    valid =
        {
            eprintln!();
            eprintln!("Check that there's no nesting or overlap in a single worker for worker-local
activities of the same type.");
            eprintln!();
            let nesting_result = &workers.par_iter()
            .map(|w| {
                let mut in_progress = HashMap::new();
                for record in records.iter().filter(|x| x.local_worker == *w) {
                    match record.event_type {
                        EventType::Start => {
                            use std::collections::hash_map::Entry;
                            match in_progress.entry(record.activity_type) {
                                Entry::Vacant(v) => {
                                    v.insert(record);
                                },
                                Entry::Occupied(o) => {
                                    return Err(format!("double start detected: {:?}, previous: {:?}", record, o.get()));
                                },
                            }
                        },
                        EventType::End => {
                            if in_progress.remove(&record.activity_type).is_none() {
                                return Err(format!("end without start detected: {:?}", record));
                            }
                        },
                        _ => ()
                    }
                }
                Ok(())
            }).collect::<Vec<_>>();

            println!("# nesting report:");
            for r in nesting_result.into_iter() {
                println!("{:?}", r);
            }
            println!();
            nesting_result.iter().all(|x| x.is_ok())
        } && valid;
    valid = {
        eprintln!();
        eprintln!("Check that worker-local activities of the same type are properly nested within a worker",);
        eprintln!();
        let stack_result = &workers
                                .par_iter()
                                .map(|w| {
            let mut in_progress = HashMap::new();
            for record in records.iter().filter(|x| x.local_worker == *w) {
                match record.event_type {
                    EventType::Start => {
                        let entry = in_progress
                            .entry(record.activity_type)
                            .or_insert(Vec::new());
                        entry.push(record);
                    }
                    EventType::End => {
                        if in_progress
                               .get_mut(&record.activity_type)
                               .and_then(|e| e.pop())
                               .is_none() {
                            return Err(format!("end without start detected: {:?}", record));
                        }
                    }
                    _ => (),
                }
            }
            Ok(())
        })
                                .collect::<Vec<_>>();

        println!("# stacking report (per type):");
        for r in stack_result.into_iter() {
            println!("{:?}", r);
        }
        println!();
        stack_result.iter().all(|x| x.is_ok())
    } && valid;
    valid = {
        eprintln!();
        eprintln!("Check that worker-local activities are properly nested within a worker (this may not be required)",);
        eprintln!();
        let any_type_stack_result = &workers.par_iter()
            .map(|w| {
                let mut max_nesting = 0;
                let mut in_progress = Vec::new();
                for record in records.iter().filter(|x| x.local_worker == *w) {
                    match record.event_type {
                        EventType::Start => {
                            in_progress.push(record);
                            max_nesting = std::cmp::max(max_nesting, in_progress.len());
                        },
                        EventType::End => {
                            match in_progress.pop() {
                                Some(begin) => if begin.activity_type != record.activity_type {
                                    return Err(format!("inconsistent nesting: {:?} is terminated by {:?}", begin, record));
                                },
                                None => return Err(format!("end without start detected: {:?}", record)),
                            }
                        },
                        _ => ()
                    }
                }
                Ok(format!("max nesting: {}", max_nesting))
            }).collect::<Vec<_>>();

        println!("# stacking report (any type):");
        for r in any_type_stack_result.into_iter() {
            println!("{:?}", r);
        }
        println!();
        any_type_stack_result.iter().all(|x| x.is_ok())
    } && valid;

    if !valid {
        eprintln!("trace seems invalid");
        ::std::process::exit(-1);
    }

}
