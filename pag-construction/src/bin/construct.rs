// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std::str::FromStr;

use clap::{App, Arg};

use pag_construction::dataflow::{Config, run_dataflow};

const NS_TO_SEC: u64 = 1_000_000_000;

fn main() {
    // NOTE: implement a dataflow program which constructs the program activity graph:
    // [INPUT]
    //   LogRecord { worker_id: u32, time: u64, activity_type: enum, event_type: enum, correlator: OpaqueCorrelator<u64> }
    //
    // [DESIRED OUTPUT]
    //   Edge { src: Node, dest: Node, type: ?? }
    //   Node { timestamp: u64, worker_id: u64 }
    //
    // [STEPS]
    // * timeline per worker (group_by + sort)
    // * communication edges
    // * collapse/fill gaps based on threshold
    // * insert waiting edges
    // * slice according to time window (either merge across multiple tumbling or sliding windows)

    let matches = App::new("construct")
        .setting(clap::AppSettings::TrailingVarArg)
        .about("Construct PAG from log")
        .arg(Arg::with_name("INPUT")
            .help("Sets the log file to read")
            .index(1)
            .required(true))
        .arg(Arg::with_name("threshold")
            .help("Sets the unknown edge threshold")
            .index(2)
            .short("t")
            .long("threshold")
            .value_name("THRESHOLD")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("window")
            .help("Sets the window size in seconds")
            .index(3)
            .short("w")
            .long("window")
            .value_name("WINDOW_SIZE")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("epochs")
            .help("Sets the number of epochs in flight")
            .short("e")
            .long("epochs")
            .value_name("EPOCHS")
            .takes_value(true)
            .required(true))
        .arg(Arg::with_name("message-delay")
            .help("Sets a constant message dely")
            .long("message-delay")
            .value_name("message-delay")
            .takes_value(true)
            .required(false))
        .arg(Arg::with_name("bc-dot")
            .help("Produce a BC DOT file per time slice")
            .short("d")
            .long("bc-dot"))
        .arg(Arg::with_name("pag-dot")
            .help("Produce a PAG DOT file per time slice")
            .long("pag-dot"))
        .arg(Arg::with_name("pag-msgpack")
            .help("Produce a PAG msgpack file per time slice")
            .long("pag-msgpack"))
        .arg(Arg::with_name("v")
            .help("Print more verbose output")
            .short("v")
            .multiple(true)
            .long("verbose"))
        .arg(Arg::with_name("dump-pag")
            .help("Print the contents of the PAG to stdout. Potentially produces a lot of data.")
            .long("dump-pag"))
        .arg(Arg::with_name("no-insert-waiting")
            .help("Do not nsert waiting edges but use unknown for all gaps")
            .long("no-insert-waiting"))
        .arg(Arg::with_name("no-summary")
            .help("Do not compute summaries")
            .long("no-summary"))
        .arg(Arg::with_name("no-bc")
            .help("Do not compute BC")
            .long("no-bc"))
        .arg(Arg::with_name("waiting-message")
            .help("Consider messages with a lenght of 2*threshold waiting")
            .long("waiting-message")
            .takes_value(true)
            .value_name("delayed_message_threshold"))
        .arg(Arg::with_name("TIMELY")
            .multiple(true))
        .get_matches();


    let local_matches = matches.clone();
    let timely_args = if let Some(values) = local_matches.values_of("TIMELY") {
        values.map(String::from).collect()
    } else {
        vec![]
    };
    let window_size_s = f64::from_str(matches
                                          .value_of("window")
                                          .expect("Window parameter missing"))
            .expect("Cannot read window size");

    let config = Config {
        timely_args: timely_args,
        log_path: String::from(matches.value_of("INPUT").expect("log input path required")),
        threshold: u64::from_str(matches
                                     .value_of("threshold")
                                     .expect("Threshold parameter missing"))
                .expect("Cannot read threshold"),
        window_size_ns: (window_size_s * (NS_TO_SEC as f64)) as u64,
        epochs: u64::from_str(matches
                                  .value_of("epochs")
                                  .expect("Epochs parameter missing"))
                .expect("Cannot read epochs parameter"),
        message_delay: if matches.is_present("message-delay") {
            Some(u64::from_str(matches.value_of("message-delay").unwrap())
                     .expect("Cannot read message-delay parameter"))
        } else {
            None
        },
        verbose: matches.occurrences_of("v"),
        dump_pag: matches.is_present("dump-pag"),
        write_bc_dot: matches.is_present("bc-dot"),
        write_pag_dot: matches.is_present("pag-dot"),
        write_pag_msgpack: matches.is_present("pag-msgpack"),
        insert_waiting_edges: !matches.is_present("no-insert-waiting"),
        disable_summary: matches.is_present("no-summary"),
        disable_bc: matches.is_present("no-bc"),
        waiting_message: u64::from_str(matches.value_of("waiting-message").unwrap_or("0"))
            .expect("Cannot read waiting-message parameter"),
    };

    run_dataflow(config).unwrap();
}
