// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

#[macro_use]
extern crate json;

use std::fs::File;
use std::io::{Result, BufReader, BufWriter, Write};
use std::error::Error as ErrorTrait;
use std::collections::HashMap;

use json::JsonValue;
use logformat::{LogRecord, EventType};

struct JsonWriter<W: Write> {
    writer: W,
    started: bool,
}

impl<W: Write> JsonWriter<W> {
    fn new(writer: W) -> Self {
        JsonWriter {
            writer: writer,
            started: false,
        }
    }

    fn write(&mut self, json: &JsonValue) -> Result<()> {
        if !self.started {
            self.writer.write_all(b"[")?;
            self.started = true;
        } else {
            self.writer.write_all(b",")?;
        }

        json.write(&mut self.writer)
    }
}

impl<W: Write> Drop for JsonWriter<W> {
    fn drop(&mut self) {
        drop(self.writer.write_all(b"]"));
    }
}

fn convert(logfile: &str, json: &str, skip: Option<usize>, take: Option<usize>) -> Result<()> {
    let input = File::open(logfile)?;
    let mut reader = BufReader::new(input);

    let output = File::create(json)?;
    let buffered = BufWriter::new(output);
    let mut writer = JsonWriter::new(buffered);

    // TODO properly detect errors when reading the log records
    let mut records = Vec::new();
    while let Ok(record) = LogRecord::read(&mut reader) {
        records.push(record);
    }

    records.sort_by_key(|rec| rec.timestamp);

    let records: Vec<_> = records
        .drain(..)
        .skip(skip.unwrap_or(0))
        .take(take.unwrap_or(::std::usize::MAX))
        .collect();

    let mut open_edges = HashMap::new();

    for record in records {
        let ts = record.timestamp;
        let activity_type = format!("{:?}", record.activity_type);
        let w_id = record.local_worker;
        let _r_id = record.remote_worker;
        let corr_id = record.correlator_id.expect("correlator id required");


        match record.event_type {
            EventType::Start | EventType::Sent => {
                let event = object!{
                    "worker" => w_id,
                    "start" => ts,
                    "type" => activity_type,
                    "corr_id" => corr_id
                };
                open_edges.insert(corr_id, event);
            }
            EventType::End | EventType::Received => {
                if let Some(ref event) = open_edges.get(&corr_id) {
                    let mut write_event = (*event).clone();
                    write_event["end"] = JsonValue::from(ts);
                    write_event["remote"] = JsonValue::from(w_id);
                    writer.write(&write_event)?;
                }
            }
            EventType::Bogus => {}
        };
    }

    Ok(())
}

fn main() {
    use clap::*;

    let matches = App::new("cpviz")
        .arg(Arg::with_name("input_file"))
        .arg(Arg::with_name("output_file"))
        .arg(Arg::with_name("skip").long("skip").takes_value(true))
        .arg(Arg::with_name("take").long("take").takes_value(true))
        .get_matches();

    // let input = env::args().nth(1).expect("required argument: logfromat input file");
    // let output = env::args().nth(2).unwrap_or_else(|| format!("{}.json", input));
    let input = matches
        .value_of("input_file")
        .expect("input_file is required");
    let output = matches
        .value_of("output_file")
        .map(|x| x.to_owned())
        .unwrap_or_else(|| format!("{}.json", input));
    let skip: Option<usize> = matches
        .value_of("skip")
        .map(|x| x.parse().expect("invalid skip"));
    let take: Option<usize> = matches
        .value_of("take")
        .map(|x| x.parse().expect("invalid take"));

    if let Err(err) = convert(&input, &output, skip, take) {
        println!("Conversion ended with: {}", err.description());
    }
}
