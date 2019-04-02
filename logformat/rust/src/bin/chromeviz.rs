// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate logformat;
#[macro_use]
extern crate json;

use std::env;
use std::error::Error as ErrorTrait;
use std::fs::File;
use std::io::{BufReader, BufWriter, Error, ErrorKind, Result, Write};

use json::JsonValue;
use logformat::{EventType, LogReadError, LogRecord};

struct JsonWriter<W: Write> {
    writer: W,
    started: bool,
    buf: Vec<u8>,
}

impl<W: Write> JsonWriter<W> {
    fn new(writer: W) -> Self {
        JsonWriter {
            writer: writer,
            started: false,
            buf: Vec::new(),
        }
    }

    fn write(&mut self, json: &JsonValue) -> Result<usize> {
        if !self.started {
            self.writer.write_all(b"[")?;
            self.started = true;
        } else {
            self.writer.write_all(b",")?;
        }

        // unfortunately, the json crate doesn't export the amount of bytes
        // written, so we measure this ourselves
        let mut written = 1;

        self.buf.clear();
        json.write(&mut self.buf).expect("cannot fail");
        self.writer.write_all(&self.buf)?;

        written += self.buf.len();

        Ok(written)
    }
}

impl<W: Write> Drop for JsonWriter<W> {
    fn drop(&mut self) {
        drop(self.writer.write_all(b"]"));
    }
}

const CHUNK_MAX_SIZE: usize = 250_000_000;

struct SplitWriter {
    basename: String,
    bytes: usize,
    chunk: usize,
    writer: Option<JsonWriter<BufWriter<File>>>,
}

impl SplitWriter {
    fn new(basename: &str) -> Self {
        SplitWriter {
            basename: basename.into(),
            bytes: 0,
            chunk: 0,
            writer: None,
        }
    }

    fn current_file(&self) -> String {
        format!("{}.{:03}.json", self.basename, self.chunk)
    }

    fn get_writer(&mut self) -> Result<&mut JsonWriter<BufWriter<File>>> {
        if self.writer.is_none() {
            let file = File::create(self.current_file())?;
            let buffered = BufWriter::new(file);
            let writer = JsonWriter::new(buffered);

            self.bytes = 0;
            self.writer = Some(writer);
        }

        Ok(self.writer.as_mut().unwrap())
    }

    fn write(&mut self, json: &JsonValue) -> Result<()> {
        self.bytes += self.get_writer()?.write(json)?;

        // close the current writer if it exceeds our limit
        if self.bytes > CHUNK_MAX_SIZE {
            self.writer.take();
            self.chunk += 1;
            self.bytes = 0;
        }

        Ok(())
    }
}

// https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview

fn convert(logfile: &str, json: &str) -> Result<()> {
    let input = File::open(logfile)?;
    let mut reader = BufReader::new(input);

    let mut writer = SplitWriter::new(json);

    let mut records = Vec::new();
    loop {
        match LogRecord::read(&mut reader) {
            Ok(record) => records.push(record),
            Err(LogReadError::Eof) => break,
            Err(LogReadError::DecodeError(err)) => {
                return Err(Error::new(ErrorKind::Other, err));
            }
        };
    }

    // the chrome tracing format expects the traces to be sorted by
    // timestamp within a worker, we simply do a global sort since it
    // seems otherwise sometimes miss flow events
    records.sort_by_key(|rec| rec.timestamp);

    for record in records {
        let ts = record.timestamp as f64 / 1_000.0; // ns -> us
        let name = format!("{:?}", record.activity_type);
        let tid = record.local_worker;
        let pid = 0;
        let cat = format!("ActivityType::{:?}", record.activity_type);
        let id = JsonValue::from(record.correlator_id);
        let args = object! {
            &format!("{:?}", record.event_type) => format!("{:#?}", record)
        };

        let mut event = object! {
            "name" => name,
            "cat" => cat,
            "ts" => ts,
            "tid" => tid,
            "pid" => pid,
            "args" => args
        };

        match record.event_type {
            EventType::Start => {
                // start of a duration event
                event["ph"] = "B".into();
                writer.write(&event)?;
            }
            EventType::End => {
                // end of a duration event
                event["ph"] = "E".into();
                writer.write(&event)?;
            }
            EventType::Sent => {
                // start enclosing duration
                event["ph"] = "B".into();
                writer.write(&event)?;

                // start flow event
                event["ph"] = "s".into();
                event["id"] = id;
                writer.write(&event)?;

                // end enclosing duration
                event["ph"] = "E".into();
                event.remove("id");
                writer.write(&event)?;
            }
            EventType::Received => {
                // start enclosing duration
                event["ph"] = "B".into();
                writer.write(&event)?;

                // finish flow event
                event["ph"] = "t".into();
                event["id"] = id;
                writer.write(&event)?;

                // end enclosing duration
                event["ph"] = "E".into();
                event.remove("id");
                writer.write(&event)?;
            }
            _ => unreachable!("invalid event type in msgpack log"),
        };
    }

    Ok(())
}

fn main() {
    let input = env::args()
        .nth(1)
        .expect("required argument: logfromat input file");
    let output = env::args()
        .nth(2)
        .unwrap_or_else(|| format!("{}.chrome", input));

    if let Err(err) = convert(&input, &output) {
        println!("Conversion ended with: {}", err.description());
    }
}
