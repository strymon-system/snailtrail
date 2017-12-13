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
extern crate svg;

use std::collections::HashMap;
use logformat::{LogRecord, EventType, ActivityType};

use clap::{App, Arg};

trait PositionalMap<K: Eq, V> {
    fn positional_insert(&mut self, k: K, v: V) -> (usize, Option<V>);
    fn find(&self, k: &K) -> Option<(usize, &V)>;
    fn find_mut(&mut self, k: &K) -> Option<(usize, &mut V)>;
}

trait StablePositionalMap<K: Eq, V> {
    fn positional_remove(&mut self, k: &K) -> Option<(usize, V)>;
}

impl<K: Eq, V> PositionalMap<K, V> for Vec<(K, V)> {
    fn positional_insert(&mut self, k: K, v: V) -> (usize, Option<V>) {
        if self.find(&k).is_some() {
            let (pos, value_cell) = self.find_mut(&k).unwrap();
            let old = ::std::mem::replace(value_cell, v);
            (pos, Some(old))
        } else {
            self.push((k, v));
            (self.len() - 1, None)
        }
    }
    fn find(&self, k: &K) -> Option<(usize, &V)> {
        self.iter()
            .enumerate()
            .find(|&(_, &(ref cell_key, _))| k == cell_key)
            .map(|(i, &(_, ref value))| (i, value))
    }
    fn find_mut(&mut self, k: &K) -> Option<(usize, &mut V)> {
        self.iter_mut()
            .enumerate()
            .find(|&(_, &mut (ref cell_key, _))| k == cell_key)
            .map(|(i, &mut (_, ref mut value))| (i, value))
    }
}

impl<K: Eq, V> PositionalMap<K, V> for Vec<Option<(K, V)>> {
    fn positional_insert(&mut self, k: K, v: V) -> (usize, Option<V>) {
        if self.find(&k).is_some() {
            let (pos, value_cell) = self.find_mut(&k).unwrap();
            let old = ::std::mem::replace(value_cell, v);
            (pos, Some(old))
        } else {
            if self.iter().filter(|x| x.is_none()).next().is_some() {
                let (i, cell) = self.iter_mut()
                    .enumerate()
                    .filter(|&(_, ref x)| x.is_none())
                    .next()
                    .unwrap();
                *cell = Some((k, v));
                (i, None)
            } else {
                self.push(Some((k, v)));
                (self.len() - 1, None)
            }
        }
    }
    fn find(&self, k: &K) -> Option<(usize, &V)> {
        self.iter()
            .enumerate()
            .filter_map(|(i, cell)| {
                            cell.as_ref()
                                .and_then(|&(ref cell_key, ref value)| if k == cell_key {
                                              Some((i, value))
                                          } else {
                                              None
                                          })
                        })
            .next()
    }
    fn find_mut(&mut self, k: &K) -> Option<(usize, &mut V)> {
        self.iter_mut()
            .enumerate()
            .filter_map(|(i, cell)| {
                cell.as_mut()
                    .and_then(|&mut (ref cell_key, ref mut value)| if k == cell_key {
                                  Some((i, value))
                              } else {
                                  None
                              })
            })
            .next()
    }
}

impl<K: Eq, V> StablePositionalMap<K, V> for Vec<Option<(K, V)>> {
    fn positional_remove(&mut self, k: &K) -> Option<(usize, V)> {
        self.iter_mut()
            .enumerate()
            .find(|&(_, ref cell)| match *cell {
                        &mut Some((ref cell_key, _)) => cell_key == k,
                        &mut None => false,
                    })
            .map(|(i, cell)| (i, cell.take().map(|(_, v)| v).expect("must be some")))
    }
}

struct OpenActivity {
    record: LogRecord,
}

struct WorkerTimeline {
    open_activities: Vec<Option<(u64, OpenActivity)>>,
}

impl WorkerTimeline {
    fn new() -> WorkerTimeline {
        WorkerTimeline { open_activities: Vec::new() }
    }
}

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
        .arg(Arg::with_name("output")
                 .help("Output path")
                 .short("o")
                 .long("output")
                 .takes_value(true)
                 .required(true))
        .arg(Arg::with_name("skip-ts")
                 .help("Skip till this timestamp (ms)")
                 .short("s")
                 .long("skip-ts")
                 .takes_value(true)
                 .required(false))
        .arg(Arg::with_name("take")
                 .help("Max number of worker-local events to retain")
                 .short("n")
                 .takes_value(true)
                 .required(false))
        .get_matches();

    let message_delay = matches
        .value_of("message-delay")
        .map(|m| m.parse::<u64>().expect("message-delay must be u64"));

    let log_path = matches.value_of("INPUT").unwrap();
    let out_path = matches.value_of("output").unwrap();
    let skip_ts: Option<u64> =
        matches
            .value_of("skip-ts")
            .map(|x| x.parse::<u64>().expect("start-ts must be usize") * 1_000_000);
    let take: usize = matches
        .value_of("take")
        .map(|x| x.parse().expect("event-count must be usize"))
        .unwrap_or(5_000);

    let (records, workers) =
        {
            let mut records = pag_construction::input::read_sorted_trace_from_file_and_cut_messages(log_path, message_delay);
            let workers = pag_construction::input::workers_in_trace(&records);
            ::pag_construction::input::infer_correlator_ids(&workers, &mut records);
            (records, workers)
        };

    eprintln!("{} records", records.len());

    let num_workers = workers.len();
    let mut timelines: Vec<(u32, WorkerTimeline)> = workers
        .into_iter()
        .map(|w| (w, WorkerTimeline::new()))
        .collect();

    fn worker_width() -> u64 {
        120
    }

    fn worker_offset(worker_pos: usize) -> u64 {
        (150 + worker_pos * worker_width() as usize) as u64
    }

    fn offset(worker_pos: usize, pos: usize) -> u64 {
        (worker_offset(worker_pos) as usize + pos * 25 + 5) as u64
    }

    fn width() -> u64 {
        20
    }

    fn activity_name(activity_type: ActivityType) -> &'static str {
        match activity_type {
            ActivityType::Input => "in",
            ActivityType::Buffer => "buf",
            ActivityType::Scheduling => "schd",
            ActivityType::Processing => "proc",
            ActivityType::BarrierProcessing => "barr",
            ActivityType::Serialization => "ser",
            ActivityType::Deserialization => "deser",
            ActivityType::FaultTolerance => "ft",
            ActivityType::ControlMessage => "cmsg",
            ActivityType::DataMessage => "dmsg",
            ActivityType::Unknown => "unk",
            ActivityType::Waiting => "wait",
            ActivityType::BusyWaiting => "busy",
        }
    }

    fn activity_fill(activity_type: ActivityType) -> &'static str {
        match activity_type {
            ActivityType::Processing => "#aaf",
            ActivityType::Serialization => "#aa0",
            ActivityType::Deserialization => "#afa",
            _ => "none",
        }
    }

    fn message_pattern(activity_type: ActivityType) -> &'static str {
        match activity_type {
            _ => "",
        }
    }

    fn render_activity(left: u64,
                       width: u64,
                       from: u64,
                       to: u64,
                       name: &'static str,
                       fill: &'static str,
                       _op_id: Option<&str>)
                       -> ::svg::node::element::Group {

        use svg::node::element::*;
        Group::new()
            .add(Rectangle::new()
                     .set("x", left)
                     .set("y", from)
                     .set("width", width)
                     .set("height", to - from)
                     .set("fill", fill)
                     .set("stroke", "black")
                     .set("stroke-width", 2))
            .add(Text::new()
                     .set("x", left)
                     .set("y", from + 10)
                     .add(::svg::node::Text::new(name)))
    }

    fn render_message(left1: u64,
                      left2: u64,
                      width: u64,
                      from: u64,
                      to: u64,
                      _pattern: &'static str)
                      -> ::svg::node::element::Group {

        use svg::node::element::*;
        Group::new()
            .add(Line::new()
                     .set("x1", left1)
                     .set("y1", from)
                     .set("x2", left1 + width)
                     .set("y2", from)
                     .set("stroke", "black")
                     .set("stroke-width", 1))
            .add(Line::new()
                     .set("x1", left2)
                     .set("y1", to)
                     .set("x2", left2 + width)
                     .set("y2", to)
                     .set("stroke", "black")
                     .set("stroke-width", 1))
            .add(Line::new()
                     .set("x1", left1 + width / 2)
                     .set("y1", from)
                     .set("x2", left2 + width / 2)
                     .set("y2", to)
                     .set("stroke", "black")
                     .set("stroke-width", 1))
    }

    let mut groups = Vec::new();
    let mut last_ts = 0;

    let worker_local_records = || records.iter().filter(|r| r.activity_type.is_worker_local());

    let ts_base = skip_ts.unwrap_or(worker_local_records()
                                        .next()
                                        .expect("expected one event")
                                        .timestamp);
    eprintln!("timestamp base  at {} ({} ms)",
              ts_base,
              ts_base / 1_000_000);

    let ts_to_px = |ns: u64| (ns - ts_base) / 25_000;

    let mut taken = 0;
    for record in worker_local_records() {
        let (worker_pos, timeline) = timelines
            .find_mut(&record.local_worker)
            .expect("worker not found");
        match record.event_type {
            EventType::Start => {
                let (pos, removed) =
                    timeline
                        .open_activities
                        .positional_insert(record.correlator_id.expect("correlator_ids expected"),
                                           OpenActivity { record: record.clone() });
                assert!(pos < 4);
                assert!(removed.is_none());
            }
            EventType::End => {
                let (pos, open_activity) = timeline
                    .open_activities
                    .positional_remove(&record.correlator_id.expect("correlator_ids expected"))
                    .expect("end without start");

                let operator_id = open_activity.record.operator_id.map(|x| format!("{}", x));
                let operator_id_str = operator_id.as_ref().map(|x| x.as_str());
                assert!(pos < 4); // check we fit in the worker column

                if record.timestamp >= ts_base {
                    groups.push(render_activity(offset(worker_pos, pos),
                                                width(),
                                                ts_to_px(open_activity.record.timestamp),
                                                ts_to_px(record.timestamp),
                                                activity_name(open_activity.record.activity_type),
                                                activity_fill(open_activity.record.activity_type),
                                                operator_id_str));
                    last_ts = record.timestamp;
                    taken += 1;
                    if taken >= take {
                        break;
                    }
                }
            }
            EventType::Bogus => (),
            _ => panic!("unexpected event type: {:?}", record),
        }
    }

    let mut open_messages = HashMap::new();

    for record in records
            .iter()
            .filter(|r| !r.activity_type.is_worker_local()) {
        match record.event_type {
            EventType::Sent => {
                if record.timestamp > last_ts {
                    break;
                }
                open_messages.insert(record.correlator_id.expect("correlator_ids expected"),
                                     record.clone());
            }
            EventType::Received => {
                let open_message = open_messages
                    .remove(&record.correlator_id.expect("correlator_ids expected"))
                    .expect("end without start");

                let (start_worker_pos, _) = timelines
                    .find_mut(&open_message.local_worker)
                    .expect("worker not found");
                let (end_worker_pos, _) = timelines
                    .find_mut(&record.local_worker)
                    .expect("worker not found");

                if record.timestamp >= ts_base {
                    groups.push(render_message(worker_offset(start_worker_pos),
                                               worker_offset(end_worker_pos),
                                               worker_width(),
                                               ts_to_px(open_message.timestamp),
                                               ts_to_px(record.timestamp),
                                               message_pattern(open_message.activity_type)));
                }
            }
            EventType::Bogus => (),
            _ => panic!("unexpected event type: {:?}", record),
        }
    }

    fn tick_distance_ts() -> u64 {
        10_000_000
    }


    eprintln!("last visible timestamp {}, {} visible events",
              last_ts,
              groups.len());
    let document = ::svg::Document::new()
        .set("width", offset(num_workers, 0))
        .set("height", ts_to_px(last_ts) + 10);

    let ts_base_upper = ((ts_base - 1) / tick_distance_ts() + 1) * tick_distance_ts();

    let document = document.add((0..num_workers).fold(::svg::node::element::Group::new(),
                                                      |g, w| {
        use svg::node::element::*;
        g.add(Rectangle::new()
                  .set("x", worker_offset(w))
                  .set("y", 0)
                  .set("width", worker_width() - 40)
                  .set("height", ts_to_px(last_ts))
                  .set("fill", "#eee"))
    }));

    let document = document.add((0..((last_ts - ts_base) / tick_distance_ts()))
                                    .map(|x| x * tick_distance_ts() + ts_base_upper)
                                    .fold(::svg::node::element::Group::new(), |g, dist| {
        g.add(::svg::node::element::Line::new()
                     .set("x1", 0)
                     .set("y1", ts_to_px(dist))
                     .set("x2", worker_offset(num_workers))
                     .set("y2", ts_to_px(dist))
                     .set("stroke", "black")
                     .set("stroke-width", 2)
                     .set("stroke-dasharray", "10, 10"))
            .add(::svg::node::element::Text::new()
                     .set("x", 0)
                     .set("y", ts_to_px(dist))
                     .add(::svg::node::Text::new(format!("{}ms", dist / 1_000_000))))
    }));

    let document = groups.into_iter().fold(document, |d, g| d.add(g));


    ::svg::save(out_path, &document).expect("failed saving svg");
}
