// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std;
use std::collections::HashMap;
use std::hash::Hash;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::{Filter, Inspect, Map, Unary};
use timely::dataflow::{Scope, Stream};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;

use logformat::{ActivityType, EventType, LogRecord};

use {PagEdge, PagNode, PagOutput};

use std::fs::File;
use std::io::prelude::*;

/// Pairs up events local to a single worker timeline and closes gaps by merging events or adding
/// filler edges.
pub trait DumpPAG<S: Scope> {
    fn dump_graph(&self, prefix: &str);
    fn dump_msgpack(&self, prefix: &str);
}

pub trait DumpPAGFormatting {
    // Converts a PagNode to a string representation
    fn format(&self) -> String;
}

impl DumpPAGFormatting for PagNode {
    // Converts a PagNode to a string representation
    fn format(&self) -> String {
        format!("{:?} {:?}", self.timestamp, self.worker_id)
    }
}

impl DumpPAGFormatting for PagEdge {
    // Converts a PagLabel to an endge description
    fn format(&self) -> String {
        format!("{:?}", self.edge_type)
    }
}

impl<S: Scope<Timestamp = Product<RootTimestamp, u64>>> DumpPAG<S> for Stream<S, PagOutput>
where
    S::Timestamp: std::fmt::Debug + Hash,
{
    fn dump_graph(&self, prefix: &str) {
        let prefix = prefix.to_owned();
        let mut pag_per_epoch = HashMap::new();
        self.unary_notify::<(), _, _>(
            Exchange::new(|_| 0),
            "Dump graph",
            vec![],
            move |input, _output, notificator| {
                // Organize all data by time and then according to worker ID
                input.for_each(|time, data| {
                    let epoch_slot = pag_per_epoch.entry(*time.time()).or_insert_with(Vec::new);
                    for pag in data.drain(..) {
                        if let PagOutput::Edge(record) = pag {
                            epoch_slot.push(record);
                        }
                    }
                    notificator.notify_at(time);
                });
                // Sequentially assemble the edges for each worker timeline by pairing up log records
                notificator.for_each(|time, _count, _notify| {
                    if let Some(mut timelines) = pag_per_epoch.remove(time.time()) {
                        let path = format!("{}graph_{:?}.dot", prefix, time.time().inner);
                        let path = std::path::Path::new(&path);
                        if let Some(dir) = path.parent() {
                            std::fs::DirBuilder::new()
                                .recursive(true)
                                .create(dir)
                                .unwrap();
                        }
                        let mut file = match File::create(path) {
                            Err(why) => panic!("couldn't open {:?}: {:?}", path, why),
                            Ok(file) => file,
                        };
                        file.write_all(b"digraph G {\n").unwrap();
                        file.write_all(b" node[shape=\"box\"];\n").unwrap();

                        let mut local_timeline_per_worker = HashMap::new();
                        let mut communication = vec![];

                        for e in timelines.drain(..) {
                            if e.source.worker_id == e.destination.worker_id {
                                local_timeline_per_worker
                                    .entry(e.source.worker_id)
                                    .or_insert_with(Vec::new)
                                    .push(e);
                            } else {
                                communication.push(e);
                            }
                        }

                        for (worker, mut timeline) in local_timeline_per_worker.drain() {
                            timeline.sort_by(|a, b| a.source.timestamp.cmp(&b.source.timestamp));
                            file.write_all(format!("subgraph cluster{} {{\n", worker).as_bytes())
                                .unwrap();
                            for e in timeline {
                                file.write_all(
                                    format!(
                                        "  {:?} -> {:?} [label = {:?}];\n",
                                        e.source.format(),
                                        e.destination.format(),
                                        e.format()
                                    )
                                    .as_bytes(),
                                )
                                .unwrap();
                            }
                            file.write_all(b"}\n").unwrap();
                        }
                        for e in communication.drain(..) {
                            file.write_all(
                                format!(
                                    "  {:?} -> {:?} [label = {:?}];\n",
                                    e.source.format(),
                                    e.destination.format(),
                                    e.format()
                                )
                                .as_bytes(),
                            )
                            .unwrap();
                        }
                        file.write_all(b"}").unwrap();
                    }
                });
            },
        );
    }

    fn dump_msgpack(&self, prefix: &str) {
        let prefix = prefix.to_owned();
        let mut pag_per_epoch = HashMap::new();
        self.unary_notify::<(), _, _>(
            Exchange::new(|_| 0),
            "Dump graph to msgpack",
            vec![],
            move |input, _output, notificator| {
                // Organize all data by time and then according to worker ID
                input.for_each(|time, data| {
                    let epoch_slot = pag_per_epoch.entry(*time.time()).or_insert_with(Vec::new);
                    for pag in data.drain(..) {
                        if let PagOutput::Edge(record) = pag {
                            epoch_slot.push(record);
                        }
                    }
                    notificator.notify_at(time);
                });
                // Sequentially assemble the edges for each worker timeline by pairing up log records
                notificator.for_each(|time, _count, _notify| {
                    if let Some(mut timelines) = pag_per_epoch.remove(time.time()) {
                        let path = format!("{}pag_{:?}.msgpack", prefix, time.time().inner);
                        let path = std::path::Path::new(&path);
                        if let Some(dir) = path.parent() {
                            std::fs::DirBuilder::new()
                                .recursive(true)
                                .create(dir)
                                .unwrap();
                        }
                        let file = match File::create(path) {
                            Err(why) => panic!("couldn't open {:?}: {:?}", path, why),
                            Ok(file) => file,
                        };
                        let mut writer = std::io::BufWriter::new(file);
                        // makes a record from a PagNode and additional info
                        fn to_record(
                            correlator_id: u64,
                            pag_node: PagNode,
                            activity_type: ActivityType,
                            operator_id: Option<u32>,
                            is_message: bool,
                            is_start: bool,
                            remote_worker: Option<u32>,
                        ) -> LogRecord {
                            LogRecord {
                                timestamp: pag_node.timestamp,
                                local_worker: pag_node.worker_id,
                                activity_type: activity_type,
                                operator_id: operator_id,
                                remote_worker: remote_worker,
                                event_type: match (is_start, is_message) {
                                    (true, true) => EventType::Sent,
                                    (false, true) => EventType::Received,
                                    (true, false) => EventType::Start,
                                    (false, false) => EventType::End,
                                },
                                correlator_id: Some(correlator_id),
                            }
                        }
                        let mut correlator_id: u64 = 1;
                        for PagEdge {
                            source,
                            destination,
                            edge_type,
                            operator_id,
                            ..
                        } in timelines.drain(..)
                        {
                            let first = to_record(
                                correlator_id,
                                source,
                                edge_type,
                                operator_id,
                                !edge_type.is_worker_local(),
                                true,
                                Some(destination.worker_id),
                            );
                            first.write(&mut writer).unwrap();
                            let second = to_record(
                                correlator_id,
                                destination,
                                edge_type,
                                operator_id,
                                !edge_type.is_worker_local(),
                                false,
                                Some(source.worker_id),
                            );
                            second.write(&mut writer).unwrap();
                            correlator_id += 1;
                        }
                    }
                });
            },
        );
    }
}

pub trait DumpHistogram<S: Scope> {
    fn dump_histogram(&self) -> Stream<S, (i64, u64)>;
}

impl<S: Scope> DumpHistogram<S> for Stream<S, LogRecord>
where
    S::Timestamp: Hash,
{
    fn dump_histogram(&self) -> Stream<S, (i64, u64)> {
        // Retain communication edges
        self.filter(|rec| rec.remote_worker.is_some())
            // transform to (key, edge) format
            .map(|rec| (rec.correlator_id, rec))
            // Aggregate per key
            .aggregate::<_, Vec<_>, _, _, _>(
                |_key, val, agg| agg.push(val),
                |_key, mut agg| {
                    let tsdiff = if agg.len() > 1 {
                        // Make sure send event is always first
                        if agg[0].event_type == EventType::Received {
                            agg.swap(0, 1);
                        }
                        let first_ts = agg[0].timestamp as i64;
                        let second_ts = agg[1].timestamp as i64;
                        (second_ts - first_ts) / 100_000_000
                    } else {
                        0
                    };
                    (agg, tsdiff + u32::max_value() as i64)
                },
                |key| key.unwrap_or(0),
            )
            // Retain where two aggregates, i.e. matched communication
            .filter(|&(ref agg, _)| agg.len() == 2)
            // Transform to grouping for histogram
            .map(|(_, tsdiff)| (tsdiff as i64, 0))
            // Count elements per bucket
            .aggregate::<_, u64, _, _, _>(
                |_key, _val, agg| *agg += 1,
                |key, agg| (key, agg),
                |key| *key as u64,
            )
            // Print it
            .inspect(|&(key, agg)| {
                println!(
                    "DEBUG histogram {} {}",
                    (key - u32::max_value() as i64) as f64 / 10.,
                    agg
                )
            })
    }
}
