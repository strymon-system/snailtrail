// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;
extern crate logformat;
extern crate rand;
extern crate rayon;
extern crate snailtrail;
extern crate time;
extern crate timely;
extern crate timely_communication;

use logformat::{LogRecord, ActivityType, EventType, Worker, Timestamp, OperatorId};

use snailtrail::graph::{Partitioning, SrcDst};
use snailtrail::exploration::Capacity;

use std::collections::HashMap;
use std::hash::Hash;

use timely::ExchangeData;
use timely::dataflow::channels::pact::{Exchange, Pipeline};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::{Concat, Filter, Map, Unary, Partition};
use timely::dataflow::{Scope, Stream};
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use snailtrail::hash_code;

pub mod dataflow;
pub mod input;
pub mod output;

/// A node in the activity graph
#[derive(Abomonation, Clone, Debug, PartialEq, Hash, Eq, Copy, Ord, PartialOrd)]
pub struct PagNode {
    pub timestamp: Timestamp,
    pub worker_id: Worker,
}

impl Partitioning for PagNode {
    fn partition(&self) -> u64 {
        u64::from(self.worker_id)
    }
}

impl<'a> From<&'a LogRecord> for PagNode {
    fn from(record: &'a LogRecord) -> Self {
        PagNode {
            timestamp: record.timestamp,
            worker_id: record.local_worker,
        }
    }
}

/// Elements of a complete activity graph, including ingress/egress points
#[derive(Abomonation, Clone, Debug, Eq, Hash, PartialEq)]
pub enum PagOutput {
    // Entry point into the graph
    StartNode(PagNode),
    // Exit point from the graph
    EndNode(PagNode),
    // Graph edges
    Edge(PagEdge),
}

impl SrcDst<PagNode> for PagOutput {
    fn src(&self) -> Option<PagNode> {
        match *self {
            PagOutput::StartNode(_) => None,
            PagOutput::EndNode(ref n) => Some(*n),
            PagOutput::Edge(ref e) => Some(e.source),
        }
    }

    fn dst(&self) -> Option<PagNode> {
        match *self {
            PagOutput::StartNode(ref n) => Some(*n),
            PagOutput::EndNode(_) => None,
            PagOutput::Edge(ref e) => Some(e.destination),
        }
    }
}

/// Information on how traverse an edge
#[derive(Abomonation, Hash, Clone, Eq, Ord, PartialEq, PartialOrd, Debug)]
pub enum TraversalType {
    Undefined,
    Block,
    Unbounded,
}

/// An edge in the activity graph
#[derive(Abomonation, Clone, Debug, PartialEq, Hash, Eq, PartialOrd, Ord)]
pub struct PagEdge {
    /// The source node
    pub source: PagNode,
    /// The destination node
    pub destination: PagNode,
    /// The activity type
    pub edge_type: ActivityType,
    /// An optional operator ID
    pub operator_id: Option<OperatorId>,
    /// Edge dependency information
    pub traverse: TraversalType,
}

impl PagEdge {
    pub fn weight(&self) -> u64 {
        (self.destination.timestamp as i64 - self.source.timestamp as i64).abs() as u64
    }

    pub fn is_message(&self) -> bool {
        self.source.worker_id != self.destination.worker_id
    }
}

impl PagOutput {
    pub fn weight(&self) -> u64 {
        match *self {
            PagOutput::Edge(ref e) => e.weight(),
            _ => 0,
        }
    }

    pub fn destination(&self) -> &PagNode {
        match *self {
            PagOutput::Edge(ref e) => &e.destination,
            PagOutput::StartNode(ref e) |
            PagOutput::EndNode(ref e) => e,
        }
    }

    pub fn destination_worker(&self) -> Worker {
        match *self {
            PagOutput::Edge(ref e) => e.destination.worker_id,
            PagOutput::StartNode(ref e) |
            PagOutput::EndNode(ref e) => e.worker_id,
        }
    }

    pub fn source_timestamp(&self) -> Timestamp {
        match *self {
            PagOutput::Edge(ref e) => e.source.timestamp,
            PagOutput::StartNode(ref e) |
            PagOutput::EndNode(ref e) => e.timestamp,
        }
    }

    pub fn destination_timestamp(&self) -> Timestamp {
        match *self {
            PagOutput::Edge(ref e) => e.destination.timestamp,
            PagOutput::StartNode(ref e) |
            PagOutput::EndNode(ref e) => e.timestamp,
        }
    }

    pub fn is_message(&self) -> bool {
        match *self {
            PagOutput::Edge(ref e) => e.is_message(),
            PagOutput::StartNode(_) |
            PagOutput::EndNode(_) => false,
        }
    }
}

pub struct TraverseNoWaiting;
impl<V: From<u8>> Capacity<PagOutput, V> for TraverseNoWaiting {
    fn apply_capacity(edge: &PagOutput, value: V) -> V {
        match *edge {
            PagOutput::StartNode(_) |
            PagOutput::EndNode(_) => From::from(0),
            PagOutput::Edge(ref e) => {
                match e.traverse {
                    TraversalType::Undefined => panic!("Undefined traversal capacity!"),
                    TraversalType::Block => From::from(0),
                    TraversalType::Unbounded => value,
                }
            }
        }
    }
}

pub struct TraverseWaitingOne;
impl<V: From<u8>> Capacity<PagOutput, V> for TraverseWaitingOne {
    fn apply_capacity(edge: &PagOutput, value: V) -> V {
        match *edge {
            PagOutput::StartNode(_) |
            PagOutput::EndNode(_) => From::from(0),
            PagOutput::Edge(ref e) => {
                match e.traverse {
                    TraversalType::Undefined => panic!("Undefined traversal capacity!"),
                    TraversalType::Block => From::from(1),
                    TraversalType::Unbounded => value,
                }
            }
        }
    }
}

// Used internal to this module during PAG construction.  We need a single stream containing all
// a worker's activity and an indication of whether it was entirely local or involved a remote
// worker.
#[derive(Abomonation, Clone, Debug)]
enum Timeline {
    Local(PagEdge), // full-edge: computation
    Remote(LogRecord), // half-edge: communication
}

impl Timeline {
    fn get_start_timestamp(&self) -> u64 {
        match *self {
            Timeline::Local(ref edge) => edge.source.timestamp,
            Timeline::Remote(ref rec) => rec.timestamp,
        }
    }

    fn get_end_timestamp(&self) -> u64 {
        match *self {
            Timeline::Local(ref edge) => edge.destination.timestamp,
            Timeline::Remote(ref rec) => rec.timestamp,
        }
    }

    fn get_worker_id(&self) -> Worker {
        match *self {
            Timeline::Local(ref edge) => edge.source.worker_id,
            Timeline::Remote(ref rec) => rec.local_worker,
        }
    }

    fn get_sort_key(&self) -> (Timestamp, Timestamp) {
        match *self {
            Timeline::Local(ref edge) => (edge.source.timestamp, edge.destination.timestamp),
            Timeline::Remote(ref rec) => (rec.timestamp, 0),
        }
    }
}

/// Collects all data within a single epoch and applies user-defined logic.
/// (A fusion of the `Accumulate` and `Map` operators but the logic is
/// triggered on notification rather than as each data element is delivered.)
trait MapEpoch<S: Scope, D: ExchangeData> {
    fn map_epoch<F: Fn(&mut Vec<D>) + 'static>(&self, logic: F) -> Stream<S, D>;
}

impl<S: Scope, D: ExchangeData> MapEpoch<S, D> for Stream<S, D>
    where S::Timestamp: Hash
{
    fn map_epoch<F: Fn(&mut Vec<D>) + 'static>(&self, logic: F) -> Stream<S, D> {
        let mut accums = HashMap::new();
        self.unary_notify(Pipeline,
                          "MapEpoch",
                          vec![],
                          move |input, output, notificator| {
            input.for_each(|time, data| {
                               accums
                                   .entry(time.time().clone())
                                   .or_insert_with(Vec::new)
                                   .extend_from_slice(data);
                               notificator.notify_at(time);
                           });

            notificator.for_each(|time, _count, _notify| if let Some(mut accum) =
                accums.remove(time.time()) {
                                     logic(&mut accum);
                                     output.session(&time).give_iterator(accum.drain(..));
                                 });
        })
    }
}

fn create_initial_pag_edges(worker_id: Worker,
                            mut timeline: Vec<LogRecord>,
                            window_size_ns: u64,
                            window_start_time: Timestamp)
                            -> Vec<Timeline> {
    // We insert two records just before and after the window boundaries.
    // This will cause the analysis later on to include potential gaps between
    // the window boundary and first/last activity in the wait state analysis
    // to insert Unknown/Waiting activities accordingly.
    timeline.push(LogRecord {
                      timestamp: window_start_time * window_size_ns - 1,
                      local_worker: worker_id,
                      activity_type: ActivityType::Unknown,
                      event_type: EventType::Bogus,
                      correlator_id: None,
                      remote_worker: None,
                      operator_id: None,
                  });
    timeline.push(LogRecord {
                      timestamp: window_start_time * window_size_ns + window_size_ns,
                      local_worker: worker_id,
                      activity_type: ActivityType::Unknown,
                      event_type: EventType::Bogus,
                      correlator_id: None,
                      remote_worker: None,
                      operator_id: None,
                  });

    // Sort local events by timestamp
    timeline.sort_by_key(|record| record.timestamp);

    // Split timeline events two ways, according to their type
    let mut communication = Vec::new();

    // Events may be nested.  Unravel and switch between these to fit our model
    // of exactly one activity per worker at any given moment.  Be aware, our
    // model still does not allow for overlap (as with concurrent events; e.g.
    // A [0,2] and B [1,3]) and this matching logic will fail due to a mismatch
    // between the current event and top of the stack.
    let mut new_timeline = Vec::new();
    // Stack contains open 'Start' events
    let mut stack: Vec<LogRecord> = Vec::new();

    // last record that caused some edge to be emitted. This is used to track
    // the start time stamp for an edge.
    let mut last_end: Option<LogRecord> = None;
    for record in timeline {
        // println!("record: {:?} \n   last_end: {:?}", record, last_end);
        match record.event_type {
            EventType::Start => {
                if let Some(top) = stack.last() {
                    // Close any pending nested activities to avoid overlap
                    let prev = last_end.as_ref().unwrap_or(top);

                    assert!(prev.timestamp <= record.timestamp);
                    assert_eq!(prev.local_worker, record.local_worker);
                    // Output edge from some event to here
                    // If a previous Start event, let A, is open,
                    // this assumes that the activity that started with the event A finishes at the presence of the current 'Start' event,
                    // even if the activity that started with A was in fact interrupted by another activity.

                    // In case of two subsequent 'Start' events, this will add an edge whose endpoints have different activity types
                    new_timeline.push(Timeline::Local(PagEdge {
                                                          source: PagNode::from(prev),
                                                          destination:
                                                              PagNode::from(&record.clone()),
                                                          edge_type: top.activity_type,
                                                          operator_id: top.operator_id,
                                                          traverse: TraversalType::Undefined,
                                                      }));
                    //assert_eq!(prev.activity_type, record.activity_type, "mismatch activity type: prev={:?}, record={:?}", prev, record);
                }
                // remember the current event as the last produced event
                last_end = Some(record.clone());
                stack.push(record);
            }
            EventType::End => {
                // We cannot assume there will be a matching element on the
                // stack because there might be an unpaired end event with no
                // corresponding start, for example due to having sliced the
                // trace along the time axis into disjoint time windows.

                // Check if there is a corresponding start even on the stack,
                // i.e. same type. If so, pop until it has been found
                // Otherwise, ignore it.
                if stack
                       .iter()
                       .rev()
                       .any(|r| r.activity_type == record.activity_type) {
                    while let Some(top) = stack.pop() {
                        assert!(top.timestamp <= record.timestamp);
                        assert_eq!(top.local_worker, record.local_worker);
                        // println!("stack: {:?} top: {:?}", stack, top);
                        if top.activity_type == record.activity_type {

                            let prev = last_end.unwrap_or_else(||top.clone());
                            //assert_eq!(prev.activity_type, record.activity_type, "mismatch activity type: prev={:?}, record={:?}", prev, record);
                            if prev.timestamp < record.timestamp {
                                new_timeline.push(Timeline::Local(PagEdge {
                                                                      source: PagNode::from(&prev),
                                                                      destination:
                                                                          PagNode::from(&record),
                                                                      edge_type:
                                                                          record.activity_type,
                                                                      operator_id:
                                                                          record.operator_id,
                                                                      traverse:
                                                                          TraversalType::Undefined,
                                                                  }));
                            }
                            // Whatever was previously nested will be emitted as a new edge
                            // once the next activity starts (match clause above -> non-empty stack).
                            last_end = Some(record.clone());
                            break;
                        }
                    }
                } else {
                    // Terminate the activity and set start to last emitted time
                    let prev = if last_end.is_none() {
                        LogRecord {
                            timestamp: window_start_time * window_size_ns,
                            ..record
                        }
                    } else {
                        last_end.unwrap()
                    };
                    new_timeline.push(Timeline::Local(PagEdge {
                                                          source: PagNode::from(&prev),
                                                          destination: PagNode::from(&record),
                                                          edge_type: record.activity_type,
                                                          operator_id: record.operator_id,
                                                          traverse: TraversalType::Undefined,
                                                      }));
                    last_end = Some(record);
                }
            }
            EventType::Sent | EventType::Received | EventType::Bogus => {
                // Split the current active activity when it is interrupted
                // by a message
                if let Some(top) = stack.last() {
                    assert!(top.timestamp <= record.timestamp);
                    assert_eq!(top.local_worker, record.local_worker);
                    let prev = last_end.unwrap();
                    // This adds an edge whose endpoints have different activity types
                    new_timeline.push(Timeline::Local(PagEdge {
                                                          source: PagNode::from(&prev),
                                                          destination: PagNode::from(&record),
                                                          edge_type: top.activity_type,
                                                          operator_id: top.operator_id,
                                                          traverse: TraversalType::Undefined,
                                                      }));
                }
                last_end = Some(record.clone());
                communication.push(record); // This also includes Bogus records
            }

        }
    }

    // Terminate the last activity if there are unterminated activities
    // The other will not be terminated
    if let Some(top) = stack.last() {
        // Close any pending nested activities to avoid overlap
        let prev = last_end.as_ref().unwrap_or(top);

        let record = LogRecord {
            timestamp: window_start_time * window_size_ns + window_size_ns,
            ..*top
        };
        if prev.timestamp < record.timestamp {
            assert_eq!(prev.local_worker, record.local_worker);

            new_timeline.push(Timeline::Local(PagEdge {
                                                  source: PagNode::from(prev),
                                                  destination: PagNode::from(&record.clone()),
                                                  edge_type: top.activity_type,
                                                  operator_id: top.operator_id,
                                                  traverse: TraversalType::Undefined,
                                              }));
        }
    }

    // This sanity check is disabled because we might have seen the start of
    // computation events but without a matching end event.  It would be
    // possible to clamp any unfinished execution to the end of the PAG slice
    // but we currently dont't know the two end-points of the time window.
    //assert!(stack.is_empty(), format!("unterminated activities are still pending: {:?}", stack));

    // Merge in all communication (half-edges) and ensure all events are in the
    // same order as they originally occurred.
    new_timeline.extend(communication.into_iter().map(Timeline::Remote));
    // (This step may not be necessary, however the stack unravelling step
    // above may have withheld events and altered the event ordering.)
    new_timeline.sort_by_key(Timeline::get_sort_key);

    new_timeline
}


fn connect_pag_and_apply_wait_analysis(new_timeline: Vec<Timeline>,
                                       unknown_threshold: u64,
                                       insert_waitig_edges: bool,
                                       spark_driver_hack: bool)
                                       -> Vec<PagEdge> {
    // Perform another pass and splices in PAG edges with unknown activities to
    // fill any gaps and ensure the graph is connected.  Needs access to the
    // full set of worker activities (even remote) because the order events
    // occurred in needs to be respected.  For example, communication indicates
    // the worker was busy and this interruption results in *two* separate
    // filler edges.
    let mut final_timeline = Vec::new();

    // Iterate over pairs of elements from the new_timeline
    // Node: windows(2) cannot be used because it only gives
    // immutable access to its contents.
    let mut iter = new_timeline.into_iter();
    let mut last_event = iter.next();
    for mut second in iter {
        let mut first = last_event.unwrap();
        // println!("\nslice first: {:?}", first);
        // println!("slice scond: {:?}", second);
        let first_ts = first.get_end_timestamp();
        let worker_id = first.get_worker_id();
        // println!("delta t: {:?}", second_ts as i64 - first_ts as i64);

        // Does an Unknown edge need to be added to the PAG?
        let mut fill_gap = false;
        let mut edge_type = ActivityType::Unknown;

        let second_ts = second.get_start_timestamp();
        // Time difference between the two events currently analyzing
        let delta_t = second_ts as i64 - first_ts as i64;

        assert!(delta_t >= 0);

        // Do we need to bridge the gap with what we have?
        if delta_t < (unknown_threshold as i64) {
            // move end and start
            match first {
                Timeline::Local(ref mut left) => {
                    match second {
                        Timeline::Local(ref mut right) => {
                            // local->local: move start/end to mid point between the two
                            let mid_point = (first_ts + second_ts) / 2;
                            left.destination.timestamp = mid_point;
                            right.source.timestamp = mid_point;
                        }
                        Timeline::Remote(ref mut right) => {
                            // move left end time to message time
                            left.destination.timestamp = right.timestamp;
                        }
                    };
                }
                Timeline::Remote(ref mut left) => {
                    match second {
                        Timeline::Local(ref mut right) => {
                            // move right start time to message time
                            right.source.timestamp = left.timestamp;
                        }
                        Timeline::Remote(ref mut right) => {
                            // Cannot move adjacent messages, create a bridging edge
                            // In case (i) the first event is 'Sent' and the second is 'Received' or
                            // (ii) both events are Receieved, we should add a 'Waiting' instead of an 'Unknown' edge.
                            if right.event_type == EventType::Received {
                                if (left.event_type == EventType::Sent || spark_driver_hack) &&
                                   insert_waitig_edges {
                                    edge_type = ActivityType::Waiting;
                                }
                            }

                            fill_gap = true;
                        }
                    }
                }
            };
        } else if delta_t >= (unknown_threshold as i64) {
            // Gap is bigger threshold, create Unknown or Waiting instead of adjusting ends
            fill_gap = true;

            // only insert waiting edges if enabled
            if insert_waitig_edges {
                // check if the gap is ended by a receive activity
                if let Timeline::Remote(ref mut right) = second {
                    if right.event_type == EventType::Received {
                        // we sometimes insert a waiting here.
                        if spark_driver_hack {
                            // always for spark
                            edge_type = ActivityType::Waiting;
                        } else if let Timeline::Remote(ref left) = first {
                            if left.event_type == EventType::Sent {
                                // send, receive -> waiting
                                edge_type = ActivityType::Waiting;
                            }
                            // implicit else: receive, receive -> unknown
                        } else {
                            // local, receive -> waiting
                            edge_type = ActivityType::Waiting;
                        }
                    }
                }
            }
        }

        if let Timeline::Local(ref edge) = first {
            // Filter out and only retain worker-local activities.
            final_timeline.push(edge.clone());
        }

        if fill_gap {
            // Only for Spark: Replace 'Unknown' activity with 'Scheduling' in the driver
            if worker_id == 0 && edge_type == ActivityType::Unknown && spark_driver_hack {
                edge_type = ActivityType::Scheduling;
            }

            // Gap between timestamps -- indicates missing or incomplete instrumentation.
            final_timeline.push(PagEdge {
                                    source: PagNode {
                                        timestamp: first.get_end_timestamp(),
                                        worker_id: worker_id,
                                    },
                                    destination: PagNode {
                                        timestamp: second.get_start_timestamp(),
                                        worker_id: worker_id,
                                    },
                                    edge_type: edge_type,
                                    operator_id: None,
                                    traverse: TraversalType::Undefined,
                                });
        }
        last_event = Some(second);
    }

    // Handle last event
    if let Some(Timeline::Local(ref edge)) = last_event {
        final_timeline.push(edge.clone());
    }

    // This essentially removes local edges whose start time is larger than the end time. Do we need this check?
    final_timeline.retain(|edge| {
                              edge.source.worker_id != edge.destination.worker_id ||
                              edge.source.timestamp < edge.destination.timestamp
                          });

    // Sanity check
    for window in final_timeline.windows(2) {
        let first_ts = window[0].destination.timestamp;
        let second_ts = window[1].source.timestamp;
        assert_eq!(first_ts, second_ts);
    }

    final_timeline
}

/// Pairs up events local to a single worker timeline and closes gaps by merging events or adding
/// filler edges.
trait WorkerTimelines<S: Scope> {
    // The threshold parameter (unit: nanoseconds) determines when empty gaps between neighbouring
    // program activites will be combined and how large this time interval is allowed to be.  We
    // have not tested with real traces but expect 1_000_000_000 (== 1 millisecond) to be a
    // reasonable starting value.
    fn build_worker_timelines(&self,
                              unknown_threshold: u64,
                              window_size_ns: u64,
                              insert_waitig_edges: bool,
                              spark_driver_hack: bool)
                              -> Stream<S, PagOutput>;
}

impl<S: Scope<Timestamp = Product<RootTimestamp, u64>>> WorkerTimelines<S> for Stream<S, LogRecord>
    where S::Timestamp: Hash
{
    fn build_worker_timelines(&self,
                              unknown_threshold: u64,
                              window_size_ns: u64,
                              insert_waitig_edges: bool,
                              spark_driver_hack: bool)
                              -> Stream<S, PagOutput> {
        let mut timelines_per_epoch = HashMap::new();
        let exchange = Exchange::new(|record: &LogRecord| record.local_worker as u64);
        self.unary_notify(exchange, "WorkerTimelines", vec![], move |input, output, notificator| {
            // Organize all data by time and then according to worker ID
            input.for_each(|time, data| {
                let epoch_slot = timelines_per_epoch.entry(*time.time())
                    .or_insert_with(HashMap::new);
                for record in data.drain(..) {
                    epoch_slot.entry(record.local_worker)
                        .or_insert_with(Vec::new)
                        .push(record);
                }
                notificator.notify_at(time);
            });
            // Sequentially assemble the edges for each worker timeline by pairing up log records
            notificator.for_each(|time, _count, _notify| {
                if let Some(mut timelines) = timelines_per_epoch.remove(time.time()) {
                    for (worker_id, raw_timeline) in timelines.drain() {
                        // Assumption: the previous step should have already applied thresholding
                        // (quantization) to eliminate gaps and merge log records which are in
                        // close proximity in terms of event time.

                        let initial_timeline = create_initial_pag_edges(worker_id, raw_timeline, window_size_ns, time.time().inner);

                        let final_timeline = connect_pag_and_apply_wait_analysis(initial_timeline, unknown_threshold, insert_waitig_edges, spark_driver_hack);

                        // Emits the PAG together with markers of the first/last node on each
                        // worker timeline so that the edge ranking step has a root set to start
                        // its traversal from.
                        let mut session = output.session(&time);
                        session.give_iterator(final_timeline.first().map(|e| PagOutput::StartNode(e.source)).into_iter());
                        session.give_iterator(final_timeline.last().map(|e| PagOutput::EndNode(e.destination)).into_iter());
                        session.give_iterator(final_timeline.into_iter().map(PagOutput::Edge));
                    }
                }
            });
        })
    }
}



/// Matches up starting and ending program activities by pairing up occurences that have a matching
/// key.  This independently examines each epoch of the data stream (tumbling window of size 1).
trait PairUpEvents<S: Scope> {
    fn pair_up_events(&self,
                      start_type: EventType,
                      end_type: EventType,
                      window_size_ns: u64)
                      -> Stream<S, Timeline>;

    fn pair_up_events_and_check<F>(&self,
                                   start_type: EventType,
                                   end_type: EventType,
                                   window_size_ns: u64,
                                   assert_fn: F)
                                   -> Stream<S, Timeline>
        where F: Fn(&LogRecord, &LogRecord) -> () + 'static;
}

impl<S: Scope<Timestamp=Product<RootTimestamp, u64>>, K: ExchangeData+Eq+Hash> PairUpEvents<S> for Stream<S, (K, LogRecord)>
    where S::Timestamp: Hash
{
    fn pair_up_events(&self, start_type: EventType, end_type: EventType, window_size_ns: u64) -> Stream<S, Timeline> {
self.pair_up_events_and_check(start_type, end_type, window_size_ns, |_sent, _recv| {
/* no assertion */
})
    }

    fn pair_up_events_and_check<F>(&self, start_type: EventType, end_type: EventType, window_size_ns: u64, assert_fn: F) -> Stream<S, Timeline>
        where F: Fn(&LogRecord, &LogRecord) -> () + 'static
    {
        let paired = self.aggregate::<_, (Option<LogRecord>, Vec<LogRecord>), _, _, _>(
            move |_key, val, agg| {  // fold
                if val.event_type == start_type {
                    assert!(agg.0.is_none(), "duplicate start event");
                    agg.0 = Some(val)
                } else if val.event_type == end_type {
                    agg.1.push(val)
                } else {
                    panic!("unexpected event type for communication record ({:?})", val)
                }
            },
            |key, agg| (key, agg),
            |key| hash_code(key)
        );

        paired.unary_notify(Pipeline, "assemble messages", vec!(), move |input, output, _| {
            input.for_each(|time, data| {    // emit
                let mut session = output.session(&time);
                for (_, agg) in data.drain(..) {
                    let ends = agg.1;
                    match agg.0 {
                        Some(start) => {
                            // Is it a start w/o an end? Assume end is outside current window
                            if ends.is_empty() && start.remote_worker.is_some() {
                                session.give(Timeline::Local(PagEdge {
                                    source: PagNode {
                                        timestamp: start.timestamp,
                                        worker_id: start.local_worker,
                                    },
                                    destination: PagNode {
                                        timestamp: time.time().inner * window_size_ns + window_size_ns,
                                        worker_id: start.remote_worker.expect("comm w/o remote worker"),
                                    },
                                    edge_type: start.activity_type,
                                    operator_id: start.operator_id,
                                    traverse: TraversalType::Undefined,
                                }));
                                session.give(Timeline::Remote(LogRecord {
                                    timestamp: time.time().inner * window_size_ns + window_size_ns,
                                    local_worker: start.remote_worker.unwrap(),
                                    remote_worker: Some(start.local_worker),
                                    ..start
                                }));
                            } else {
                                for end in ends {
                                    assert_eq!(start.activity_type, end.activity_type);
                                    assert_fn(&start, &end);

                                    session.give(Timeline::Local(PagEdge {
                                        source: PagNode {
                                            timestamp: start.timestamp,
                                            worker_id: start.local_worker,
                                        },
                                        destination: PagNode {
                                            timestamp: end.timestamp,
                                            worker_id: end.local_worker,
                                        },
                                        edge_type: start.activity_type,
                                        operator_id: start.operator_id,
                                        traverse: TraversalType::Undefined,
                                    }));
                                }
                            }
                        },
                        // The events on either side may be missing either due to incomplete
                        // instrumentation, lossy logging or edges which span across multiple time
                        // windows.  For simplicity we drop these from our graph for now.
                        //
                        // TODO: retain spanned edges and either (i) split into two halves which are
                        // clamped to the current PAG slice or (ii) propagate edges until they're
// complete and we've seen both nodes and only emit then.
// (Some(_start), None) => {println!("DEBUG end missing {:?}", _start); None},
// (None, Some(_end)) => {println!("DEBUG start missing {:?}", _end); None},
                        None =>  {
                            for end in ends {
                                session.give(Timeline::Local(PagEdge {
                                    source: PagNode {
                                        timestamp: time.time().inner * window_size_ns- 1,
                                        worker_id: end.remote_worker.expect("comm w/o remote worker"),
                                    },
                                    destination: PagNode {
                                        timestamp: end.timestamp,
                                        worker_id: end.local_worker
                                    },
                                    edge_type: end.activity_type,
                                    operator_id: end.operator_id,
                                    traverse: TraversalType::Undefined,
                                }));
                                session.give(Timeline::Remote(LogRecord {
                                    timestamp: time.time().inner * window_size_ns - 1,
                                    local_worker: end.remote_worker.unwrap(),
                                    remote_worker: Some(end.local_worker),
                                    ..end
                                }));
                            }
                        },
                    };
                };
            });
        })
    }
}


/// Main entry point which converts a stream of events produced from instrumentation into a Program
/// Activity Graph (PAG).  This method expects log records which are batched into disjoint windows
/// of event time (epoch) and the output will contain a time-ordered stream of edges which include
/// both ends of an activity (e.g. start/end or send/receive pairs).

pub trait BuildProgramActivityGraph<S: Scope> {
    fn build_program_activity_graph(&self,
                                    threshold: u64,
                                    delayed_message_threshold: u64,
                                    window_size_ns: u64,
                                    insert_waitig_edges: bool,
                                    spark_driver_hack: bool)
                                    -> Stream<S, PagOutput>;
}

impl<S: Scope> BuildProgramActivityGraph<S> for Stream<S, LogRecord>
    where S: Scope<Timestamp = Product<RootTimestamp, u64>>,
          S::Timestamp: Hash
{
    fn build_program_activity_graph(&self,
                                    threshold: u64,
                                    delayed_message_threshold: u64,
                                    window_size_ns: u64,
                                    insert_waitig_edges: bool,
                                    spark_driver_hack: bool)
                                    -> Stream<S, PagOutput> {
        let input = self;
        // Check worker timelines for completeness

        // Step 1: Group events by worker

        // Step 2: pair up control and data events and add edges between worker timelines
        let communication_edges = input
            .filter(|record| (record.activity_type == ActivityType::ControlMessage ||
                            record.activity_type == ActivityType::DataMessage))
            .map(|record| {
                let sender_id = match record.event_type {
                    EventType::Sent => record.local_worker,
                    EventType::Received => record.remote_worker.unwrap(),
                    et =>  panic!("unexpected event type for communication record ({:?})", et),
                };
                // Assign key to group related message events
                ((sender_id, record.correlator_id), record)
            })
        // This matching logic largely duplicates the aggregation operator above but is
        // parameterized slightly differently -> make this into a reusable operator.
        .pair_up_events_and_check(EventType::Sent, EventType::Received, window_size_ns, |sent, recv| {
            assert!((sent.local_worker == recv.remote_worker.unwrap()) &&
                    (sent.remote_worker.is_none() || sent.remote_worker.unwrap() == recv.local_worker));
        });

        let partitions = communication_edges.partition(2, |e| match e {
            Timeline::Local(_) => (0, e),
            Timeline::Remote(_) => (1, e),
        });

        // partitions[0]: communication edges
        // partitions[1]: records

        let communication_edges =
            partitions[0].map(|rec| match rec {
                                  Timeline::Local(edge) => PagOutput::Edge(edge),
                                  _ => panic!("Incorrect data!"),
                              });
        let communication_records =
            partitions[1].map(|rec| match rec {
                                  Timeline::Remote(log_record) => log_record,
                                  _ => panic!("Incorrect data!"),
                              });

        let worker_timeline_input = input.concat(&communication_records);

        // construct worker time line and filter zero time events
        let worker_timelines = worker_timeline_input
            .build_worker_timelines(threshold,
                                    window_size_ns,
                                    insert_waitig_edges,
                                    spark_driver_hack)
            .filter(|pag| if let PagOutput::Edge(ref e) = *pag {
                        e.source.worker_id != e.destination.worker_id ||
                        e.source.timestamp < e.destination.timestamp
                    } else {
                        true
                    });

        // Step 3: merge the contents of both streams and sort according to event time
        let result = worker_timelines.concat(&communication_edges);

        let exchange = Exchange::new(|e: &PagOutput| u64::from(e.destination_worker()));
        let mut timelines_per_epoch = HashMap::new();
        let result = result.unary_notify(exchange, "add traversal info", vec![], move |input, output, notificator| {
            // Organize all data by time and then according to worker ID
            input.for_each(|time, data| {
                let epoch_slot = timelines_per_epoch.entry(*time.time())
                    .or_insert_with(HashMap::new);
                for record in data.drain(..) {
                    epoch_slot.entry(record.destination_worker())
                        .or_insert_with(Vec::new)
                        .push(record);
                }
                notificator.notify_at(time);
            });
            // Sequentially assemble the edges for each worker timeline by pairing up log records
            notificator.for_each(|time, _count, _notify| {
                if let Some(mut timelines) = timelines_per_epoch.remove(time.time()) {
                    let mut session = output.session(&time);
                    for (_worker_id, mut raw_timeline) in timelines.drain() {
                        raw_timeline.sort_by_key(PagOutput::destination_timestamp);
                        let mut last_local_was_waiting = false;
                        for mut record in raw_timeline {
                            if let PagOutput::Edge(ref mut edge) = record {
                                if edge.is_message() // is message?
                                    && delayed_message_threshold > 0 // delayed_message functionality enabled?
                                    && edge.weight() as u64 > delayed_message_threshold // is delayed?
                                    && !last_local_was_waiting // preceding was not a local waiting activity
                                {
                                    // message is delayed and last is not waiting -> do not traverse
                                    edge.traverse = TraversalType::Block;
                                } else if edge.edge_type == ActivityType::Waiting {
                                    // edge is waiting -> do not traverse
                                    edge.traverse = TraversalType::Block;
                                    last_local_was_waiting = true;
                                } else {
                                    // edge is local not waiting -> traverse
                                    last_local_was_waiting = false;
                                    edge.traverse = TraversalType::Unbounded;
                                }
                                debug_assert!(edge.traverse != TraversalType::Undefined,
                                    "Edge must not have undefined traversal type after adding traversal info, edge: {:?}", edge);
                            }
                            session.give(record);
                        }
                    }
                }
            });
        });
        result
    }
}
