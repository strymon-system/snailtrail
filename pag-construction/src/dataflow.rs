// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

use std;
use std::collections::HashMap;
use std::convert::From as StdFrom;

use rand;

use time;

use abomonation::Abomonation;

use timely;
use timely::dataflow::channels::pact;
use timely::dataflow::operators::{Accumulate, Filter, Input, Inspect, Map, Probe, Binary, Unary};
use timely::dataflow::operators::aggregation::Aggregate;
use timely::dataflow::operators::input::Handle as InputHandle;
use timely::dataflow::operators::probe::Handle as ProbeHandle;
use timely::dataflow::operators::exchange::Exchange;
use timely::dataflow::Scope;
use timely::dataflow::scopes::Root;
use timely::progress::nested::product::Product;
use timely::progress::timestamp::RootTimestamp;
use timely_communication::{Allocate, Allocator, WorkerGuards};

use logformat::{LogRecord, Worker};
use BuildProgramActivityGraph;
use {PagOutput, TraverseNoWaiting};
use output::{DumpPAG, DumpHistogram};
use input;

use snailtrail::exploration::{BetweennessCentrality, SinglePath};
use snailtrail::graph::SrcDst;

use snailtrail::hash_code;

use rand::Rng;

#[derive(Clone)]
pub struct Config {
    pub timely_args: Vec<String>,
    pub log_path: String,
    pub threshold: u64,
    pub window_size_ns: u64,
    pub epochs: u64,
    pub message_delay: Option<u64>,
    pub verbose: u64,
    pub dump_pag: bool,
    pub write_bc_dot: bool,
    pub write_pag_dot: bool,
    pub write_pag_msgpack: bool,
    pub insert_waiting_edges: bool,
    pub disable_summary: bool,
    pub disable_bc: bool,
    pub waiting_message: u64,
}


#[derive(Abomonation, Debug, Clone, Default)]
struct Summary<T: Abomonation> {
    bc: T,
    weighted_bc: T,
    weight: u64,
    count: u64,
}

/// Trait defining `from` similarly to `From` but allowed to lose precision.
trait ImpreciseFrom<T> {
    fn from(T) -> Self;
}

impl<T> ImpreciseFrom<T> for T {
    fn from(value: T) -> T {
        value
    }
}
impl ImpreciseFrom<u64> for f64 {
    fn from(value: u64) -> f64 {
        value as f64
    }
}

/// Force a type to be the same type as the target.
///
/// This is useful when Rust cannot determine the appropriate target type for a computation.
trait SameType {
    fn same_type(&self, typed: Self) -> Self;
}

impl<T> SameType for T {
    fn same_type(&self, typed: Self) -> Self {
        typed
    }
}

impl<T: Abomonation + std::ops::Add<Output = T> + Copy> std::ops::AddAssign for Summary<T> {
    fn add_assign(&mut self, other: Self) {
        *self = Self {
            bc: self.bc + other.bc,
            weighted_bc: self.weighted_bc + other.weighted_bc,
            weight: self.weight + other.weight,
            count: self.count + other.count,
        }
    }
}

/// Key for aggregation. Local indicates a worker-local activity, with its `Worker` ID.
/// Remote indicates a cross-worker activity, with source and destination.
#[derive(PartialEq, Eq, Hash, Abomonation, Clone)]
enum ActivityWorkers {
    Local(Worker),
    Remote(Worker, Worker),
}

struct ProbeWrapper {
    probe: ProbeHandle<Product<RootTimestamp, u64>>,
    name: String,
    current: u64,
}

impl ProbeWrapper {
    pub fn new(name: String, probe: ProbeHandle<Product<RootTimestamp, u64>>) -> Self {
        ProbeWrapper {
            probe,
            name,
            current: 0,
        }
    }

    pub fn print_and_advance(&mut self) {
        while !self.probe.less_than(&RootTimestamp::new(self.current)) {
            println!("EPOCH {} {:?} {:?}",
                     self.name,
                     self.current,
                     time::precise_time_ns());
            // probe is past
            self.current += 1;
        }
    }

    pub fn set_current(&mut self, new_current: u64) {
        self.current = new_current;
    }
}

fn feed_input(mut input: InputHandle<u64, LogRecord>,
              input_records: Vec<LogRecord>,
              mut probes: Vec<ProbeWrapper>,
              computation: &mut Root<Allocator>,
              window_size_ns: u64,
              epochs: u64) {
    let mut last_probe = probes.pop().expect("last probe has to exist");

    let mut old_epoch = 0;
    let mut node_count = 0;
    let mut first = true;
    for rec in input_records {
        // Assign records to slices by rounding timestamps
        let epoch: u64 = rec.timestamp / window_size_ns;
        if first {
            first = false;
            for probe in &mut probes {
                probe.set_current(epoch);
            }
            last_probe.set_current(epoch);
            old_epoch = epoch;
            input.advance_to(epoch - 1);
        }
        // Advance time (must increase monotonically)
        if input.epoch() < &epoch {
            println!("EPOCH input {:?} {:?}", epoch, time::precise_time_ns());

            input.advance_to(epoch);
            let timer = ::std::time::Instant::now();
            // Allow the computation to run until all data has been processed.
            // TODO: This will crash on timestamps < 3
            while last_probe
                      .probe
                      .less_than(&RootTimestamp::new(input.time().inner - epochs)) {
                for probe in &mut probes {
                    probe.print_and_advance();
                }
                last_probe.print_and_advance();
                computation.step();
            }
            println!("Time: {:?}", timer.elapsed());
        }
        if epoch > old_epoch {
            println!("COUNT {:?} {:?} nodes {:?}", old_epoch, 0, node_count);
            node_count = 0;
            old_epoch = epoch;
        }
        input.send(rec);
        node_count += 1;
    }
    while last_probe
              .probe
              .less_than(&RootTimestamp::new(input.time().inner)) {
        for probe in &mut probes {
            probe.print_and_advance();
        }
        last_probe.print_and_advance();
        computation.step();
    }
    for probe in &mut probes {
        probe.print_and_advance();
    }
    last_probe.print_and_advance();
    println!("COUNT {:?} {:?} nodes {:?}", old_epoch, 0, node_count);
}

// Read and decode all log records from a log file and give them as input in a single epoch.  In a
// real computation we'd read input in the background and allow the computation to progress by
// continually making steps.
fn read_and_execute_trace_from_file(log_path: &str,
                                    input: InputHandle<u64, LogRecord>,
                                    probes: Vec<ProbeWrapper>,
                                    computation: &mut Root<Allocator>,
                                    window_size_ns: u64,
                                    epochs: u64,
                                    message_delay: Option<u64>) {
    let input_records = input::read_sorted_trace_from_file_and_cut_messages(log_path,
                                                                            message_delay);
    feed_input(input,
               input_records,
               probes,
               computation,
               window_size_ns,
               epochs);
}


pub fn run_dataflow(config: Config) -> Result<WorkerGuards<()>, String> {
    timely::execute_from_args(config.timely_args.clone().into_iter(), move |computation| {
        let config = config.clone();
        if computation.index() == 0 {
            println!("Input parameters: threshold {}, window size {}ns, verbosity {}, 1+{} epochs",
                     config.threshold,
                     config.window_size_ns,
                     config.verbose,
                     config.epochs);
        }

        let (input, probes) =
            computation.dataflow::<u64, _, _>(|scope| build_dataflow(config.clone(), scope));

        if computation.index() == 0 {
            let names = vec!["pag", "bc", "sp", "summary", "sp_summary"];
            let mut probe_wrappers = Vec::with_capacity(probes.len());
            for (probe, name) in probes.into_iter().zip(names.into_iter()) {
                probe_wrappers.push(ProbeWrapper::new(StdFrom::from(name), probe));
            }
            read_and_execute_trace_from_file(&config.log_path,
                                             input,
                                             probe_wrappers,
                                             computation,
                                             config.window_size_ns,
                                             config.epochs,
                                             config.message_delay);
        }
    })
}

pub fn build_dataflow<'a, A, S>
    (config: Config,
     scope: &mut S)
     -> (InputHandle<u64, LogRecord>, Vec<ProbeHandle<Product<RootTimestamp, u64>>>)
    where A: Allocate,
          S: Scope + Input<'a, A, u64>
{
    let (input, stream) = scope.new_input();
    if false {
        stream.dump_histogram();
    }
    let pag_output = stream.build_program_activity_graph(config.threshold,
                                                         config.waiting_message,
                                                         config.window_size_ns,
                                                         config.insert_waiting_edges);

    let probe_pag = pag_output.filter(|_| false).exchange(|_| 0).probe();
    // Dump all program activities to the console for debugging
    if config.dump_pag {
        pag_output
            .exchange(|_| 0)
            .inspect_batch(|time, data| {
                               println!("[EPOCH {}]", time.inner);
                               for datum in data {
                                   println!("  {:?}", datum);
                               }
                           });
    }

    // Crete a DOT file of the graph for each epoch?
    if config.write_pag_dot {
        pag_output.dump_graph("dot/pag");
    }

    if config.write_pag_msgpack {
        pag_output.dump_msgpack("msgpack_pag/output");
    }

    let index = scope.index();
    pag_output
        .exchange(|_| 0)
        .count()
        .inspect_batch(move |ts, c| {
            c.first()
                .map(|c| println!("COUNT {:?} {:?} pag_output {:?}", ts.inner, index, c));
        });
    if config.verbose > 1 {
        pag_output.inspect_batch(move |ts, cs| for c in cs {
                                     println!("CONTENT {:?} {:?} pag_output {:?}",
                                              ts.inner,
                                              index,
                                              c)
                                 });
    }

    if config.disable_bc {
        return (input, vec![probe_pag]);
    }

    let forward = pag_output.filter(|output| match *output {
                                        PagOutput::StartNode(_) => true,
                                        _ => false,
                                    });

    forward
        .exchange(|_| 0)
        .count()
        .inspect_batch(move |ts, c| {
                           c.first()
                               .map(|c| {
                                        println!("COUNT {:?} {:?} forward {:?}", ts.inner, index, c)
                                    });
                       });
    if config.verbose > 1 {
        forward.inspect_batch(move |ts, cs| for c in cs {
                                  println!("CONTENT {:?} {:?} forward {:?}", ts.inner, index, c)
                              });
    }

    let backward = pag_output.filter(|output| match *output {
                                         PagOutput::EndNode(_) => true,
                                         _ => false,
                                     });

    if config.verbose > 0 {
        backward
            .count()
            .inspect_batch(move |ts, c| {
                c.first()
                    .map(|c| println!("COUNT {:?} {:?} backward {:?}", ts.inner, index, c));
            });
        if config.verbose > 1 {
            backward.inspect_batch(move |ts, cs| for c in cs {
                                       println!("CONTENT {:?} {:?} backward {:?}",
                                                ts.inner,
                                                index,
                                                c)
                                   });
        }
    }

    // We do not want to traverse Waiting edges so remove them from the PAG
    let graph = pag_output.filter(|rec| match *rec {
                                      PagOutput::Edge(_) => true,
                                      _ => false,
                                  });

    if config.verbose > 0 {
        graph
            .count()
            .inspect_batch(move |ts, c| {
                c.first()
                    .map(|c| println!("COUNT {:?} {:?} graph {:?}", ts.inner, index, c));
            });
        if config.verbose > 1 {
            graph.inspect_batch(move |ts, cs| for c in cs {
                                    println!("CONTENT {:?} {:?} graph {:?}", ts.inner, index, c)
                                });
        }
    }

    let forward_count = forward.map(|e| (e, From::from(1u8)));
    let backward_count = backward.map(|e| (e, From::from(1u8)));

    // Perform edge ranking by counting all distinct paths within each PAG slice
    let bc =
        graph.betweenness_centrality::<TraverseNoWaiting, f64>(&forward_count,
                                                               &backward_count,
                                                               "bc");

    // Crete a DOT file of the graph for each epoch?
    if config.write_bc_dot {
        bc.map(|(e, _)| e).dump_graph("dot/bc");
    }

    let probe_bc_stream = bc.filter(|_| false).exchange(|_| 0);
    let probe_bc = probe_bc_stream.probe();

    if config.disable_summary {
        return (input, vec![probe_pag, probe_bc]);
    }

    bc.exchange(|_| 0)
        .count()
        .inspect_batch(move |ts, c| {
                           c.first()
                               .map(|c| {
                                        println!("COUNT {:?} {:?} bc {:?}", ts.inner, index, c)
                                    });
                       });
    if config.verbose > 1 {
        bc.inspect_batch(move |ts, cs| for c in cs {
                             println!("CONTENT {:?} {:?} bc {:?}", ts.inner, index, c)
                         });
    }

    // Pick a random seed
    let mut accums = HashMap::new();
    let seed_edge = forward.unary_notify(pact::Exchange::new(|_| 0),
                                         "SeedEdge",
                                         vec![],
                                         move |input, output, notificator| {
        input.for_each(|time, data| {
                           accums
                               .entry(*time.time())
                               .or_insert_with(Vec::new)
                               .extend_from_slice(data);
                           notificator.notify_at(time);
                       });

        notificator.for_each(|time, _count, _notify| {
            if let Some(accum) = accums.remove(time.time()) {
                // The output stream will contain either zero or one element.  In the common
                // case, we pick a single random edge per epoch and emit it, however, some
                // epochs are empty and we cannot randonly sample.
                if let Some(elem) = rand::thread_rng().choose(&accum[..]) {
                    output.session(&time).give(elem.clone());
                }
            }
        });
    });

    // Single-path bc
    let sp = graph.single_path(&seed_edge); //.inspect_ts(move |ts, c| println!("{:?} {:?} Edge: {:?}", ts.inner, index, c));

    let probe_sp_stream = sp.filter(|_| false).exchange(|_| 0);
    let probe_sp = probe_sp_stream.probe();

    let mut bc_map = HashMap::new();
    let mut forward_map = HashMap::new();
    let count = bc.binary_notify(&forward,
                                 pact::Exchange::new(|_| 0),
                                 pact::Exchange::new(|_| 0),
                                 "count",
                                 Vec::new(),
                                 move |input1, input2, output, notificator| {
        input1.for_each(|time, data| {
            let bc_entry = bc_map.entry(*time.time()).or_insert_with(HashMap::new);
            for (d, count) in data.drain(..) {
                *bc_entry
                     .entry(d.src().expect("edge w/o src"))
                     .or_insert(0u64) += count as u64;
            }
            notificator.notify_at(time);
        });
        input2.for_each(|time, data| {
                            forward_map
                                .entry(*time.time())
                                .or_insert_with(Vec::new)
                                .extend(data.drain(..));
                            notificator.notify_at(time);
                        });
        notificator.for_each(|time, _count, _notificator| {
            let mut sum = 0u64;
            if let Some(forward_edges) = forward_map.remove(time.time()) {
                if let Some(bc_entry) = bc_map.get(time.time()) {
                    for edge in &forward_edges {
                        if let Some(bc) =
                            bc_entry.get(&edge.dst().expect("forward without dst found")) {
                            sum += sum.same_type(*bc);
                        }
                    }
                }
            }
            bc_map.remove(time.time());
            forward_map.remove(time.time());
            output.session(&time).give(sum);
        });
    });

    count.inspect_batch(move |ts, c| {
                            c.first()
                                .map(|c| {
                                         println!("COUNT {:?} {:?} paths {:?}", ts.inner, index, c)
                                     });
                        });

    // group aggregates by (activity_type, operator_id, worker_id)
    let probe_summary = {
        let edge_weight_stream_triples = bc.unary_stream(pact::Pipeline,
                                                         "MapToSummary",
                                                         move |input, output| {
            input.for_each(|time, data| {
                output
                    .session(&time)
                    .give_iterator(data.drain(..)
                                       .map(|(edge, bc)| {
                        let w = edge.weight();
                        let window_size_ns = config.window_size_ns;
                        let window_start_time = time.time().inner;
                        let crosses_start = edge.source_timestamp() == window_start_time * window_size_ns - 1;
                        let crosses_end = edge.destination_timestamp() ==
                            window_start_time * window_size_ns + window_size_ns;
                        let crosses = match (crosses_start, crosses_end) {
                            (true, true) => 'B',
                            (true, false) => 'S',
                            (false, true) => 'E',
                            (false, false) => 'N',
                        };
                        let edge_type = match edge {
                            PagOutput::Edge(ref e) => {
                                (e.edge_type as u8,
                                 e.operator_id.unwrap_or(std::u8::MAX as u32) as u8,
                                 if e.edge_type.is_worker_local() {
                                     ActivityWorkers::Local(e.source.worker_id)
                                 } else {
                                     ActivityWorkers::Remote(e.source.worker_id,
                                                             e.destination.worker_id)
                                 },
                                 crosses)
                            }
                            et => panic!("Unknown input: {:?}", et),
                        };
                        let summary = Summary {
                            weight: w,
                            bc: bc,
                            weighted_bc: bc * bc.same_type(ImpreciseFrom::from(w)),
                            count: 1,
                        };
                        (edge_type, summary)
                    }));
            });
        });
        let summary_triples = edge_weight_stream_triples
            .aggregate::<_, Summary<_>, _, _, _>(|_key, val, agg| *agg += val,
                                                 |key, agg| (key, agg),
                                                 |key| hash_code(key));

        if index == 0 {
            println!("# SUMMARY epoch,activity,operator,src,dst,crosses,bc,weighted_bc,count,weight",);
        }
        summary_triples
            .exchange(|_| 0)
            .inspect_batch(move |ts, output| for &((activity_type, operator_id, ref workers, crosses),
                                                   ref summary) in output {
                               let worker_csv = match *workers {
                                   ActivityWorkers::Local(w_id) => format!("{},{}", w_id, w_id),
                                   ActivityWorkers::Remote(src, dst) => format!("{},{}", src, dst),
                               };
                               let data = format!("{},{},{},{},{},{},{},{},{}",
                                                  ts.inner,
                                                  activity_type,
                                                  operator_id,
                                                  worker_csv,
                                                  crosses,
                                                  summary.bc,
                                                  summary.weighted_bc,
                                                  summary.count,
                                                  summary.weight);

                               println!("SUMMARY {}", data.to_string());
                           })
            .probe()
    };

    // Generate random single-path summaries
    let e_weight = sp.map(|edge| {
        let w = edge.weight();
        let edge_type = match edge {
            PagOutput::Edge(ref e) => (e.edge_type as u8, e.operator_id.unwrap_or(255) as u8),
            et => panic!("Unknown input: {:?}", et),
        };
        (edge_type,
         Summary {
             weight: w,
             bc: From::from(1u8),
             weighted_bc: w,
             count: 1,
         })
    });
    let sp_summary =
        e_weight.aggregate::<_, Summary<_>, _, _, _>(|_key, val, agg| *agg += val,
                                                     |key, agg| (key, agg),
                                                     |key| hash_code(key));

    sp_summary.inspect_batch(move |ts, output| for &(t, ref summary) in output {
                                 println!("SP_SUMMARY {:?} {:?} {} {} {} {} {} {}",
                                          ts.inner,
                                          index,
                                          t.0,
                                          t.1,
                                          summary.bc,
                                          summary.weighted_bc,
                                          summary.count,
                                          summary.weight)
                             });

    let probe_sp_summary = sp_summary.probe();

    (input,
     vec![probe_pag,
          probe_bc,
          probe_sp,
          probe_summary,
          probe_sp_summary])
}
