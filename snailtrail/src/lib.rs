// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate time;
extern crate timely;

#[cfg(test)]
#[macro_use]
extern crate abomonation;

pub mod graph;

pub mod exploration;

use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

/// Hash a value with the default `Hasher` (internal algorithm unspecified).
///
/// The specified value will be hashed with this hasher and then the resulting
/// hash will be returned.
pub fn hash_code<T: Hash>(value: &T) -> u64 {
    let mut hasher = DefaultHasher::new();
    value.hash(&mut hasher);
    hasher.finish()
}

#[cfg(test)]
mod tests {

    use std::sync::{Arc, Mutex};
    use time;

    use crate::graph::{SrcDst, Partitioning};
    use crate::exploration::{UnboundCapacity, BetweennessCentrality};

    use timely;
    use timely::dataflow::operators::*;
    use timely::dataflow::operators::aggregation::Aggregate;
    use timely::dataflow::operators::capture::Extract;

    use abomonation::Abomonation;

    #[derive(Clone, Debug, Eq, PartialEq, Hash, Copy, Ord, PartialOrd)]
    struct Node {
        id: u32,
        worker: u32,
    }

    impl Partitioning for Node {
        fn partition(&self) -> u64 {
            // 31 * (31u64 + 31u64 * self.id as u64) + 31u64 * self.worker as u64
            self.worker as u64
        }
    }

    unsafe_abomonate!(Node: id, worker);

    #[derive(Clone, Debug, Eq, PartialEq, Hash, Copy, Ord, PartialOrd)]
    struct Edge {
        src: Option<Node>,
        dst: Option<Node>,
    }

    impl SrcDst<Node> for Edge {
        #[inline]
        fn src(&self) -> Option<Node> {
            self.src
        }

        #[inline]
        fn dst(&self) -> Option<Node> {
            self.dst
        }
    }

    unsafe_abomonate!(Edge: src, dst);

    #[test]
    fn it_works() {

        let mut edges_result = vec![Edge {
                                        src: Some(Node { id: 1, worker: 0 }),
                                        dst: Some(Node { id: 2, worker: 0 }),
                                    },
                                    Edge {
                                        src: Some(Node { id: 2, worker: 0 }),
                                        dst: Some(Node { id: 3, worker: 1 }),
                                    },
                                    Edge {
                                        src: Some(Node { id: 2, worker: 0 }),
                                        dst: Some(Node { id: 4, worker: 0 }),
                                    },
                                    Edge {
                                        src: Some(Node { id: 3, worker: 1 }),
                                        dst: Some(Node { id: 4, worker: 0 }),
                                    },
                                    Edge {
                                        src: Some(Node { id: 4, worker: 0 }),
                                        dst: Some(Node { id: 5, worker: 0 }),
                                    }];

        let edges = edges_result.clone();

        let s = vec![Edge {
                         src: None,
                         dst: Some(Node { id: 1, worker: 0 }),
                     }];

        let t = vec![Edge {
                         src: Some(Node { id: 5, worker: 0 }),
                         dst: None,
                     }];

        let correct = vec![2, 1, 1, 1, 2];

        let (send, recv) = ::std::sync::mpsc::channel();
        let send = Arc::new(Mutex::new(send));

        ::timely::execute(timely::Configuration::Process(1), move |root| {
            let index = root.index();
            let send = send.lock().unwrap().clone();

            let (mut graph_input, mut edge_input, mut edge_input2) =
                root.dataflow(move |scope| {

                let (graph_input, graph_stream) = scope.new_input();
                let (edge_input, forward_stream) = scope.new_input();
                let (edge_input2, backward_stream) = scope.new_input();

                let result = graph_stream.betweenness_centrality::<UnboundCapacity, _>(&forward_stream, &backward_stream, "comp");
                // let result = result.inspect_batch(move |t, x| println!("[{:?}] {:?} -> {:?}", index, t, x));
                result.capture_into(send);
                let weight = result.map(|(edge, bc)| (edge, bc));
                weight.inspect(|x| println!("weight: {:?}", x));
                let summary = weight.map(|(_, bc)| ((), bc)).aggregate::<u64,_, _, _, _>(|_key, val, agg| *agg += val,
                       |_key, agg| agg,
                       |_key| 0 /* activity type */);
                summary.inspect(|x| println!("summary: {:?}", x));
                (graph_input, edge_input, edge_input2)
            });
            if index == 0 {
                for edge in edges.iter() {
                    graph_input.send(edge.clone());
                }
                for edge in s.iter() {
                    edge_input.send((edge.clone(), 1));
                }
                for edge in t.iter() {
                    edge_input2.send((edge.clone(), 1));
                }
            }
            graph_input.close();
            edge_input.close();
            edge_input2.close();
            let start = time::precise_time_s();
            while root.step() {}
            let end = time::precise_time_s();
            println!("Duration: {:?}", end - start);
        }).unwrap(); // asserts error-free execution;
        let result = recv.extract();
        println!("Output: {:?}", result);
        let result: Vec<(_, _)> = result[0]
            .1
            .clone()
            .into_iter()
            .filter(|&(ref x, bc)| x.src().is_some() && x.dst().is_some() && bc > 0)
            .collect();
        edges_result.sort();
        let expected: Vec<(Edge, u64)> = edges_result
            .into_iter()
            .zip(correct.iter().map(|&x| x))
            .collect();
        assert_eq!(result, expected);
    }
}
