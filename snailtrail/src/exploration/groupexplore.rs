// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Graph exploration trait
use std::collections::{HashMap, VecDeque};
use std::hash::Hash;
use std::fmt::Debug;
use std::ops::AddAssign;
use std::rc::Rc;

use timely::Data;
use timely::ExchangeData;
use timely::dataflow::{Stream, Scope};
use timely::dataflow::operators::generic::operator::Operator;
use timely::dataflow::channels::pact::Pipeline;

use crate::graph::Partitioning;
use crate::exploration::Capacity;

/// A trait defining an interface to explore a graph.
pub trait GroupExplore<G: Scope,
                       D1: Data,
                       DO: Data,
                       K: Hash + Eq + Copy + Data + Partitioning + 'static> {
    /// Explores a graph iteratively based on a frontier stream. The graph is exepcted to be
    /// provided by the implementation. Each edge in the input graph is joined with the frontier
    /// using the `group` and `join` functions. The `group` of an edge is an edge's source and the
    /// `join` is its target.
    ///
    /// #Examples
    ///
    /// ```
    /// ```
    fn group_explore<E, LG, LJ>(&self,
                                frontier_stream: &Stream<G, (D1, DO)>,
                                name: &str,
                                group: LG,
                                join: LJ)
                                -> Stream<G, (D1, DO)>
        where G::Timestamp: Hash,
              LG: Fn(&D1) -> Option<K> + 'static,
              LJ: Fn(&D1) -> Option<K> + 'static,
              E: Capacity<D1, DO>;
}

#[derive(Debug)]
struct NodeInfo<D, DO> {
    in_degree: u32,
    in_sum: DO,
    outgoing: Vec<D>,
}

impl<'a, D, DO: Default> NodeInfo<D, DO> {
    fn new() -> Self {
        NodeInfo {
            outgoing: vec![],
            in_degree: 0,
            in_sum: Default::default(),
        }
    }
}

/// A `GroupExplore` implementation for Timely Dataflow streams.
/// This implementation makes the following assumptions:
///
/// * The graph is a DAG, i.e. there are no cycles. (It will detect cycles and assert if one is found)
/// * The graph is partitioned to a single Timely worker. The implementation cannot partition the
/// data, it already needs to be partitioned correctly.
///
/// # Panics
///
/// Asserts if the graph is not a DAG.
///
/// # Implementation
///
/// The graph exploration is implemented as a single-step exploration without using a Timely scope.
/// On input, it stores the graph as a set of nodes. Each node maintains a count of incoming edges
/// and a collection of outgoing edges. When a timestamp is done, it traverses the graph starting
/// with edges on the `frontier_stream`. The edges are stored in a queue to determine which nodes
/// still need to be visited.
///
/// When visiting a node, two things can happen. Firstly, the count of pending in-edges is bigger
/// than one. In this case, the input capacity is accumulated and the pending edges count is
/// decreased. Secondly, the count of pending in-edges is one. In this case, the same as in the
/// previous case is performed followed by outputting an `(edge, capacity)` pair. The capacity
/// is computed using [`Capacity`].
///
/// [`Capacity`]: ../trait.Capacity.html
impl<G, D1, DO, K> GroupExplore<G, D1, DO, K> for Stream<G, D1>
    where G: Scope,
          G::Timestamp: Hash,
          D1: ExchangeData + Data + Debug + Hash + Eq + Send,
          DO: Data + Debug + Default + AddAssign + Copy,
          K: Hash + Eq + Copy + Data + Partitioning + Debug + 'static
{
    fn group_explore<E, LG, LJ>(&self,
                                frontier_stream: &Stream<G, (D1, DO)>,
                                name: &str,
                                group: LG,
                                join: LJ)
                                -> Stream<G, (D1, DO)>
        where LG: Fn(&D1) -> Option<K> + 'static,
              LJ: Fn(&D1) -> Option<K> + 'static,
              E: Capacity<D1, DO>
    {

        let join = Rc::new(join);
        let group = Rc::new(group);

        let mut node_data = HashMap::new();
        let mut frontier_stash = HashMap::new();

        let stream: &Stream<G, D1> = self;

        let mut graph_vector = Vec::new();
        let mut frontier_vector = Vec::new();

        stream.binary_notify(frontier_stream,
                             Pipeline,
                             Pipeline,
                             name,
                             vec![],
                             move |graph, frontier, output, notificator| {
            graph.for_each(|time, data| {
                let t = time.time().clone();
                data.swap(&mut graph_vector);
                let time = time.retain();
                for datum in graph_vector.drain(..) {
                    if let Some(j) = join(&datum) {
                        let node_info = node_data
                            .entry(t.clone())
                            .or_insert_with(HashMap::new)
                            .entry(j)
                            .or_insert_with(NodeInfo::new);
                        node_info.in_degree += 1;
                        // println!("Increasing in_degree: {:?} {:?}", j, node_info);
                    }
                    if let Some(g) = group(&datum) {
                        let node_info = node_data
                            .entry(t.clone())
                            .or_insert_with(HashMap::new)
                            .entry(g)
                            .or_insert_with(NodeInfo::new);
                        node_info.outgoing.push(datum.clone());
                        // println!("Adding outgoing     : {:?} {:?}", g, node_info);
                    }
                    notificator.notify_at(time.clone());
                }
            });

            frontier.for_each(|time, data| {
                data.swap(&mut frontier_vector);
                for datum in frontier_vector.drain(..) {
                    frontier_stash
                        .entry(time.time().clone())
                        .or_insert_with(VecDeque::new)
                        .push_back(datum);
                }
                notificator.notify_at(time.retain().clone());
            });

            notificator.for_each(|time, _, _| {
                // println!("done with time: {:?}", time.time());
                // println!("stash: {:?}", frontier_stash);
                // output edges
                let mut session = output.session(&time);
                let t = time.time();
                let mut frontier_edges = frontier_stash.remove(t).unwrap_or_default();
                assert!(frontier_edges.len() < 100_000_000);
                if let Some(data) = node_data.get_mut(t) {
                    while let Some(datum) = frontier_edges.pop_front() {
                        // let datum: Edge = datum;
                        if let Some(j) = join(&datum.0) {
                            // j is source of edge
                            if let Some(node_info) = data.get_mut(&j) {
                                // data is meta data for j
                                // println!("Joining: {:?}", node_info);
                                assert!(node_info.in_degree > 0,
                                        "node_info: {:?} datum: {:?}",
                                        node_info,
                                        datum);
                                node_info.in_sum += datum.1;
                                node_info.in_degree -= 1;

                                if node_info.in_degree == 0 {
                                    for o in node_info.outgoing.clone() {
                                        let in_sum = <E>::apply_capacity(&o, node_info.in_sum);
                                        session.give((o.clone(), in_sum));
                                        frontier_edges.push_back((o, in_sum));
                                    }
                                }
                            }
                        }

                    }
                }
                // cleanup
                node_data.remove(time.time());
            });
        })
    }
}
