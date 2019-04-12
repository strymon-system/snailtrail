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
use std::fmt::Debug;
use std::rc::Rc;

use timely::dataflow::channels::pact::Exchange;
use timely::dataflow::operators::*;
use timely::dataflow::{Stream, Scope};
use timely::order::Product;
use timely::progress::Timestamp;
use timely::Data;
use timely::ExchangeData;

use crate::exploration::rand::prelude::SliceRandom;
use crate::exploration::rand::thread_rng;
use crate::graph::{Partitioning, SrcDst};


pub trait ExtendedData: Data + Eq + Hash + Copy + Debug {}
impl<T: Data + Eq + Hash + Copy + Debug> ExtendedData for T {}

pub trait SinglePath<G: Scope, N: ExtendedData + Partitioning, D1: SrcDst<N> + Data + Eq + Hash + abomonation::Abomonation>
     {
    /// Traverses a path in a graph starting from a seed node.
    ///
    /// #Examples
    ///
    /// ```
    /// ```

    fn single_path(&self, edge: &Stream<G, D1>) -> Stream<G, D1> where G::Timestamp: Hash + Copy;
}

impl<G: Scope, N: ExtendedData + Partitioning, D1: SrcDst<N> + Data + Eq + Hash + Debug + Send + std::marker::Sync + abomonation::Abomonation> SinglePath<G, N, D1> for Stream<G, D1> {
    fn single_path(&self, edge: &Stream<G, D1>) -> Stream<G, D1>
        where G::Timestamp: Hash+Copy
    {

// The edges in the snapshot
let graph_stream = self; //.concat(&forward_edges); //.concat(&backward_edges);

// Traverse a transient path
        self.scope().scoped("traversal", |inner| {
            let (helper, cycle) = inner.loop_variable(1);

            let seed = edge.enter(inner).concat(&cycle);

            let output = graph_stream.enter(inner).traverse_from(&seed,|e| e.src(),|e| e.dst(),|t| &t.outer);

            output.connect_loop(helper);
            output.leave()
        })

/*
        // Compute single-path summary
        let summary = path.map(|e| (e.tipe().expect("Type not found"),e.weight())).aggregate::<_,Vec<u64>,_,_,_>(
            |_key, val, agg| agg.push(val.expect("Weight not found")),
            |key, agg| { (key, agg.iter().sum()) },
            |key| hash_code(key));
        */
    }
}

pub trait TraverseFrom<G: Scope, D1: ExchangeData, K: Hash + Eq + Copy + Data + Partitioning + 'static>
     {
    /// Explores a graph iteratively based on a frontier stream.
    ///
    /// #Examples
    ///
    /// ```
    /// ```
    fn traverse_from<LG, LJ, TO, TS>(&self,
                                     seed: &Stream<G, D1>,
                                     group: LG,
                                     join: LJ,
                                     outer: TO)
                                     -> Stream<G, D1>
        where G::Timestamp: Hash,
              LG: Fn(&D1) -> Option<K> + 'static,
              LJ: Fn(&D1) -> Option<K> + 'static,
              TO: Fn(&G::Timestamp) -> &TS + 'static,
              TS: Hash + Eq + Copy + Clone + 'static;
}

impl<TOuter: Timestamp,
     G: Scope<Timestamp = Product<TOuter, u32>>,
     D1: ExchangeData,
     K: Hash + Eq + Copy + Data + Partitioning + Debug + 'static> TraverseFrom<G, D1, K>
    for Stream<G, D1> {
    fn traverse_from<LG, LJ, TO, TS>(&self,
                                     seed: &Stream<G, D1>,
                                     group: LG,
                                     join: LJ,
                                     outer: TO)
                                     -> Stream<G, D1>
        where G::Timestamp: Hash,
              LG: Fn(&D1) -> Option<K> + 'static,
              LJ: Fn(&D1) -> Option<K> + 'static,
              TO: Fn(&G::Timestamp) -> &TS + 'static,
              TS: Hash + Eq + Copy + Clone + 'static
    {
        // Local state
        let mut snapshots = HashMap::new();
        let mut seeds = HashMap::new();

        // Not sure why we need to use rc here but it doesn't compile otherwise
        let join = Rc::new(join);
        let group = Rc::new(group);
        let graph_exchange = group.clone();
        let join_exchange = join.clone();
        let exchange1 = Exchange::new(move |e| graph_exchange(e).partition());
        let exchange2 = Exchange::new(move |e| join_exchange(e).partition());
        let mut vector1 = Vec::new();
        let mut vector2 = Vec::new();
        self.binary_notify(seed,
                           exchange1,
                           exchange2,
                           "traverse",
                           Vec::new(),
                           move |input1, input2, output, notificator| {
            // Pull graph edges
            input1.for_each(|cap, data| {
                let key = snapshots.entry(*outer(&cap)).or_insert_with(HashMap::new);
                data.swap(&mut vector1);
                for d in vector1.drain(..) {
                    let slot = key.entry(group(&d)).or_insert_with(Vec::new);
                    slot.push(d);
                }
                let mut time = cap.time().clone();
                time.inner = std::u32::MAX - 1;
                notificator.notify_at(cap.delayed(&time));
                notificator.notify_at(cap.retain().clone());
            });
            // Pull edge seeds
            input2.for_each(|time, data| {
                                let slot = seeds.entry(*outer(&time)).or_insert_with(Vec::new);
                data.swap(&mut vector2);
                                for d in vector2.drain(..) {
                                    slot.push(d);
                                }
                                notificator.notify_at(time.retain());
                            });
            notificator.for_each(|time, _count, _notificator| {
                if time.time().inner < std::u32::MAX - 1 {
                    let mut session = output.session(&time);
                    let t = time.time();
                    if let Some(snapshot) = snapshots.get(outer(t)) {
                        //.expect("No snapshot found.") {
                        if let Some(mut epoch_seeds) = seeds.remove(outer(t)) {
                            // Always assume a single initial seed
                            assert_eq!(epoch_seeds.len(), 1);
                            for seed in epoch_seeds.drain(..) {
                                // Pick a next edge to visit at random
                                if let Some(next_edges) = snapshot.get(&join(&seed)) {
                                    //.expect("No edges found.");
                                    let mut rng = thread_rng();
                                    let next = next_edges[..]
                                        .choose(&mut rng)
                                        .expect("No edges found")
                                        .clone(); //next_edges[0].clone();
                                    session.give(next);
                                }
                            }
                        }
                    }
                } else {
                    // cleanup
                    snapshots.remove(outer(time.time()));
                }
            });
        })
    }
}
