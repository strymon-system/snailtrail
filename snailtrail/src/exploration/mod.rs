// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Graph traversal operators for Snailtrail.
//!
//! # Concepts
//!
//! Capacity: An edge's capacity specifies what it can carry given a concrete input defined on abstract values.


extern crate rand;

pub mod betweenness_centrality;
pub mod groupexplore;
pub mod single_path;

pub use self::betweenness_centrality::BetweennessCentrality;
pub use self::single_path::SinglePath;
pub use self::groupexplore::GroupExplore;

/// A trait that describes the capacity of an edge.
///
/// # Paremeters
/// * `E` - The edge type.
/// * `V` - The value type.
pub trait Capacity<E, V> {
    /// Compute the effective capacity of an edge.
    ///
    /// # Parameters
    /// * `edge` - The edge to compute the output capacity for.
    /// * `input` - The input value.
    fn apply_capacity(edge: &E, input: V) -> V;
}

/// A `Capacity` implementation that does not bound an edge's capacity.
pub struct UnboundCapacity;
impl<E, V> Capacity<E, V> for UnboundCapacity {
    fn apply_capacity(_edge: &E, value: V) -> V {
        value
    }
}
