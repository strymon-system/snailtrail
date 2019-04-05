// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

//! Helper traits defining graphs.

/// Something that can be partitioned. A partition is represented
/// as a `u64`.
pub trait Partitioning {
    fn partition(&self) -> u64;
}

/// A `SrcDst` represents and edge in a directed graph.
pub trait SrcDst<N: Partitioning> {
    /// The source of the edge. Can be `None` if edge doesn't have a source
    fn src(&self) -> Option<N>;

    /// The destination of the edge. Can be `None` if edge doesn't have a destination.
    fn dst(&self) -> Option<N>;
}

impl<N: Partitioning> Partitioning for Option<N> {
    fn partition(&self) -> u64 {
        self.iter().map(Partitioning::partition).sum()
    }
}

impl Partitioning for u64 {
    fn partition(&self) -> u64 {
        *self
    }
}
