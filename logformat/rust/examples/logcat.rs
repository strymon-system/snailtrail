// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

/// Small binary to decode a program activity log from MsgPack format into text

extern crate logformat;

use std::fs::File;
use std::io::BufReader;

use logformat::LogRecord;

fn main() {
    // Sample input file -> see F17655 on Phabricator or the repository below:
    // `git clone ssh://vcs-user@code.systems.ethz.ch:8006/diffusion/292/flink-instrumentation.git`
    // `cargo run --release --example logcat -- ../../../flink-instrumentation/flink/msgpack_log`
    let path = std::env::args().nth(1).expect("input path required");

    let file = File::open(path).unwrap();
    let mut reader = BufReader::new(file);
    let mut errors = 0;
    while errors < 5 {
        let e = LogRecord::read(&mut reader);
        println!("{:?}", e);
        // Lousy test for EOF. Decoding errors are returned as strings so we can't distinguish the
        // exact reasons apart easily and gracefully terminate once we've reached the last record.
        if let Ok(_) = e {
            errors = 0
        } else {
            errors += 1
        };
    }
}
