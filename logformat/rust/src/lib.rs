// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

///! Data structure and (de)serialization for a `LogRecord`


extern crate rmp as msgpack;
#[macro_use]
extern crate enum_primitive_derive;
extern crate num_traits;
extern crate abomonation;
#[macro_use]
extern crate abomonation_derive;

use std::io::{Write, Read};
use num_traits::{FromPrimitive, ToPrimitive};

use msgpack::decode::NumValueReadError;

#[derive(Primitive, Abomonation, PartialEq, Debug, Clone, Copy, Hash, Eq, PartialOrd, Ord)]
pub enum ActivityType {
    Input = 1,
    Buffer = 2,
    Scheduling = 3,
    Processing = 4,
    BarrierProcessing = 5,
    Serialization = 6,
    Deserialization = 7,
    FaultTolerance = 8,
    ControlMessage = 9,
    DataMessage = 10,
    // Should not be emitted by instrumentation; inserted during PAG construction.
    Unknown = 11,
    Waiting = 12,
    BusyWaiting = 13,
}

impl ActivityType {
    pub fn is_worker_local(&self) -> bool {
        match *self {
            ActivityType::Input => true,
            ActivityType::Buffer => true,
            ActivityType::Scheduling => true,
            ActivityType::Processing => true,
            ActivityType::BarrierProcessing => true,
            ActivityType::Serialization => true,
            ActivityType::Deserialization => true,
            ActivityType::FaultTolerance => false,
            ActivityType::ControlMessage => false,
            ActivityType::DataMessage => false,
            ActivityType::Unknown => true,
            ActivityType::Waiting => true,
            ActivityType::BusyWaiting => true,
        }
    }
}

#[derive(Primitive, Abomonation, PartialEq, Debug, Hash, Clone, Copy)]
pub enum EventType {
    Start = 1,
    End = 2,
    Sent = 3,
    Received = 4,
    Bogus = 5,
}

#[derive(Debug, Clone)]
pub enum LogReadError {
    Eof,
    DecodeError(String),
}

impl From<String> for LogReadError {
    fn from(msg: String) -> Self {
        LogReadError::DecodeError(msg)
    }
}

impl<'a> From<&'a str> for LogReadError {
    fn from(msg: &'a str) -> Self {
        LogReadError::DecodeError(msg.to_owned())
    }
}

pub type Worker = u32;
pub type Timestamp = u64;
pub type CorrelatorId = u64;
pub type OperatorId = u32;

// ***************************************************************************
// * Please always update tests and java/c++ code after changing the schema. *
// ***************************************************************************
#[derive(Abomonation, PartialEq, Hash, Debug, Clone)]
pub struct LogRecord {
    // Event time in nanoseconds since the Epoch (midnight, January 1, 1970 UTC).
    pub timestamp: Timestamp,
    // Context this event occured in; denotes which of the parallel timelines it belongs to.
    pub local_worker: Worker,
    // Describes the instrumentation point which triggered this event.
    pub activity_type: ActivityType,
    // Identifies which end of an edge this program event belongs to (beginning or end).
    pub event_type: EventType,
    // Opaque label used to group the two records belonging to a program activity.
    pub correlator_id: Option<CorrelatorId>,
    // Similar to `local_worker` but specifies the worker ID for the other end of a sent/received message.
    pub remote_worker: Option<Worker>,
    // Unique id for the operator in the dataflow
    pub operator_id: Option<OperatorId>,
}

impl LogRecord {
    pub fn write<W: Write>(&self, writer: &mut W) -> std::io::Result<()> {
        msgpack::encode::write_uint(writer, self.timestamp)?;
        msgpack::encode::write_uint(writer, u64::from(self.local_worker))?;
        msgpack::encode::write_uint(writer, self.activity_type.to_u64().unwrap())?;
        msgpack::encode::write_uint(writer, self.event_type.to_u64().unwrap())?;
        msgpack::encode::write_sint(writer, self.correlator_id.map(|x| x as i64).unwrap_or(-1))?;
        msgpack::encode::write_sint(writer, self.remote_worker.map(i64::from).unwrap_or(-1))?;
        msgpack::encode::write_sint(writer, self.operator_id.map(i64::from).unwrap_or(-1))?;
        Ok(())
    }

    pub fn read<R: Read>(reader: &mut R) -> Result<LogRecord, LogReadError> {
        // TODO: this method should return the error cause as an enum so that the caller can test
        // whether failures were due to having reached the end of the file (break), due to invalid
        // data (show partially-decoded data), or some other reason.

        let timestamp = {
            let result = msgpack::decode::read_int(reader);
            if let Err(NumValueReadError::InvalidMarkerRead(ref e)) = result {
                if e.kind() == ::std::io::ErrorKind::UnexpectedEof {
                    return Err(LogReadError::Eof);
                }
            }
            result
                .map_err(|read_err| format!("cannot decode timestamp: {:?}", read_err))?
        };
        let local_worker =
            msgpack::decode::read_int(reader)
                .map_err(|read_err| format!("cannot decode local_worker: {:?}", read_err))?;
        let activity_type =
            ActivityType::from_u32(msgpack::decode::read_int(reader)
                                       .map_err(|read_err| format!("{:?}", read_err))?)
                    .ok_or("invalid value for activity_type")?;
        let event_type =
            EventType::from_u32(msgpack::decode::read_int(reader)
                                    .map_err(|read_err| format!("{:?}", read_err))?)
                    .ok_or("invalid value for activity_type")?;
        let correlator_id =
            {
                let val: i64 = msgpack::decode::read_int(reader).map_err(|read_err| format!("cannot decode correlator_id: {:?}", read_err))?;
                if val == -1 { None } else { Some(val as u64) }
            };
        let remote_worker =
            {
                let val: i32 = msgpack::decode::read_int(reader).map_err(|read_err| format!("cannot decode remote_worker: {:?}", read_err))?;
                if val == -1 { None } else { Some(val as u32) }
            };
        let operator_id = {
            let val: i32 =
                msgpack::decode::read_int(reader)
                    .map_err(|read_err| format!("cannot decode operator_id: {:?}", read_err))?;
            if val == -1 { None } else { Some(val as u32) }
        };

        Ok(LogRecord {
               timestamp: timestamp,
               local_worker: local_worker,
               activity_type: activity_type,
               event_type: event_type,
               correlator_id: correlator_id,
               remote_worker: remote_worker,
               operator_id: operator_id,
           })
    }
}

#[test]
fn logrecord_roundtrip() {
    // one record
    let mut v = Vec::with_capacity(2048);
    let r = LogRecord {
        timestamp: 124353023,
        local_worker: 123,
        activity_type: ActivityType::DataMessage,
        event_type: EventType::Start,
        correlator_id: Some(12),
        remote_worker: Some(15),
        operator_id: Some(3),
    };
    r.write(&mut v).unwrap();
    let r_out = LogRecord::read(&mut &v[..]).unwrap();
    assert!(r == r_out);
    let r2 = {
        let mut r2 = r.clone();
        r2.local_worker = 16;
        r2.correlator_id = None;
        r2
    };
    println!("{:?}", r2);
    let r3 = {
        let mut r3 = r.clone();
        r3.event_type = EventType::End;
        r3
    };
    // multiple records
    println!("### {:?}", v);
    r2.write(&mut v).unwrap();
    println!("{:?}", v);
    r3.write(&mut v).unwrap();
    println!("{:?}", v);
    let reader = &mut &v[..];
    assert!(r == LogRecord::read(reader).unwrap());
    println!(">> {:?}", reader);
    let r2_out = LogRecord::read(reader);
    println!("{:?}", r2_out);
    assert!(r2 == r2_out.unwrap());
    println!(">> {:?}", reader);
    assert!(r3 == LogRecord::read(reader).unwrap());
    println!(">> {:?}", reader);
}
