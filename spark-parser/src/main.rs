// Copyright 2017 ETH Zurich. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 <LICENSE-APACHE or
// http://www.apache.org/licenses/LICENSE-2.0> or the MIT license
// <LICENSE-MIT or http://opensource.org/licenses/MIT>, at your
// option. This file may not be copied, modified, or distributed
// except according to those terms.

extern crate env_logger;
extern crate json;
#[macro_use]
extern crate log;
extern crate getopts;
extern crate logformat;

use std::env;
use std::fmt;
use std::fs::File;
use std::io::{self, BufRead, BufReader, BufWriter, Write};
use std::process;

use std::collections::{HashMap, HashSet};

use getopts::{Matches, Options};
use json::JsonValue;
use logformat::{ActivityType, EventType, LogRecord};

/// SnailTrail worker id of the Spark driver.
const DRIVER: u32 = 0;

#[derive(Debug)]
enum Error {
    Io(io::Error),
    Json(json::Error),
    Other(String),
}

impl Error {
    fn other<S: Into<String>>(s: S) -> Self {
        Error::Other(s.into())
    }
}

impl From<io::Error> for Error {
    fn from(io: io::Error) -> Self {
        Error::Io(io)
    }
}

impl From<json::Error> for Error {
    fn from(json: json::Error) -> Self {
        Error::Json(json)
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match *self {
            Error::Io(ref io) => io.fmt(f),
            Error::Json(ref json) => json.fmt(f),
            Error::Other(ref s) => s.fmt(f),
        }
    }
}

trait JsonParse {
    fn parse_u32(&self) -> Result<u32, Error> {
        self.parse_u64().and_then(|val| {
            if val > u32::max_value() as u64 {
                Err(Error::other(format!("integer too large for u32: {}", val)))
            } else {
                Ok(val as u32)
            }
        })
    }

    fn parse_u64(&self) -> Result<u64, Error>;
}

impl JsonParse for JsonValue {
    fn parse_u64(&self) -> Result<u64, Error> {
        if let Some(val) = self.as_u64() {
            Ok(val)
        } else if let Some(s) = self.as_str() {
            s.parse::<u64>()
                .map_err(|err| Error::other(err.to_string()))
        } else {
            Err(Error::other(format!("failed to parse number: {:?}", self)))
        }
    }
}

//type JobId = u32;
type StageId = u32;
type WorkerId = u32;
type ExecutorId = String;

type Nanoseconds = u64;
type Timestamp = u64; // Unix time in nanoseconds

/// We do not have any instrumentation for the driver. Thus, we assume a
/// simplistic model where it is either scheduling some tasks to be run on
/// executors, or it is waiting for executors to finish their assigned tasks.
#[derive(Copy, Clone, Debug)]
enum DriverState {
    GettingResult,
    Scheduling,
    Waiting,
}

/// Models the driver's SnailTrail activities. We unfortunately do not have
/// instrumentation for the driver, and the wait-state heuristic of SnailTrail
/// is subtly different from what we expect to see in Spark.
#[derive(Clone, Debug)]
pub struct Driver {
    /// The timestamp of marks *beginning* of a driver activity. It is
    /// implicitly finished when the next state begins.
    state: Vec<(Timestamp, DriverState, StageId)>,
}

impl Driver {
    fn new() -> Self {
        Driver { state: Vec::new() }
    }
}

/// Metadata about Spark executors. We currently only need it's identifier
/// and the number of worker threads it has.
#[derive(Clone, Debug)]
struct Executor {
    id: ExecutorId,
    cores: usize,
}

impl Executor {
    fn parse(data: &JsonValue) -> Result<Self, Error> {
        let id = data["Executor ID"].to_string();
        let cores = data["Executor Info"]["Total Cores"].parse_u32()? as usize;

        Ok(Executor {
            id: id,
            cores: cores,
        })
    }
}

/// A stage which has been submitted, but we have not seen all tasks yet
#[derive(Clone, Debug)]
struct SubmittedStage {
    /// the globally unique stage id.
    // TODO(swicki): id in the log file is not globally unique if there are
    // multiple jobs in the trace.
    id: StageId,
    /// stages which calculated our RDD dependencies
    parents: Vec<StageId>,
    /// number of tasks we expect to see for this stage
    num_tasks: usize,
}

impl SubmittedStage {
    fn parse(data: &JsonValue) -> Result<Self, Error> {
        debug!("SubmittedStage::parse({})", data);
        let ref stage = data["Stage Info"];
        let id = stage["Stage ID"].parse_u32()?;
        let num_tasks = stage["Number of Tasks"].parse_u32()? as usize;

        let mut parents = Vec::new();
        for parent in stage["Parent IDs"].members() {
            parents.push(parent.parse_u32()?);
        }

        Ok(SubmittedStage {
            id: id,
            num_tasks: num_tasks,
            parents: parents,
        })
    }
}

/// A stage for which we have seen all tasks, but we have not yet assigned
/// a schedule.
#[derive(Clone, Debug)]
struct CompletedStage {
    id: StageId,
    submission_ts: Timestamp,
    completion_ts: Timestamp,
    parents: Vec<StageId>,
    num_tasks: usize,
}

/// A completed stage which has been scheduled. This stage contains the task
/// schedule, as well as the set of all involved SnailTrail worker ids.
#[derive(Clone, Debug)]
struct ProcessedStage {
    id: StageId,
    submission_ts: Timestamp,
    completion_ts: Timestamp,
    parents: Vec<StageId>,
    /// worker which were involved when calculating this stage, used to draw
    /// the shuffle dependencies for each child stage
    workers: HashSet<WorkerId>,
    schedule: Vec<ScheduledTask>,
}

/// The internal representation of a Spark task. This more or less corresponds
/// to `TaskInfo` class, see `INSTRUMENTATION.md` for more details about these
/// fields.
#[derive(Clone, Eq, PartialEq)]
struct Task {
    stage: StageId,
    executor: ExecutorId,
    launch_ts: Timestamp,
    finish_ts: Timestamp,
    getting_result_ts: Option<Timestamp>,
    shuffle_read: Nanoseconds, // duration, overlaps with `executor_run_time`
    shuffle_write: Nanoseconds, // duration, overlaps with `executor_run_time`
    jvm_gc_time: Nanoseconds,  // duration, overlaps with `executor_run_time`
    result_serialize: Nanoseconds, // duration
    task_deserialize: Nanoseconds, // duration
    executor_run_time: Nanoseconds, // duration
}

impl Task {
    /// total duration of the tasks existance
    fn total_duration(&self) -> Nanoseconds {
        self.finish_ts - self.launch_ts
    }

    /// time it took the driver to fetch the results
    fn getting_result_duration(&self) -> Nanoseconds {
        self.getting_result_ts
            .map(|x| self.finish_ts - x)
            .unwrap_or(0)
    }

    /// time it took a executor thread to execute this task
    fn executor_duration(&self) -> Nanoseconds {
        self.result_serialize + self.executor_run_time + self.task_deserialize
    }

    /// task run time on the executor without shuffle read/write
    fn executor_computing_time(&self) -> Nanoseconds {
        self.executor_run_time - self.shuffle_read - self.shuffle_write
    }

    /// time spent in non-instrumented parts
    fn scheduler_delay(&self) -> Nanoseconds {
        self.total_duration() - self.executor_duration() - self.getting_result_duration()
    }
}

impl fmt::Debug for Task {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        f.debug_struct("Task")
            .field("Stage", &self.stage)
            .field("Executor", &self.executor)
            .field("Driver Lauch Time (ms)", &(self.launch_ts / 1_000_000))
            .field("Driver Finish Time (ms)", &(self.finish_ts / 1_000_000))
            .field(
                "Driver Getting Result Time (ms)",
                &(self.getting_result_ts.map(|ts| ts / 1_000_000)),
            )
            .field(
                "Shuffle Read (ms)",
                &(self.shuffle_read as f64 / 1_000_000.0),
            )
            .field(
                "Shuffle Write (ms)",
                &(self.shuffle_write as f64 / 1_000_000.0),
            )
            .field("JVM GC Time (ms)", &(self.jvm_gc_time as f64 / 1_000_000.0))
            .field(
                "Result Serialize (ms)",
                &(self.result_serialize as f64 / 1_000_000.0),
            )
            .field(
                "Task Deserialize (ms)",
                &(self.task_deserialize as f64 / 1_000_000.0),
            )
            .field(
                "Executor Run Time (ms)",
                &(self.executor_run_time as f64 / 1_000_000.0),
            )
            .field(
                "Getting Result Duration (ms)",
                &(self.getting_result_duration() as f64 / 1_000_000.0),
            )
            .field(
                "Scheduler Delay (ms)",
                &(self.scheduler_delay() as f64 / 1_000_000.0),
            )
            .field(
                "Executor Duration (ms)",
                &(self.executor_duration() as f64 / 1_000_000.0),
            )
            .field(
                "Total Duration (ms)",
                &(self.total_duration() as f64 / 1_000_000.0),
            )
            .finish()
    }
}

/// A wrapper around the `Task` struct which contains it's start and end time,
/// as well as the SnailTrail worker id it was executed on.
#[derive(Debug, Clone, Eq, PartialEq)]
struct ScheduledTask {
    /// information about this task known to the driver
    task: Task,
    /// start time *on the executor* thread
    start_time: Timestamp,
    /// end time *on the executor* thread
    end_time: Timestamp,
    /// globally unique id of the worker which executed the task
    worker_id: WorkerId,
}

/// The thread of an executor. Corresponds to a SnailTrail worker and thus
/// contains a globally unique id.
#[derive(Debug, Clone, Eq, PartialEq)]
struct Thread {
    /// globally unique worker id for this thread
    worker_id: WorkerId,
    /// executor local (!) index of this thread (i.e. 0..num_cores)
    index: usize,
    /// timestamp of when this thread becomes free again
    busy_until: Timestamp,
}

impl Thread {
    fn new(worker_id: WorkerId, index: usize) -> Self {
        Thread {
            worker_id: worker_id,
            index: index,
            busy_until: 0,
        }
    }
}

/// Reads in Spark log events and models the state of Spark during exeuction.
///
/// This struct is filled from the log events read in `SparkState::read_json`,
/// executors and stages are added for logged events. The most crucal state
/// reconstruction is the assignment of tasks to executor threads, since this
/// information is not contained in the logs, but has to be inferred under the
/// assumption that an executor thread can only ever execute one task at a time.
/// This post-mortem "task scheduling" is implement in the `schedule_tasks`
/// method.
///
/// The observed stages itself can be in one of three states, first it is
/// `submitted`, then `completed`, and finally `processed`.
///
///   - When a `StageSubmitted` event is observed, it is considered a
///     `SubmittedStage`: This means that we know about the dependencies of the
///     stage, as well as the number of tasks we expect for this stage.
///   - `CompletedStage`. This is a stage for which we have observed the
///     completion event. This implies that we should have seen all the tasks
///     of that stage. However, there might be a running sibling stage, still
///     spawning tasks. Thus we hold off with scheduling until we reach point
///     in the logs where there are no pending tasks (i.e. no stages are
///     in the `submitted` state)
///   - `ProcessedStage`: A `processed` stage is a completed stage with an
///      assigned task schedule, i.e. we know all the workers involved for the
///      computation of the stage.
// TODO(swicki): This currently assumes a single job.
#[derive(Debug)]
struct SparkState {
    /// list of currently available executors
    pub executors: HashMap<ExecutorId, Executor>,
    /// for each executor, a list of threads
    pub threads: HashMap<ExecutorId, Vec<Thread>>,
    /// stages for which we have not seen all tasks yet
    pub submitted: HashMap<StageId, SubmittedStage>,
    /// stages which have been completed, but not yet processed
    pub completed: HashMap<StageId, CompletedStage>,
    /// stages which have been processed (i.e. we know the involved workers)
    pub processed: HashMap<StageId, ProcessedStage>,
    /// stages which were never scheduled
    pub skipped: HashSet<StageId>,
    /// state of the driver (for up until the last task in `processed`)
    pub driver: Driver,

    /// tasks of stages which have been submitted or completed, but not yet processed
    task_queue: Vec<Task>,
    /// globally unique worker id of the most recently created worker
    prev_worker_id: u32,
    /// if set, will be used to create unknown executors on the fly
    default_executor_cores: Option<usize>,
    /// percentage of scheduler delay we attribute to task shipping
    delay_split: f64,
}

impl SparkState {
    /// Initalize an empty, idle Spark cluster. Use `read_json` to populate it.
    ///
    /// See the crate README for more information about the `delay_split`
    /// and `default_cores` arguments. They correspond to the `--delay-split`
    /// and `--executor-cores` command-line arguments.
    fn new(delay_split: f64, default_cores: Option<usize>) -> Self {
        SparkState {
            executors: HashMap::new(),
            submitted: HashMap::new(),
            completed: HashMap::new(),
            processed: HashMap::new(),
            skipped: HashSet::new(),
            threads: HashMap::new(),
            driver: Driver::new(),
            task_queue: Vec::new(),
            prev_worker_id: DRIVER,
            default_executor_cores: default_cores,
            delay_split: delay_split,
        }
    }

    /// generate a worker id unique to this run (driver always has ID 0)
    fn generate_worker_id(&mut self) -> WorkerId {
        self.prev_worker_id += 1;
        self.prev_worker_id
    }

    fn add_executor(&mut self, executor: Executor) {
        assert!(!self.executors.contains_key(&executor.id));
        let id = executor.id.clone();

        // create a thread object for each core (and assign a worker id for the
        // log format to each thread), each of which is idle (busy_until = 0)
        let threads: Vec<Thread> = (0..executor.cores)
            .map(|index| Thread::new(self.generate_worker_id(), index))
            .collect();

        self.threads.insert(id.clone(), threads);
        self.executors.insert(id, executor);
    }

    fn submit_stage(&mut self, stage: SubmittedStage) {
        // TODO(swicki): not sure if this properly deals with re-submitted stages,
        // i.e. stages with attempt > 0

        assert!(!self.submitted.contains_key(&stage.id));
        assert!(!self.skipped.contains(&stage.id));
        // if this stage has parents which have not been completed, we assume
        // that the parent page must have been skipped - a stage cannot be
        // submitted otherwise
        for parent in &stage.parents {
            // we check self.processed instead of self.completed, since any
            // stage that has been completed but not processed must run in
            // parallel to this newly submitted stage, so it cannot be a
            // parent
            if !self.processed.contains_key(parent) {
                self.skipped.insert(*parent);
            }
        }

        // nothing else to do, since we don't know much about the stage until it has been completed
        self.submitted.insert(stage.id, stage);
    }

    fn complete_stage(&mut self, id: StageId, submission_ts: Timestamp, completion_ts: Timestamp) {
        assert!(self.submitted.contains_key(&id));
        assert!(!self.completed.contains_key(&id));
        assert!(!self.skipped.contains(&id));
        assert!(
            submission_ts < completion_ts,
            "invalid stage completion time"
        );

        let stage = self.submitted.remove(&id).unwrap();
        let completed = CompletedStage {
            id: stage.id,
            parents: stage.parents,
            num_tasks: stage.num_tasks,
            submission_ts: submission_ts,
            completion_ts: completion_ts,
        };

        self.completed.insert(stage.id, completed);

        // if there are no other stages currently running, schedule what
        // we have right now
        if self.submitted.is_empty() {
            self.schedule_tasks();
        }
    }

    fn schedule_tasks(&mut self) {
        assert!(self.submitted.is_empty());
        // make sure the number of tasks matches
        assert_eq!(
            // count the number of tasks for each stage
            self.task_queue
                .iter()
                .fold(HashMap::new(), |mut counts, task| {
                    *counts.entry(task.stage).or_insert(0) += 1;
                    counts
                }),
            self.completed
                .iter()
                .map(|(&id, stage)| { (id, stage.num_tasks) })
                .collect(),
            "invalid number of tasks"
        );

        // now we have the schedule the tasks on the available threads to avoid
        // overlapping. We do this by first sorting the task according to their
        // launch time (which is measured at the driver, but is the earliest
        // possible time it could run on the executor) and then find a free
        // thread on the executor the task was running.
        self.task_queue.sort_by_key(|task| task.launch_ts);

        // assert that we do not break the already inferred driver state
        assert!(
            self.task_queue.first().map(|task| task.launch_ts)
                > self.driver.state.last().map(|&(ts, _, _)| ts)
        );

        // for each completed stage, we want to keep track of which workers
        // were involved for the computation
        let mut stages: HashMap<StageId, ProcessedStage> = self
            .completed
            .drain()
            .map(|(id, stage)| {
                let processed = ProcessedStage {
                    id: stage.id,
                    parents: stage.parents,
                    submission_ts: stage.submission_ts,
                    completion_ts: stage.completion_ts,
                    workers: HashSet::new(),
                    schedule: Vec::new(),
                };

                (id, processed)
            })
            .collect();

        // indicates the *end* of certain driver states
        let mut driver_ending_state: Vec<(Timestamp, DriverState, StageId)> = Vec::new();

        // now schedule all these tasks that were executed concurrently on
        // the available threads
        for task in self.task_queue.drain(..) {
            let ref mut stage = stages.get_mut(&task.stage).expect("unknown stage");

            // unfortunately, we do not know how to split the scheduler delay
            // into the time it takes to ship the task to the executor, and the
            // time between the end of the execution and the driver seeing this
            // event. we currently just do a split according to a configurable ratio,
            // i.e. the --delay-split command line argument
            let shipping_delay = (self.delay_split * task.scheduler_delay() as f64) as u64;

            // indicate that the driver was scheduling before launching the task
            let id = stage.id;
            let send_ts = task.launch_ts;
            let done_ts = task.finish_ts;
            driver_ending_state.push((send_ts, DriverState::Scheduling, id));
            if let Some(recv_ts) = task.getting_result_ts {
                driver_ending_state.push((recv_ts, DriverState::Waiting, id));
                driver_ending_state.push((done_ts, DriverState::GettingResult, id));
            } else {
                driver_ending_state.push((done_ts, DriverState::Waiting, id));
            }

            // now calculate the start & end time *on the executor thread*
            let task_duration = task.executor_duration();
            let start_time = task.launch_ts + shipping_delay;
            let end_time = start_time + task_duration;

            info!("scheduling {:#?}", task);

            // get the threads for the executor the task was scheduled on
            let available = self
                .threads
                .get_mut(&task.executor)
                .expect("unknown executor");
            // find the most recently used thread
            let thread = available
                .iter_mut()
                // ignore threads which are still busy
                .filter(|t| t.busy_until < start_time)
                // find the most recently used thread
                .max_by_key(|t| t.busy_until)
                // panic if all threads are busy
                .expect("overlapping tasks, try decreasing the --delay-split");

            info!("on worker {}", thread.worker_id);

            // now mark the thread as busy
            thread.busy_until = end_time;

            // mark the worker for this task
            stage.workers.insert(thread.worker_id);

            let scheduled_task = ScheduledTask {
                task: task,
                start_time: start_time,
                end_time: end_time,
                worker_id: thread.worker_id,
            };

            stage.schedule.push(scheduled_task);
        }

        self.processed.extend(stages);

        // update the driver state based on sends and receives, note that we
        // only know the end of the activity
        driver_ending_state.sort_by_key(|&(ts, _, _)| ts);
        for pair in driver_ending_state.windows(2) {
            let starting_ts = pair[0].0;
            let (_, ending_state, stage) = pair[1];
            // translate the ending state into a starting state
            self.driver.state.push((starting_ts, ending_state, stage));
        }

        // at the end of a processed state, the driver cannot be waiting, as
        // there are no pending tasks running, so we need to open a scheduling
        // state
        if let Some(&(last_ts, _, _)) = driver_ending_state.last() {
            self.driver
                .state
                .push((last_ts, DriverState::Scheduling, 0));
        }

        assert!(self.task_queue.is_empty());
        assert!(self.completed.is_empty());
    }

    fn add_task(&mut self, task: Task) {
        // check if we have to create a new executor for this task
        if !self.executors.contains_key(&task.executor) {
            if let Some(cores) = self.default_executor_cores {
                let executor = Executor {
                    id: task.executor.clone(),
                    cores: cores,
                };
                self.add_executor(executor);
            } else {
                panic!("cannot add task with unknown executor: {}", task.executor);
            }
        }

        self.task_queue.push(task);
    }
}

/// this function extracts the full task description from a
/// "SparkListenerTaskEnd" log object
impl Task {
    fn parse(data: &JsonValue) -> Result<Task, Error> {
        debug!("Task::parse({})", data);
        assert_eq!(data["Event"], "SparkListenerTaskEnd");

        let ref taskinfo = data["Task Info"];
        let ref metrics = data["Task Metrics"];

        let stage = data["Stage ID"].parse_u32()?;
        let executor = taskinfo["Executor ID"].to_string();

        let launch_time = taskinfo["Launch Time"].parse_u64()? * 1_000_000; // ms -> ns
        let finish_time = taskinfo["Finish Time"].parse_u64()? * 1_000_000; // ms -> ns

        let getting_result_time = taskinfo["Getting Result Time"].parse_u64()?;
        let getting_result_time = if getting_result_time > 0 {
            Some(getting_result_time * 1_000_000) // ms -> ns
        } else {
            None
        };

        let ref shuffle_read = metrics["Shuffle Read Metrics"]["Fetch Wait Time"];
        let shuffle_read = if !shuffle_read.is_null() {
            shuffle_read.parse_u64()? * 1_000_000 // ms -> ns
        } else {
            0
        };

        let ref shuffle_write = metrics["Shuffle Write Metrics"]["Shuffle Write Time"];
        let shuffle_write = if !shuffle_write.is_null() {
            shuffle_write.parse_u64()? // already nanoseconds (!)
        } else {
            0
        };

        let jvm_gc_time = metrics["JVM GC Time"].parse_u64()? * 1_000_000; // ms -> ns

        let result_serialize = metrics["Result Serialization Time"].parse_u64()? * 1_000_000; // ms -> ns
        let task_deserialize = metrics["Executor Deserialize Time"].parse_u64()? * 1_000_000; // ms -> ns
        let executor_run_time = metrics["Executor Run Time"].parse_u64()? * 1_000_000; // ms -> ns

        Ok(Task {
            stage: stage,
            executor: executor,
            launch_ts: launch_time,
            finish_ts: finish_time,
            getting_result_ts: getting_result_time,
            shuffle_read: shuffle_read,
            shuffle_write: shuffle_write,
            jvm_gc_time: jvm_gc_time,
            result_serialize: result_serialize,
            task_deserialize: task_deserialize,
            executor_run_time: executor_run_time,
        })
    }
}

#[derive(Debug)]
struct LogWriter<W> {
    writer: W,
    sequence_no: u64,
    activity_cnt: u64,
}

impl<W: Write> LogWriter<W> {
    fn new(writer: W) -> LogWriter<W> {
        LogWriter {
            writer: writer,
            sequence_no: 0,
            activity_cnt: 0,
        }
    }

    fn communication(
        &mut self,
        sender: u32,
        send_ts: Timestamp,
        receiver: u32,
        recv_ts: Timestamp,
        activity: ActivityType,
    ) -> io::Result<()> {
        let tx = LogRecord {
            timestamp: send_ts,
            local_worker: sender,
            activity_type: activity,
            event_type: EventType::Sent,
            correlator_id: Some(self.sequence_no),
            remote_worker: Some(receiver),
            operator_id: None,
        };

        let rx = LogRecord {
            timestamp: recv_ts,
            local_worker: receiver,
            activity_type: activity,
            event_type: EventType::Received,
            correlator_id: Some(self.sequence_no),
            remote_worker: Some(sender),
            operator_id: None,
        };

        self.sequence_no += 1;

        tx.write(&mut self.writer)?;
        rx.write(&mut self.writer)
    }

    pub fn data(
        &mut self,
        sender: u32,
        send_ts: Timestamp,
        receiver: u32,
        recv_ts: Timestamp,
    ) -> io::Result<()> {
        self.communication(
            sender,
            send_ts,
            receiver,
            recv_ts,
            ActivityType::DataMessage,
        )
    }

    pub fn control(
        &mut self,
        sender: u32,
        send_ts: Timestamp,
        receiver: u32,
        recv_ts: Timestamp,
    ) -> io::Result<()> {
        self.communication(
            sender,
            send_ts,
            receiver,
            recv_ts,
            ActivityType::ControlMessage,
        )
    }

    pub fn activity(
        &mut self,
        worker: u32,
        start: Timestamp,
        duration: Nanoseconds,
        activity: ActivityType,
        operator: u32,
    ) -> io::Result<()> {
        let start_event = LogRecord {
            timestamp: start,
            local_worker: worker,
            activity_type: activity,
            event_type: EventType::Start,
            correlator_id: None,
            remote_worker: None,
            operator_id: Some(operator),
        };

        let end_event = LogRecord {
            timestamp: start + duration,
            local_worker: worker,
            activity_type: activity,
            event_type: EventType::End,
            correlator_id: None,
            remote_worker: None,
            operator_id: Some(operator),
        };

        self.activity_cnt += 1;

        start_event.write(&mut self.writer)?;
        end_event.write(&mut self.writer)
    }
}

impl SparkState {
    fn read_json<R: BufRead>(&mut self, reader: R) -> Result<(), Error> {
        // Spark logs are new-line delimnited JSON
        for line in reader.lines() {
            let data = json::parse(&line?)?;
            trace!("processing {}", data);

            // each entry has an Event key
            let ref event = data["Event"]
                .as_str()
                .ok_or_else(|| Error::other("\"Event\" has invalid value"))?;

            match *event {
                "SparkListenerExecutorAdded" => {
                    let executor = Executor::parse(&data)?;
                    self.add_executor(executor);
                }
                "SparkListenerExecutorRemoved" => {
                    error!("currently cannot remove executors");
                }
                "SparkListenerTaskStart" => {
                    /* silently ignored, we have the launch time in end */
                }
                "SparkListenerTaskEnd" => {
                    let task = Task::parse(&data)?;
                    self.add_task(task);
                }
                "SparkListenerStageSubmitted" => {
                    // note: for some reason, not all stage submission events
                    // actually contain the submission time!?
                    let stage = SubmittedStage::parse(&data)?;
                    self.submit_stage(stage);
                }
                "SparkListenerStageCompleted" => {
                    let ref info = data["Stage Info"];
                    let id = info["Stage ID"].parse_u32()?;
                    let submission_ts = info["Submission Time"].parse_u64()? * 1_000_000; // ms -> ns
                    let completion_ts = info["Completion Time"].parse_u64()? * 1_000_000; // ms -> ns

                    self.complete_stage(id, submission_ts, completion_ts);
                }
                ev => warn!("unknown event {:?}", ev),
            }
        }

        Ok(())
    }

    fn write_msgpack<W: Write>(&self, writer: W, draw_shuffle_edges: bool) -> Result<(), Error> {
        let mut logwriter = LogWriter::new(writer);

        // emit worker local activities for driver
        for pair in self.driver.state.windows(2) {
            let (start_ts, state, stage) = pair[0];
            let (end_ts, _, _) = pair[1];

            let duration = end_ts - start_ts;
            let activity = match state {
                DriverState::Waiting => ActivityType::Waiting,
                DriverState::Scheduling => ActivityType::Scheduling,
                DriverState::GettingResult => ActivityType::Buffer,
            };
            logwriter.activity(DRIVER, start_ts, duration, activity, stage)?;
        }

        for stage in self.processed.values() {
            // draw task related activities, i.e. draw a control message from
            // the driver to the thread (indicating task submission), then some
            // computational activity on the thread, and then an control message
            // back to the driver
            let op = stage.id;
            for scheduled in &stage.schedule {
                let task = &scheduled.task;
                let worker = scheduled.worker_id;

                // send task from driver to executor thread
                let send_ts = task.launch_ts;
                let recv_ts = scheduled.start_time;
                logwriter.control(DRIVER, send_ts, worker, recv_ts)?;

                // create executor activities
                let start_deserialize = scheduled.start_time;
                logwriter.activity(
                    worker,
                    start_deserialize,
                    task.task_deserialize,
                    ActivityType::Deserialization,
                    op,
                )?;

                // shuffle read before the computation
                let start_read = start_deserialize + task.task_deserialize;
                if task.shuffle_read > 0 {
                    logwriter.activity(
                        worker,
                        start_read,
                        task.shuffle_read,
                        ActivityType::Buffer,
                        op,
                    )?;
                }

                // we use the task.executor_computing_time() calculation here to execlude shuffling
                let start_computing = start_read + task.shuffle_read;
                logwriter.activity(
                    worker,
                    start_computing,
                    task.executor_computing_time(),
                    ActivityType::Processing,
                    op,
                )?;

                // shuffle write at the end of a task
                let start_write = start_computing + task.executor_computing_time();
                if task.shuffle_write > 0 {
                    logwriter.activity(
                        worker,
                        start_write,
                        task.shuffle_write,
                        ActivityType::Buffer,
                        op,
                    )?;
                }

                let start_serialize = start_write + task.shuffle_write;
                logwriter.activity(
                    worker,
                    start_serialize,
                    task.result_serialize,
                    ActivityType::Serialization,
                    op,
                )?;

                // communication edge for sending back the result
                let send_ts = scheduled.end_time;
                let recv_ts = task.getting_result_ts.unwrap_or(task.finish_ts);
                logwriter.control(worker, send_ts, DRIVER, recv_ts)?;
            }

            if draw_shuffle_edges {
                // then, draw the shuffle messages by drawing an edge between each worker
                // of the parent and child stages
                for parent_id in &stage.parents {
                    // check if the parent stage exists
                    let parent = match self.processed.get(parent_id) {
                        Some(parent) => parent,
                        None => {
                            if self.skipped.contains(&parent_id) {
                                continue;
                            } else {
                                panic!("no parent stage {} found!", parent_id);
                            }
                        }
                    };

                    let send_ts = parent.completion_ts;
                    let recv_ts = stage.submission_ts;
                    for &recv_id in &stage.workers {
                        for &send_id in &parent.workers {
                            logwriter.data(send_id, send_ts, recv_id, recv_ts)?;
                        }
                    }
                }
            }
        }

        info!(
            "wrote {} activities and {} messages",
            logwriter.activity_cnt, logwriter.sequence_no
        );

        Ok(())
    }
}

fn convert(
    reader: &str,
    writer: &str,
    draw_shuffle_edges: bool,
    executor_cores: Option<usize>,
    delay_split: f64,
) -> Result<(), Error> {
    // reader of the Spark logs
    let reader = File::open(reader)?;
    let reader = BufReader::new(reader);

    // Read in the whole Spark log file at once
    // TODO(swicki): We could probably do this for each processed stage and
    // just keep the parent information around, but for now this seems easier.
    let mut spark = SparkState::new(delay_split, executor_cores);
    spark.read_json(reader)?;
    assert!(
        spark.submitted.is_empty(),
        "log contained unprocessed stages"
    );
    assert!(
        spark.completed.is_empty(),
        "log contained unprocessed stages"
    );

    info!("{:#?}", spark);

    // writer of the common logformat
    let writer = File::create(writer)?;
    let writer = BufWriter::new(writer);

    spark.write_msgpack(writer, draw_shuffle_edges)?;

    Ok(())
}

fn print_usage(program: &str, opts: Options) -> ! {
    let brief = format!("Usage: {} [options] INPUT [OUTPUT]", program);
    print!("{}", opts.usage(&brief));
    process::exit(0)
}

fn pop_arg(matches: &mut Matches) -> Option<String> {
    if !matches.free.is_empty() {
        Some(matches.free.remove(0))
    } else {
        None
    }
}

fn main() {
    env_logger::init().expect("failed to initalize logger");

    let args: Vec<String> = env::args().collect();
    let program = args[0].clone();

    let mut opts = Options::new();
    opts.optflag("s", "shuffle", "emit edges for shuffle phases");
    opts.optopt(
        "e",
        "executor-cores",
        "assume number of cores for missing executors",
        "NUM",
    );
    opts.optopt(
        "d",
        "delay-split",
        "percentage of the scheduler delay assigned to task shipping (default: 0.33)",
        "PERCENTAGE",
    );
    opts.optflag("h", "help", "print this help menu");
    let mut matches = match opts.parse(&args[1..]) {
        Ok(m) => m,
        Err(f) => panic!(f.to_string()),
    };

    if matches.opt_present("h") {
        print_usage(&program, opts);
    }

    let shuffle = matches.opt_present("shuffle");
    let executor_cores = matches.opt_str("executor-cores").map(|s| {
        s.parse::<usize>()
            .expect("failed to parse number of executor cores")
    });

    let delay_split = if let Some(split) = matches.opt_str("delay-split") {
        split
            .parse::<f64>()
            .map_err(|e| e.to_string())
            .and_then(|p| {
                if p >= 0.0 && p <= 1.0 {
                    Ok(p)
                } else {
                    Err(format!("value must be between 0.0 and 1.0: {}", p))
                }
            })
            .expect("invalid percentage provided for --delay-split")
    } else {
        0.33f64
    };

    let reader = pop_arg(&mut matches).unwrap_or_else(|| print_usage(&program, opts));
    let writer = pop_arg(&mut matches).unwrap_or_else(|| format!("{}.msgpack", reader));

    convert(&reader, &writer, shuffle, executor_cores, delay_split)
        .expect("failed to convert Spark logs");
}
