# spark-parser

This crate is a pre-processor for converting traces from Spark's instrumentation
into SnailTrail's messagepack-based program activity graph format.

## Usage

The input of this converter are Spark logging traces in the JSON format.
Instrumentation in Spark has to be enabled by setting the following options in
`conf/spark-defaults.conf`:

    spark.eventLog.enabled  true
    spark.eventLog.dir      file:///tmp/spark-events

The generated JSON files can then be converted into SnailTrail input as follows:

    # produces a `/tmp/spark-events/app-20170324182509-0000.msgpack` file
    cargo run -- /tmp/spark-events/app-20170324182509-0000

Please refer to the Spark documentation for more details about its built-in
tracing: https://spark.apache.org/docs/2.1.0/monitoring.html

## Limitations

#### Splitting the Scheduler Delay

Due to the limited instrumentation of Spark, the pre-processor does not know the
time it takes to ship a task from the driver to the executor, and the time it
takes to send the task result back from the executor to the driver. The sum
of these two durations is known as the "scheduler delay". In order to be able
to construct a PAG, the converter has split the scheduler delay into the
*task shipping delay* and the *task result delay*, and by default assumes
a ratio of 1:2.

However, this assumed default value of this split sometimes results in
impossible traces. This is automatically detected and the the converter will
fail with the following error message:

```
thread 'main' panicked at 'overlapping tasks, try decreasing the --delay-split'
```

In such cases, the assumed task shipping delay of `0.33 * scheduler_delay` is
too high and can be decreased e.g. to 1% by passing `--delay-split 0.01` as a
command line argument.

The size of the ratio does not change SnailTrail's critical participation
metric, however it might not be suitable for other analyses performed on the
PAG.

#### Missing Executor Metadata

By default, the converter assumes that the traces contain metadata about the
executors used to compute the job. This might not be true for very old Spark
traces where this information is missing, and the conversion will fail.
In such cases, the user can supply a default configuration for missing
executors by passing the `--executor-cores N` command-line option.

The convert currently ignores the removal of executors from the cluster.

#### Other Limitations

The current converter only supports reading a single job per log trace, as it
assumes stage identifiers to be unique across the whole trace.
