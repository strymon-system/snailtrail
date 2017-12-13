# Chrome Timeline Traces

Tensorflow ships with a piece of code which translates its internal statistics to the [Chrome Trace Format](https://docs.google.com/document/d/1CvAClvFfyA5R-PhYUmn5OOQtYMH4h6I0nSsKchNAySU/preview#heading=h.nb4ul0l9tsgk).

 Note that the mapping creates some artificial information:

 - Flow events between node activities are inserted based on their dependency in the dataflow graph [Source](https://github.com/tensorflow/tensorflow/blob/0054c39243f7bdba159cd530241aa968211d9f83/tensorflow/python/client/timeline.py#L547)
 - Each device is assigned a Process ID [Source](https://github.com/tensorflow/tensorflow/blob/0054c39243f7bdba159cd530241aa968211d9f83/tensorflow/python/client/timeline.py#L469), note that since this is done on a run by run basis, we cannot simply merge the JSON files: Across different files, the PID of a device might chance.
 - Within a device, the thread IDs are also artificially created, such that a thread never
   has overlapping activities. This is very similar to the thread assignment we took for Spark. [Source](https://github.com/tensorflow/tensorflow/blob/0054c39243f7bdba159cd530241aa968211d9f83/tensorflow/python/client/timeline.py#L399)

# TensorFlow [Summaries](https://www.tensorflow.org/versions/r1.1/api_docs/python/tf/summary)

> Summaries provide a way to export condensed information about a model, which is then accessible in tools such as TensorBoard.

They do not seem useful for our purposes, as they do not contain information about the actual execution schedule.

From the [ProtoBuf Definition](https://github.com/tensorflow/tensorflow/blob/9dc6c17797c065796603d9259b2aa57b3c07ff71/tensorflow/core/framework/summary.proto):

> A Summary is a set of named values to be displayed by the
> visualizer.
>
> Summaries are produced regularly during training, as controlled by
> the "summary_interval_secs" attribute of the training operation.
> Summaries are also produced at the end of an evaluation.

> Supported values include "scalar", "histogram", "image", "audio"

# Run Metadata

When calling [`session.run`](https://www.tensorflow.org/versions/r1.1/api_docs/python/tf/Session#run) to run a step, one can request TensorFlow to emit metadata about that run. This is also what is used to create the Chrome Trace Timeline discussed above.

A "run" seems to correspond to a "step", the documentation states:
> This method runs one "step" of TensorFlow computation, by running the necessary graph fragment to execute every Operation and evaluate every Tensor

The RunMetadata object optionally returned by this method contains three fields ([ProtoBuf definition](https://github.com/tensorflow/tensorflow/blob/9dc6c17797c065796603d9259b2aa57b3c07ff71/tensorflow/core/protobuf/config.proto#L280)):

 - `step_stats: StepStats`: Statistics traced for this step. Populated if tracing is turned on.
 - `cost_graph: CostGraphDef`:  The cost graph for the computation defined by the run call.
 - List of `partition_graphs: GraphDef`: Graphs of the partitions executed by executors.

### [StepStats](https://github.com/tensorflow/tensorflow/blob/b10f50ff15944badb7262a207f6628dfa52d6a9d/tensorflow/core/framework/step_stats.proto)

Contains a list of `DeviceStats`, which stores for each device its `name` and a list of [`NodeExecStats`](https://github.com/tensorflow/tensorflow/blob/b10f50ff15944badb7262a207f6628dfa52d6a9d/tensorflow/core/framework/step_stats.proto#L41). 

A `NodeExecStats` are the time/size stats recorded for a single execution of a graph node. It contains strictly more information than the Chrome traces, i.e. also contains timestamps added by the scheduler, e.g. `scheduled_micros`.

### [CostGraphDef](https://github.com/tensorflow/tensorflow/blob/b10f50ff15944badb7262a207f6628dfa52d6a9d/tensorflow/core/framework/cost_graph.proto)

TODO

### [GraphDef](https://github.com/tensorflow/tensorflow/blob/b10f50ff15944badb7262a207f6628dfa52d6a9d/tensorflow/core/framework/graph.proto)

TODO Essentiall a list of NodeDefs.

# [Executor](https://github.com/tensorflow/tensorflow/blob/efe5376f3dec8fcc2bf3299a4ff4df6ad3591c88/tensorflow/core/common_runtime/executor.cc)  - [(Header)](https://github.com/tensorflow/tensorflow/blob/9dc6c17797c065796603d9259b2aa57b3c07ff71/tensorflow/core/common_runtime/executor.h#L50)

The executor assigns the timestamps to the `NodeExecStat`s and stores each completed event in a `StepStatsCollector`

# Useful Links
Posts by Derek Murray explaining: the difference between:
* [[http://stackoverflow.com/questions/41600321/distributed-tensorflow-the-difference-between-in-graph-replication-and-between | In-graph replication and Between-graph replication]]
* [[http://stackoverflow.com/questions/33610685/in-tensorflow-what-is-the-difference-between-session-run-and-tensor-eval/33610914#33610914 | `Session.run()` and `Tensor.eval()`]]
