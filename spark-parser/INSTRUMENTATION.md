# Spark Log Format

## Task Metrics and Timestamps

All source code locations refer to the Spark 2.1.0 source code.

### Task Launch Time

Unix Timestamp (in milliseconds) of the point in time where the task was created *in the driver*.

  - JSON Path:
    Event == SparkListenerTaskEnd > Task Info > Launch Time
  - Source Code:
    - Storage Attribute:
      - `TaskInfo.launchTime: Long` (Unix Time in Milliseconds)
      - `core/src/main/scala/org/apache/spark/scheduler/TaskInfo.scala:39`
    - Creation: `TaskSetManager.resourceOffer()`
      - `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala:420`

### Task Finish Time

Unix Timestamp (in milliseconds) of the point in time where the task was marked as finished *in the driver*. This includes the time it took to remotely fetch the results.

  - JSON Path:
    Event == SparkListenerTaskEnd > Task Info > Finish Time
  - Source Code:
    - Storage Attribute:
      - `TaskInfo.finishTime: Long` (Unix Time in Milliseconds)
      - `core/src/main/scala/org/apache/spark/scheduler/TaskInfo.scala:63`
    - Creation: `TaskSetManager.handleSuccessfulTask() / handleFailedTask()`
      - `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala:674`

### Task Deserialization Time

Time it took the *executor thread* to deserialize the task code, including the time
spent reading broadcast variables.

- JSON Path:
  Event == SparkListenerTaskEnd > Task Info > Task Metrics > Executor Deserialize Time
- Source Code:
    - Storage Attribute:
      - `TaskMetrics.executorDeserializeTime: Long` (Milliseconds)
      - `core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala:62`
    - Creation: `Executor::TaskRunner::run()`
    - `core/src/main/scala/org/apache/spark/executor/Executor.scala:329`

### Task Execution Time

Time it took the *executor thread* to execute the `task.run` method.

- JSON Path:
  Event == SparkListenerTaskEnd > Task Info > Task Metrics > Executor Deserialize Time
- Source Code:
    - Storage Attribute:
      - `TaskMetrics.executorRunTime: Long` (Milliseconds)
      - `core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala:72`
    - Creation: `Executor::TaskRunner::run()`
    - `core/src/main/scala/org/apache/spark/executor/Executor.scala:329`

### Task Result Serialization Time

Time it took the *executor thread* to serialize the result value returned by the `task.run`
method.

- JSON Path:
  Event == SparkListenerTaskEnd > Task Info > Task Metrics > Result Serialization Time
- Source Code:
    - Storage Attribute:
      - `TaskMetrics.resultSerializationTime: Long` (Milliseconds)
      - `core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala:93`
    - Creation: `Executor::TaskRunner::run()`
    - `core/src/main/scala/org/apache/spark/executor/Executor.scala:338`

### Task GC Time

Accumulated time spent in the garbage collector during the execution of `task.run`.
**Important: This overlaps with the `Task Execution Time`!**

- JSON Path:
  Event == SparkListenerTaskEnd > Task Info > Task Metrics > JVM GC Time
- Source Code:
    - Storage Attribute:
      - `TaskMetrics.jvmGCTime: Long` (Milliseconds)
      - `core/src/main/scala/org/apache/spark/executor/TaskMetrics.scala:93`
    - Creation: `Executor::TaskRunner::run()`
    - `core/src/main/scala/org/apache/spark/executor/Executor.scala:337`


### Task Shuffle Write Time

- JSON Path:
  Event == SparkListenerTaskEnd > Task Info > Task Metrics > Shuffle Write Metrics > Write Wait Time
- Source Code:
    - Storage Attribute:
      - `ShuffleWriteMetrics.writeTime: Long` (**Nanoseconds**)
      - `core/src/main/scala/org/apache/spark/executor/ShuffleWriteMetrics.scala:48`
    - Creation: various locations


### Task Shuffle Read Time

```/**
 * Time the task spent waiting for remote shuffle blocks. This only includes the time
 * blocking on shuffle input data. For instance if block B is being fetched while the task is
 * still not finished processing block A, it is not considered to be blocking on block B.
 */```

According to the documentation in TaskMetrics, this overlaps with the *Task Execution Time*.

- JSON Path:
  Event == SparkListenerTaskEnd > Task Info > Task Metrics > Shuffle Read Metrics > Fetch Wait Time
- Source Code:
    - Storage Attribute:
      - `ShuffleReadMetrics.fetchWaitTime: Long` (Milliseconds)
      - `core/src/main/scala/org/apache/spark/executor/ShuffleReadMetrics.scala:63`
    - Creation: `ShuffleBlockFetcherIterator.next()`
    - `core/src/main/scala/org/apache/spark/storage/ShuffleBlockFetcherIterator.scala:312`

### Task Scheduler Delay

The amount of time spent in a task which is not instrumented (network delays, scheduler overhead etc).

- JSON Path: //Not available, must be calculated//
- Source Code
    - Calculation:
      - `StagePage.getSchedulerDelay(): Long` (Milliseconds)
      - `core/src/main/scala/org/apache/spark/ui/jobs/StagePage.scala:785`

Calculation:

```
totalTime = task.finishTime - task.launchTime 
executorDuration = task.executorDeserializeTime + task.executorRunTime + task.resultSerializationTime
gettingResultTimeDuration = task.finishTime - task.gettingResultTime
schedulerDelay = totalTime - executorDuration - gettingResultTimeDuration
```

### Task Getting Result Time

Timestamp *in the driver* when it starts fetching the result for the task (not all tasks have this). The visualizer shows this as a duration calculated by `finishTime - gettingResultTime` (see `StagePage.getGettingResultTime()`). See also `TaskResultGetter.enqueueSuccessfulTask()` (which is invoked by (`TaskSchedulerImpl.statusUpdate()`).

- JSON Path:
  Event == SparkListenerTaskEnd > Task Info > Getting Result Time
- Source Code:
    - Storage Attribute:
      - `TaskInfo.gettingResultTime: Long` (Unix Timestamp in Milliseconds)
      - `core/src/main/scala/org/apache/spark/scheduler/TaskInfo.scala:48`
    - Creation: `TaskSetManager::handleTaskGettingResult()`
    - `core/src/main/scala/org/apache/spark/scheduler/TaskSetManager.scala:646`

