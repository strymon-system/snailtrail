import run_benchmark

class Description(run_benchmark.Description):

    # def create_run(self, args, static_config):
    #     return RunBenchmark(args, static_config)

    def create_plot(self, static_config, args):
        return Experiment(static_config, args)

    def create_custom(self, static_config, args):
        pass

class Experiment(run_benchmark.Plotter):
    ACTIVITIES = [None, "Input", "Buffer", "Scheduling", "Proc",
                  "BarrierProcessing", "Ser", "Deser", "FaultTolerance",
                  "ControlMessage", "DataMessage", "Unknown", "Waiting"]
    # ACTIVITIES = [None, "Input", "Buffer", "Scheduling", "Processing",
    #               "BarrierProcessing", "Serialization", "Deserialization", "FaultTolerance",
    #               "ControlMessage", "DataMessage", "Unknown", "Waiting"]

    def plot_single_workers_window_threshold_epochs(self, config):
        def op_name(x):
            return x
        def act_op_name(x):
            return "{0} {1}".format(self.ACTIVITIES[x[0]], x[1])
        def src_name(x):
            return "Worker {0}".format(x)
        def src_op_name(x):
            return "Worker {0}".format(x)
        def src_act_name(x):
            return "Worker {0} {1}".format(x[0], self.ACTIVITIES[x[1]])

        # group by: operator
        # select: normalized_bc, normalized_weight
        # worker filter: None
        # operator filter: None
        # activity filter: 4, 6, 7 (proc, ser, deser)
        # messages?: False
        # rename: None TODO
        summaries, epochs = self.get_benchmark_data(config)
        d = self.group_summaries(summaries, ['operator'], ['normalized_bc', 'normalized_weight'], None, None, [4, 6, 7,], False, op_name)
        self.plot(d, titles=["CP", "Weight"], sharey=True, kind='cdf')
        self.plot(d, titles=["CP", "Weight"], normalized=[True, False])

        d = self.group_summaries(summaries, ['activity', 'operator'], ['normalized_bc', 'normalized_weight'], None, None, [4, 6, 7,], False, act_op_name)
        self.plot(d, titles=["CP", "Weight"], normalized=False, sharey=False)

        d = self.group_summaries(summaries, ['src', 'activity'], ['normalized_weight'], None, None, [2, 12], False, src_act_name)
        self.plot(d, titles=["Waiting per worker"], normalized=False, kind='line', stacked=False)

        d = self.group_summaries(summaries, ['activity', 'operator'], ['bc'], None, None, [4, 6, 7], False, act_op_name)
        self.plot(d, titles=["Centrality"], normalized=False)

        d = self.group_summaries(summaries, ['src', 'operator'], ['normalized_bc', 'normalized_weight'], None, None, [4, 6, 7], False, op_name, group_mean=['operator'])
        self.plot(d, titles=["CP", "Weight"], normalized=True, kind='line', stacked=False, sharey=True)

        # Group by: /
        # Select: paths
        d = self.group_epoch(epochs, [], ['paths'])
        self.plot(d, titles=["Path"], kind='line', normalized=False, sharey=False)
