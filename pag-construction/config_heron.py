import os
import re
import run_benchmark
import pandas
import matplotlib.pyplot as plt
import mpl_toolkits.mplot3d
import seaborn

class Description(run_benchmark.Description):

    def create_run(self, args, static_config):
        run = super(Description, self).create_run(args, static_config)
        run.config_class = DhalionConfig
        return run

    def create_plot(self, static_config, args):
        exp = Experiment(static_config, args)
        exp.config_class = DhalionConfig
        return exp

class DhalionConfig(run_benchmark.Configuration):

    def get_experiment_name(self):
        return os.path.basename(os.path.dirname(self.infile))

class Experiment(run_benchmark.Plotter):
    SNAILTRAIL_MARKER = "SNAILTRAIL_COMPONENTS"
    ACTIVITIES = [None, "Input", "Buffer", "Scheduling", "Proc",
                  "BarrierProcessing", "Ser", "Deser", "FaultTolerance",
                  "ControlMessage", "DataMessage", "Unknown", "Waiting"]
    # ACTIVITIES = [None, "Input", "Buffer", "Scheduling", "Processing",
    #               "BarrierProcessing", "Serialization", "Deserialization", "FaultTolerance",
    #               "ControlMessage", "DataMessage", "Unknown", "Waiting"]
    GROUP_NAMES=['splitters', 'counters']

    def __init__(self, static_config, args):
        super(Experiment, self).__init__(static_config, args)
        self.components = {}
        self.all_file_cache = {}

    def _load_components(self, infile):
        for root, dirs, files in os.walk(os.path.join(os.path.dirname(infile), "log-files")):
            for file in files:
                if file.startswith("container_"):
                    # Read file, look for SNAILTRAIL_COMPONENTS
                    with open(os.path.join(root, file), 'r') as f:
                        for line in f.readlines():
                            idx = line.find(self.SNAILTRAIL_MARKER)
                            if idx > 0:
                                return line[idx:].split()[1:]
        return None

    def get_components(self, config):
        if config.infile not in self.components:
            self.components[config.infile] = self._load_components(config.infile)
        components = self.components[config.infile]
        if components is None:
            raise Exception("Could not load component mapping, aborting")
        return components


    def plot_single_workers_window_threshold_epochs(self, config):
        components = self.get_components(config)
        def op_name(x):
            if x == 255:
                return "Unknown"
            return components[x]
        def act_op_name(x):
            return "{0} {1}".format(self.ACTIVITIES[x[0]], components[x[1]])
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

        message_filter = [9, 10]

        # Compute a worker to operator mapping
        # We assume that each worker only executes a single operator
        worker_to_operator = self.group_summaries(summaries, ['src', 'operator'], ['count'], None, None, [4], True, None)[0]
        worker_to_operator = pandas.DataFrame(list(worker_to_operator.columns.values), columns=['dst', 'operator']).set_index(['dst'])
        # Join summary with `worker_to_operator` to add `operator_dst` column
        summary_dst_operator = summaries.join(worker_to_operator, on='dst', rsuffix='_dst')

        d = self.group_summaries(summaries, ['operator',], ['crosses'], None, None, message_filter, False, op_name)
        self.plot(d, titles=["Rate"], normalized=False, kind='line', stacked=False, sharey=True)

        d = self.group_summaries(summaries, ['src', 'dst', 'operator'], ['count'], None, None, message_filter, False, None)
        d = self.group_summaries(summaries, ['operator'], ['normalized_count'], None, None, message_filter, False, op_name)
        self.plot(d, titles=["Send rate"], normalized=False, kind='line', stacked=False, sharey=True)

        d = self.group_summaries(summary_dst_operator, ['operator_dst'], ['normalized_count'], None, None, message_filter, False, op_name)
        self.plot(d, titles=["Receive rate"], normalized=False, kind='line', stacked=False, sharey=True)
        # Group by: /
        # Select: paths
        d = self.group_epoch(epochs, [], ['paths'])
        self.plot(d, titles=["Path"], kind='line', normalized=False, sharey=False)

    def plot_custom(self):
        for workers in self.workers:
            for window in self.windows:
                for threshold in self.thresholds:
                    self.plot_per_file(workers, window, threshold, self.epochs_in_flight[0])

    def plot_per_file(self, workers, window, threshold, epochs_in_flight):
        self.plot_per_file_all(workers, window, threshold, epochs_in_flight)
        self.plot_per_file_op(workers, window, threshold, epochs_in_flight)

    def get_all_file_data(self, workers, window, threshold, epochs_in_flight):
        cache_key = self.create_config(infile=self.static_config.infiles[0], workers=workers, window=window, threshold=threshold, epochs=epochs_in_flight)
        if cache_key in self.all_file_cache:
            return self.all_file_cache[cache_key]
        all_summaries = []
        all_epochs = []
        keys = []
        i = 0
        for infile in sorted(self.static_config.infiles):
            config = self.create_config(infile=infile, workers=workers, window=window, threshold=threshold, epochs=epochs_in_flight)
            summaries, epochs = self.get_benchmark_data(config)
            key = os.path.basename(os.path.dirname(infile))
            idx = key.find(".msgpack")
            if idx > 0:
                key = key[:idx]
            match = re.search("exp-(\d+)-(\d+)", key)
            key = int(match.group(1)), int(match.group(2))
            keys.append(key)
            all_summaries.append(summaries)
            all_epochs.append(epochs)
            i += 1
            # if i > 5:
            #     break
        summaries = pandas.concat(all_summaries, keys=keys, names=self.GROUP_NAMES)
        epochs = pandas.concat(all_epochs, keys=keys, names=self.GROUP_NAMES)
        self.all_file_cache[cache_key] = summaries, epochs
        return summaries, epochs

    def plot_per_file_all(self, workers, window, threshold, epochs_in_flight):
        name_elements = ["combined", "workers", workers, "window", window, "threshold", threshold]
        name_elements.append("all")
        name_elements.append("pdf")
        name = self._outputFileName(name_elements)
        if name is None:
            return

        config = self.create_config(infile=self.static_config.infiles[0], workers=workers, window=window, threshold=threshold, epochs=epochs_in_flight)
        components = self.get_components(config)
        def name_exp_op(x):
            return "{0}-{1} {2}".format(x[0], x[1], components[x[2]])
        summaries, epochs = self.get_all_file_data(workers, window, threshold, epochs_in_flight)

        def plot_3d(ax, column):
            plt.cla()
            ax = plt.gca(projection='3d')
            d = column.fillna(0)
            print(d)
            ax.scatter(d[d.columns[0]], d[d.columns[1]], d[d.columns[2]])
            ax.set_xlim(0, 1)
            ax.set_ylim(0, 1)
            ax.set_zlim(0, 1)

        def plot_seaborne(ax, column, i):
            with seaborn.plotting_context("paper", font_scale=.8):
                seaborn.heatmap(column, annot=True, fmt=".2f", ax=ax, vmin=0, vmax=1)
                ax.hlines(range(3, column.shape[0], 3), *ax.get_xlim())

        def escape_str(x):
            return "\\_".join(str(x).split("_"))

        with run_benchmark.PdfPagesExecution(name) as pdf:
            self.pdf = pdf
            d = self.group_summaries(summaries, ['operator'], ['normalized_bc'], None, None, [4, 6, 7,], False, name_exp_op, implicit_group=self.GROUP_NAMES)
            self.plot(d, titles=["CP"], sharey=True, kind='cdf')
            # self.plot(d, titles=["CP"], sharey=True, kind='box')

            percentiles = [.5, .95, .99]

            # Group by: /
            # Select: paths
            d = self.group_epoch(epochs, [], ['paths'], implicit_group=self.GROUP_NAMES)
            self.plot(d, titles=["Path"], kind='cdf', normalized=False, sharey=False)
            d = self.group_summaries(summaries, ['operator'], ['normalized_bc'], None, None, [4, 6, 7,], False, None, implicit_group=self.GROUP_NAMES, unstack_level=[])
            # new_index = pandas.MultiIndex.from_product(iterables=d[0].index.levels, names=d[0].index.names)
            d = d[0].fillna(0).reset_index().groupby(self.GROUP_NAMES).quantile(percentiles)
            d = [d[i].unstack(level=['splitters']) for i in range(len(components))]
            self.plot(d, titles=["CP %s" % s for s in components], sharey=True, kind=plot_seaborne, normalized=False, figsize=(12, 5))

            d = self.group_summaries(summaries, ['src', 'operator'], ['normalized_weight'], None, None, [12], False, None, implicit_group=self.GROUP_NAMES, unstack_level=[], group_mean=['operator'])
            columns = d[0].columns
            d = d[0].fillna(0).reset_index().groupby(self.GROUP_NAMES).quantile(percentiles)
            d = [d[i].unstack(level=['splitters']) for i in columns]
            self.plot(d, titles=["Waiting %s" % components[s] for s in columns], sharey=True, kind=plot_seaborne, normalized=False, figsize=(12, 5))

            d = self.group_summaries(summaries, ['src', 'operator'], ['normalized_weight'], None, None, [2], False, None, implicit_group=self.GROUP_NAMES, unstack_level=[], group_mean=['operator'])
            columns = d[0].columns
            new_index = pandas.MultiIndex.from_product(iterables=d[0].index.levels, names=d[0].index.names)
            d = d[0].reindex(new_index).fillna(0).reset_index().groupby(self.GROUP_NAMES).quantile(percentiles)
            print(d)
            d = [d[i].unstack(level=['splitters']) for i in columns]
            self.plot(d, titles=["Buffering %s" % components[s] for s in columns], sharey=True, kind=plot_seaborne, normalized=False, figsize=(12, 5))
        plt.close()


    def plot_per_file_op(self, workers, window, threshold, epochs_in_flight):
        name_elements = ["combined", "workers", workers, "window", window, "threshold", threshold]
        name_elements.append("per_op")
        name_elements.append("pdf")
        name = self._outputFileName(name_elements)
        if name is None:
            return
        config = self.create_config(infile=self.static_config.infiles[0], workers=workers, window=window, threshold=threshold, epochs=epochs_in_flight)
        components = self.get_components(config)
        summaries, epochs = self.get_all_file_data(workers, window, threshold, epochs_in_flight)

        with run_benchmark.PdfPagesExecution(name) as pdf:
            self.pdf = pdf
            for op_filter in [[0], [1], [2]]:
                d = self.group_summaries(summaries, [], ['normalized_bc'], None, op_filter, [4, 6, 7,], False, None, implicit_group=self.GROUP_NAMES)
                self.plot(d, titles=["CP Op %s" % components[op_filter[0]]], sharey=True, kind='cdf')
                self.plot(d, titles=["CP Op %s" % components[op_filter[0]]], sharey=True, kind='box', rot=90, fontsize=8, xlabel=['Experiment'])
                d[0] = d[0].reindex(columns=sorted(d[0].columns, key=lambda x: (x[1], x[0])))
                self.plot(d, titles=["CP Op %s" % components[op_filter[0]]], sharey=True, kind='box', rot=90, fontsize=8, xlabel=['Experiment'])
        plt.close()
