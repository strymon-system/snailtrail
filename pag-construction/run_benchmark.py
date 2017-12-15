#!/usr/bin/env python3

import argparse
import itertools
import multiprocessing
import io
import os
import re
import socket
import subprocess
import sys
from collections import namedtuple, defaultdict
import importlib
import pandas

try:
    import matplotlib.pyplot as plt
    from matplotlib.backends.backend_pdf import PdfPages
    from matplotlib.ticker import ScalarFormatter
    import matplotlib
    import numpy

    class FixedScalarFormatter(ScalarFormatter):
        def __init__(self, orderOfMagnitude, **kwargs):
            super(FixedScalarFormatter, self).__init__(**kwargs)
            self.orderOfMagnitude = orderOfMagnitude

        def _set_orderOfMagnitude(self, _range):
            """ Set orderOfMagnitude to best describe the specified data range.

            Does nothing except from preventing the parent class to do something.
            """
            pass

    CBcdict = {
        'Bl': (0, 0, 0),
        'Or': (.9, .6, 0),
        'SB': (.35, .7, .9),
        'bG': (0, .6, .5),
        'Ye': (.95, .9, .25),
        'Bu': (0, .45, .7),
        'Ve': (.8, .4, 0),
        'rP': (.8, .6, .7),
    }

    ##Single color gradient maps
    def lighter(colors):
        li = lambda x: x+.5*(1-x)
        return (li(colors[0]), li(colors[1]), li(colors[2]))

    def darker(colors):
        return (.5*colors[0], .5*colors[1], .5*colors[2])

    CBLDcm = {}
    for key in CBcdict:
        CBLDcm[key] = matplotlib.colors.LinearSegmentedColormap.from_list('CMcm' + key, [lighter(CBcdict[key]), darker(CBcdict[key])])


    matplotlib.rcParams.update({'font.size': 24})
    # matplotlib.rc('text', usetex=False)
    ##Two color gradient maps
    CB2cm = {}
    for key in CBcdict:
        for key2 in CBcdict:
            if key != key2:
                CB2cm[key + key2] = matplotlib.colors.LinearSegmentedColormap.from_list('CMcm' + key + key2, [CBcdict[key], CBcdict[key2]])

except Exception as e:
    print(e)
    print('Plotting unavailable')

NS_TO_SEC = 1000000000

# Allows to describe an object
def auto_str(cls):
    def __str__(self):
        return '%s(%s)' % (
            type(self).__name__,
            ', '.join('%s=%s' % item for item in vars(self).items())
        )
    cls.__str__ = __str__
    return cls

def autolabel(rects, ax, f=lambda x: '%d' % int(x)):
    """
    Attach a text label above each bar displaying its height
    """
    for rect in rects:
        height = rect.get_height()
        ax.text(rect.get_x() + rect.get_width()/2., height,
                f(height),
                ha='center', va='bottom', size=7)

class BenchmarkException(Exception):
    pass

class ProcessExecuter(object):

    def execute(self, command, **kwargs):
        print(" ".join(map(str, command)))
        # Filter None parameters
        return subprocess.Popen([str(c) for c in command if c is not None], **kwargs)

@auto_str
class StaticConfiguration(namedtuple('StaticConfigurationBase', ['infiles', 'outdir', 'no_insert_waiting', 'message_delay', 'disable_summary', 'disable_bc', 'name', 'waiting_message'])):
    def __new__(cls, *args, **kwargs):
        return super(StaticConfiguration, cls).__new__(cls, *args, **kwargs)

    def getDirName(self):
        if self.name is None:
            return self.outdir
        else:
            return "/".join([self.outdir, self.name])

ConfigurationBase = namedtuple('ConfigurationBase', ['infile', 'workers', 'window', 'threshold', 'epochs'])

class Configuration(ConfigurationBase):
    def __new__(cls, *args, **kwargs):
        return super(Configuration, cls,).__new__(cls, *args, **kwargs)

    def get_experiment_name(self):
        return os.path.basename(self.infile)

    def getFileName(self, static_config):
        """Get the experiment stdout file name."""
        # TODO: os.path.basename(self.infile)
        t = (static_config.getDirName(), self.get_experiment_name(), self.window, self.threshold, self.workers, self.epochs)
        return "%s/%s/%f_%i_%i_%i.txt" % t

class Benchmark(object):
    def __init__(self, configuration, static_config):
        self.configuration = configuration
        self.static_config = static_config

    def _saveData(self, save_file, data):
        dir_name = os.path.dirname(save_file)
        if not os.path.isdir(dir_name):
            os.makedirs(dir_name)
        with open(save_file, 'wb') as _file:
            _file.write(data)

    def run(self):
        # Do not try to re-run experiment...
        save_file = self.configuration.getFileName(self.static_config)
        if os.path.isfile(save_file):
            print('Not re-running experiment for %s' % (save_file))
        else:
            print('Running experiment for %s' % (save_file))
            command = ['cargo', 'run', '--release', '--bin', 'construct', '--',
                       self.configuration.infile,
                       self.configuration.threshold, self.configuration.window,
                       '-e', self.configuration.epochs - 1,
                       #'--print-dot', '-vv',
                       ]
            if self.static_config.no_insert_waiting:
                command.append('--no-insert-waiting')
            if self.static_config.message_delay is not None:
                command.extend(['--message-delay', self.static_config.message_delay])
            if self.static_config.disable_summary:
                command.append('--no-summary')
            if self.static_config.disable_bc:
                command.append('--no-bc')
            if self.static_config.waiting_message > 0:
                command.append('--waiting-message')
                command.append(self.static_config.waiting_message)
            command.extend(['--', '-w', self.configuration.workers])
            popen = ProcessExecuter().execute(command, stdout=subprocess.PIPE)
            (stdoutdata, _) = popen.communicate()
            if popen.returncode != 0:
                raise BenchmarkException(str(popen.returncode))
            self._saveData(save_file, stdoutdata)

class BenchmarkParser(object):

    def read(self, configuration, static_config):
        save_file = configuration.getFileName(static_config)
        print("Reading {0}".format(save_file))
        with open(save_file, 'rb') as _file:
            stdoutdata = _file
            return self._parseData(stdoutdata, configuration, static_config)

    def _parseData(self, stdoutdata, configuration, static_config):
        # Initialize summaries buffer
        summaries_buffer = io.BytesIO()
        # Initialize epochs data
        epochs = pandas.DataFrame(data={}, index=[], columns=['paths', 'input', 'summary'])
        # Parse input data
        for line in stdoutdata.readlines():
            if line.startswith(b"EPOCH"):
                parts = line.split()
                epoch_number = int(parts[2])
                if parts[1] in [b'input', b'summary']:
                    epochs.at[epoch_number, parts[1].decode('utf-8')] = int(parts[3])
            elif line.startswith(b"# SUMMARY"):
                summaries_buffer.write(line[len(b'# SUMMARY '):])
            elif line.startswith(b"SUMMARY"):
                parts = line.split()
                summaries_buffer.write(parts[1])
                summaries_buffer.write(b"\n")
            elif line.startswith(b"COUNT"):
                parts = line.split()
                epoch_number = int(parts[1])
                if parts[3] in [b'paths']:
                    epochs.at[epoch_number, parts[3].decode('utf-8')] = int(parts[4])

        summaries_buffer.seek(0)
        # print summaries_buffer.getvalue()
        summaries = pandas.read_csv(summaries_buffer, dtype={'epoch': numpy.int64, 'bc':numpy.float64, 'weighted_bc':numpy.float64, 'count':numpy.int64, 'weight':numpy.float64})
        epochs['delta_t'] = epochs['summary'] - epochs['input']
        window_ns = configuration.window * NS_TO_SEC
        # The first epoch
        epoch_offset = summaries['epoch'].min()
        # Align epoch based on 0
        summaries['epoch'] -= epoch_offset
        summaries.set_index(['epoch'], inplace=True)
        # Add a new index column
        epochs = epochs.reset_index()
        def normalize_paths(row, name):
            """ Normalize the weighted centrality or leave at zero if no paths are found """
            paths = epochs.at[row.name, 'paths']
            if paths > 0:
                return row[name] / (paths * window_ns)
            else:
                return 0
        if summaries.shape[0] > 0:
            # Divide weighted_bc by number of path and window size
            summaries['normalized_bc'] = summaries.apply(lambda row: normalize_paths(row, 'weighted_bc'), axis=1)
            # Divide weight by window size
            summaries['normalized_weight'] = summaries['weight'] / window_ns
            summaries['normalized_count'] = summaries['count'] / float(configuration.window)
        # normalize epoch number
        epochs['index'] -= epoch_offset
        # rename index column to epoch
        epochs.rename(columns={'index': 'epoch'}, inplace=True)
        # use epoch column as index
        epochs.set_index(['epoch'], inplace=True)
        return (summaries, epochs)

class BenchmarkAction(object):

    def __init__(self, static_config, args):
        self.static_config = static_config
        self.args = args
        self.workers = list(self.powers_of_two(args.workers_max, args.workers_min))
        self.thresholds = list(self.powers_of(args.threshold_max, args.threshold_min, 10))
        self.epochs_in_flight = list(self.powers_of_two(args.epochs_max, args.epochs_min))
        if args.window_override is not None:
            self.windows = [args.window_override]
        else:
            self.windows = list(self.powers_of(args.window_max, args.window_min))
        self.config_class = Configuration

    def powers_of_two(self, limit, start=1):
        num = start
        while num <= limit:
            yield num
            if num == 0:
                num = 1
            else:
                num = num * 2

    def powers_of(self, limit, start=1, power=2):
        num = start
        while num <= limit:
            yield num
            num = num * power

    def create_config(self, **kwargs):
        return self.config_class(**kwargs)

class RunBenchmark(BenchmarkAction):
    def __init__(self, static_config, cmd_args):
        super(RunBenchmark, self).__init__(static_config, cmd_args)
        self.results = {}

    def doBenchmarks(self):
        for workers in self.workers:
            for window in self.windows:
                for threshold in self.thresholds:
                    for epochs in self.epochs_in_flight:
                        for infile in self.static_config.infiles:
                            config = self.create_config(infile=infile, workers=workers, window=window, threshold=threshold, epochs=epochs)
                            self.run_benchmark(config)

    def run_benchmark(self, config):
        benchmark = Benchmark(config, self.static_config)
        benchmark.run()

class PdfPagesExecution(object):
    def __init__(self, filename, *args, **kwargs):
        self.pdfPages = None
        self.filename = filename
        self.args = args
        self.kwargs = kwargs
    def __enter__(self):
        self.pdfPages = PdfPages(self.filename, *self.args, **self.kwargs)
        return self.pdfPages.__enter__()
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.pdfPages.__exit__(exc_type, exc_val, exc_tb)
        if exc_type is not None:
            os.remove(self.filename)

class DummyExecution(object):
    def __init__(self):
        self.pdfPages = None
    def savefig(self, *args, **kwargs):
        pass
    def __enter__(self, *args, **kwargs):
        return self
    def __exit__(self, exc_type, exc_val, exc_tb):
        pass

class Analysis(BenchmarkAction):

    def __init__(self, staticConfig, args):
        super(Analysis, self).__init__(staticConfig, args)
        self.all_data = {}

    def _outputFileName(self, args, config=None):
        if config is None:
            configName = ''
        else:
            configName = config.get_experiment_name()
        dirName = self.static_config.getDirName() + '/' + configName + "/plot/"
        fileName = dirName + '_'.join(map(str, args[:-1])) + "." + args[-1]
        directory = os.path.dirname(fileName)
        if not os.path.exists(directory):
            os.mkdir(directory)
        if os.path.exists(fileName):
            # Do not update file
            return None
        print("Writing " + fileName)
        return fileName

    def _maybe_open(self, path):
        if path is not None:
            return PdfPagesExecution(path)
        else:
            return DummyExecution()

    def get_benchmark_data(self, config):
        if config in self.all_data:
            return self.all_data[config]
        else:
            data = BenchmarkParser().read(config, self.static_config)
            self.all_data[config] = data
            return data

# https://gist.github.com/notbanker/2be3ed34539c86e22ffdd88fd95ad8bc
class ChainedAssignent:

    """ Context manager to temporarily set pandas chained assignment warning. Usage:

        with ChainedAssignment():
             blah

        with ChainedAssignment('error'):
             run my code and figure out which line causes the error!

    """

    def __init__(self, chained = None):
        acceptable = [ None, 'warn','raise']
        assert chained in acceptable, "chained must be in " + str(acceptable)
        self.swcw = chained

    def __enter__( self ):
        self.saved_swcw = pandas.options.mode.chained_assignment
        pandas.options.mode.chained_assignment = self.swcw
        return self

    def __exit__(self, *args):
        pandas.options.mode.chained_assignment = self.saved_swcw

class Plotter(Analysis):

    MARKERS = ['s', 'o', 'D', '1', 'x', 'd', 'p', 'v', '^', '8', '+']

    def __init__(self, staticConfig, args):
        super(Plotter, self).__init__(staticConfig, args)
        self.pdf = None

    def plot(self, data_columns, titles, sharey='none', normalized=True, kind='area', stacked=True, xlabel=None, ylabel=None, figsize=(7, 5), **kwargs):
        if data_columns is None or len(data_columns) == 0:
            return
        xfmt = FixedScalarFormatter(9)
        xfmt.set_powerlimits((9, 9))

        if isinstance(normalized, list):
            is_normalized = lambda i: normalized[i]
        else:
            is_normalized = lambda _i: normalized

        if isinstance(kind, list):
            get_kind = lambda i: kind[i]
        else:
            get_kind = lambda _i: kind

        if isinstance(stacked, list):
            is_stacked = lambda i: stacked[i]
        else:
            is_stacked = lambda _i: stacked

        # with self._maybe_open(name) as pdf:
        fig, axes = plt.subplots(1, len(data_columns), sharey=sharey, figsize=figsize)
        if len(data_columns) == 1:
            axes = [axes]
        for i, column in enumerate(data_columns):
            # Set plot title
            axes[i].set_title(titles[i])
            # Set xlabel
            if xlabel is not None and xlabel[i] is not None:
                axes[i].set_xlabel(xlabel[i])
            elif column.index.name is not None:
                axes[i].set_xlabel(column.index.name)
            # Set ylabel
            if ylabel is not None and ylabel[i] is not None:
                axes[i].set_ylabel(ylabel[i])
            # Fill NaN values with 0
            # if len(column.shape) > 1:
            #     column.fillna(0, axis=1, inplace=True)
            # If normalized, set ylim
            if is_normalized(i):
                axes[i].set_ylim(bottom=0, top=1.05)
            # Decide how to plot
            k = get_kind(i)
            if callable(k):
                k(axes[i], column, i)
            elif k == "area":
                axes[i].stackplot(column.index, column.values.T, labels=column.columns)
            elif k == "cdf":
                for col in column:
                    ds = column[col].fillna(0)
                    if len(ds) == 0:
                        continue
                    n = numpy.arange(1, len(ds) + 1) / numpy.float(len(ds))
                    ds = numpy.sort(ds)
                    axes[i].step(ds, n, label=col)
                    axes[i].set_xlabel(titles[i])
                    axes[i].set_title('')
            else:
                column.plot(kind=k, ax=axes[i], legend=None, stacked=is_stacked(i), **kwargs)
        lines, labels = plt.gca().get_legend_handles_labels()

        ncolumns = 3
        if len(labels) in [2, 4]:
            ncolumns = 2
        fig.legend(lines, labels, loc='upper center',
            frameon=False,
            bbox_to_anchor=[0.5, -0.15],
            bbox_transform=matplotlib.transforms.BlendedGenericTransform(fig.transFigure, axes[0].transAxes),
            fancybox=False, shadow=False, ncol=ncolumns)
        plt.tight_layout()
        self.pdf.savefig(bbox_inches='tight')
        fig.clf()

    def group_epoch(self, epochs, group, columns, implicit_group=[]):
        if epochs.shape[0] == 0:
            return

        out_columns = ['paths']

        filtered_data = epochs.copy()
        filtered_data.reset_index(inplace=True)
        by_columns = filtered_data.groupby(['epoch'] + implicit_group + group)[out_columns].sum()
        unstacked = by_columns.unstack(level=group + implicit_group)
        return [unstacked[x] for x in columns]

    def group_summaries(self, summaries, group, columns, worker_filter, operator_filter, activity_filter, filter_messages, index_rename, group_mean=None, implicit_group=[], unstack_level=None):
        if summaries.shape[0] == 0:
            return None

        out_columns = ['count', 'bc', 'weight', 'normalized_weight', 'weighted_bc', 'normalized_bc'] + implicit_group

        filtered_data = summaries.copy()
        if worker_filter is not None:
            filtered_data = filtered_data[filtered_data['src'].isin(worker_filter)]
        if operator_filter is not None:
            filtered_data = filtered_data[filtered_data['operator'].isin(operator_filter)]
        if activity_filter is not None:
            filtered_data = filtered_data[filtered_data['activity'].isin(activity_filter)]
        # print(filtered_data.info())
        if filter_messages:
            filtered_data = filtered_data[filtered_data['src'] == filtered_data['dst']]
        filtered_data.reset_index(inplace=True)
        filtered_data.index = filtered_data.index.map(str)
        by_columns = filtered_data.groupby(implicit_group + group + ['epoch']).sum()
        by_columns.sort_index(level=0, sort_remaining=True, inplace=True)

        # Perform optional second grouping
        if group_mean is not None:
            by_columns = by_columns.reset_index().groupby(implicit_group + group_mean + ['epoch']).mean()
            group = group_mean

        if unstack_level is None:
            unstack_level = implicit_group
        unstacked = by_columns.unstack(level=unstack_level + group)
        data_columns = [unstacked[x].copy() for x in columns]
        for dc in data_columns:
            # Convert MultiIndex to index based on tuples
            dc.columns = [c for c in dc.columns]
            if index_rename is not None:
                dc.rename(inplace=True, columns=index_rename)

        return data_columns

    def _plot_single_workers_window_threshold_epochs(self, config):
        # print(config)
        name_elements = ["workers", config.workers, "window", config.window, "threshold", config.threshold]
        name_elements.append("pdf")
        name = self._outputFileName(name_elements, config=config)
        if name is None:
            return
        with PdfPagesExecution(name) as pdf:
            self.pdf = pdf
            self.plot_single_workers_window_threshold_epochs(config)
        plt.close()

    def absolute_to_relative_array(self, vals, norm=1, div=None):
        for i in xrange(len(vals) - 1):
            vals[i] = (vals[i + 1] - vals[i]) / norm
            if div is not None:
                vals[i] /= div[i]
        if len(vals) > 0:
            del vals[-1]

    def ref_to_relative_array(self, ref, vals, norm=1, div=None):
        for i in xrange(len(vals)):
            vals[i] = (vals[i] - ref[i]) / norm
            if div is not None:
                vals[i] /= div[i]


    def _plot_multiple_window_threshold(self, configs):
        raw_data_by_epoch = defaultdict(lambda: defaultdict(list))
        for config in configs:
            data = self.get_benchmark_data(config)
            raw_data_by_epoch[config.epoch][config.worker] = data
        window_ns = window * NS_TO_SEC

        # each epoch: data[worker] -> throughput
        #             data_std[worker] -> error bars

        latency_summary_by_epoch = defaultdict(lambda: defaultdict(list))
        node_count_by_epoch = defaultdict(lambda: defaultdict(list))
        input_times_by_epoch = defaultdict(lambda: defaultdict(list))
        latency_epoch_latency_by_epoch = defaultdict(lambda: defaultdict(list))

        median_count_by_epoch = defaultdict(list)
        median_summary_by_epoch = defaultdict(list)
        median_latency_epoch_latency_by_epoch = defaultdict(list)

        mean_latency_summary_by_epoch = defaultdict(list)

        sum_count_by_epoch = defaultdict(list)
        sum_summary_by_epoch = defaultdict(list)

        throughput_summary_by_epoch = defaultdict(lambda: 0)

        std_count_by_epoch = defaultdict(list)
        std_summary_by_epoch = defaultdict(list)
        std_latency_epoch_latency_by_epoch = defaultdict(list)

        throughput_parallel_by_epoch = {}

        for worker, epoch in itertools.product(workers, epochs_in_flight):
            data = raw_data_by_epoch[epoch][worker]
            input_times = input_times_by_epoch[epoch][worker]
            latency_summary = latency_summary_by_epoch[epoch][worker]
            latency_epoch_latency = latency_epoch_latency_by_epoch[epoch][worker]
            node_count = node_count_by_epoch[epoch][worker]
            for i, data_epoch_key in enumerate(sorted(data.epochs.keys())):
                # if i < epoch or i >= len(data.epochs) - epoch:
                #     continue
                e = data.epochs[data_epoch_key]
                input_times.append(e.latencies["input"])
                latency_summary.append(e.latencies["summary"])
                latency_epoch_latency.append(e.latencies["summary"])
                node_count.append(e.counts["nodes"])

            self.absolute_to_relative_array(latency_summary, norm=NS_TO_SEC*1.)
            self.ref_to_relative_array(input_times, latency_epoch_latency, norm=NS_TO_SEC*1.)

            median_count_by_epoch[epoch].append(numpy.median(node_count))
            median_summary_by_epoch[epoch].append(numpy.median(latency_summary))
            median_latency_epoch_latency_by_epoch[epoch].append(numpy.median(latency_epoch_latency))

            mean_latency_summary_by_epoch[epoch].append(numpy.mean(latency_summary))

            std_count_by_epoch[epoch].append(numpy.std(node_count))
            std_summary_by_epoch[epoch].append(numpy.std(latency_summary))
            std_latency_epoch_latency_by_epoch[epoch].append(numpy.std(latency_epoch_latency))

            sum_count_by_epoch[epoch].append(numpy.sum(node_count))
            sum_summary_by_epoch[epoch].append(numpy.sum(latency_summary))
            if sum_summary_by_epoch[epoch][-1] <= 0:
                print("Skipping configuration because of insufficient data {0} {1}".format(window, threshold))
                return

        for epoch in epochs_in_flight:
            throughput_summary_by_epoch[epoch] = numpy.array(sum_count_by_epoch[epoch]) / sum_summary_by_epoch[epoch]
            throughput_parallel_by_epoch[epoch] = numpy.array(sum_count_by_epoch[epoch]) / sum_summary_by_epoch[epoch]

        throughput_parallel_by_worker = defaultdict(list)
        median_latency_epoch_latency_by_worker = defaultdict(list)

        # throughput_parallel_by_epoch <-> median_latency_epoch_latency_by_epoch
        data_file_name = self._outputFileName(["workers", "all", "window", window, "threshold", threshold, "epochs", "all", "throughput_vs_latency", "txt"])
        if data_file_name is None:
            data_file = None
        else:
            data_file = open(data_file_name, 'wb')
        if data_file is not None:
            print >> data_file, "epoch worker throughput median_latency std_latency median_count std_count"
        for i, worker in enumerate(workers):
            for epoch in epochs_in_flight:
                throughput_parallel_by_worker[worker].append(throughput_parallel_by_epoch[epoch][i])
                median_latency_epoch_latency_by_worker[worker].append(median_latency_epoch_latency_by_epoch[epoch][i])

                if data_file is not None:
                    print  >> data_file, \
                        epoch, worker, throughput_parallel_by_epoch[epoch][i], \
                        median_latency_epoch_latency_by_epoch[epoch][i], std_latency_epoch_latency_by_epoch[epoch][i], \
                        median_count_by_epoch[epoch][i], std_count_by_epoch[epoch][i]
        if data_file is not None:
            data_file.close()

        def worker_label(worker):
            if worker > 1:
                return "{0} workers".format(worker)
            else:
                return "{0} worker".format(worker)

        name = self._outputFileName("workers", "all", "window", window, "threshold", threshold, "epochs", "all", "throughput_vs_latency", "pdf")
        if name is not None:
            with self._maybe_open(name) as pdf:
                fig = plt.figure()
                ax = fig.add_subplot(1, 1, 1)
                # ax.set_title("Throughput")
                # ax.plot([0, max_ts], [0, max_epoch/window], 'r--')
                for i, worker in enumerate(workers):
                    (line, blah, blah) = ax.errorbar(median_latency_epoch_latency_by_worker[worker], throughput_parallel_by_worker[worker],
                        label=worker_label(worker), marker=Plotter.MARKERS[i])
                    xs, ys = line.get_data()
                    # if i == 0:
                    #     for x, y, epoch in zip(xs, ys, epochs_in_flight):
                    #         ax.text(x, y, epoch)
                # ax.set_xticks(numpy.arange(1, len(workers) + 1))
                # ax.set_xticklabels(workers)
                ax.set_xlabel('Latency [s]')
                ax.set_ylabel('Throughput [events/s]')
                ax.set_ylim(bottom=0)
                ax.set_xlim(left=0)
                start, end = ax.get_xlim()
                if end - start > window:
                    ax.xaxis.set_ticks(numpy.arange(start, end, window))
                # ax.set_ylim(top=ylim_top)
                # ax.set_ylim(top=600)
                # ax.xaxis.set_major_formatter(xfmt)
                # ax.grid(True)
                ax.legend()

                pdf.savefig(bbox_inches='tight')
                plt.close()

        name = self._outputFileName(["workers", "all", "window", window, "threshold", threshold, "epochs", "all", "throughput", "pdf"])
        if name is not None:
            with self._maybe_open(name) as pdf:
                fig = plt.figure()
                ax = fig.add_subplot(1, 1, 1)
                ax.set_title("Throughput")
                # ax.plot([0, max_ts], [0, max_epoch/window], 'r--')
                for epoch in epochs_in_flight:
                    x = range(1, 1 + len(throughput_summary_by_epoch[epoch]))
                    ax.errorbar(x, throughput_summary_by_epoch[epoch], label=epoch)
                ax.set_xticks(numpy.arange(1, len(workers) + 1))
                ax.set_xticklabels(workers)
                ax.set_xlabel('Workers')
                ax.set_ylabel('Throughput [records/s]')
                # ax.set_ylim(top=ylim_top)
                ax.set_ylim(bottom=0)
                # ax.set_ylim(top=600)
                # ax.xaxis.set_major_formatter(xfmt)
                ax.legend()

                pdf.savefig(bbox_inches='tight')
                plt.close()

    def plot_summary(self):
        for infile, workers, window, threshold, epochs in itertools.product(self.static_config.infiles, [self.workers[0]], self.windows, self.thresholds, self.epochs_in_flight):
            config = self.create_config(infile=infile, workers=workers, window=window, threshold=threshold, epochs=epochs)
            self._plot_single_workers_window_threshold_epochs(config)

    def plot_throughput(self):
        for infile, window, threshold in itertools.product(self.static_config.infiles, self.windows, self.thresholds):
            configs = []
            for worker, epoch in itertools.product(self.workers, self.epochs_in_flight):
                configs.append(self.create_config(infile=infile, workers=worker, window=window, threshold=threshold, epochs=epoch))
            self._plot_multiple_window_threshold(configs)

    def plot_custom(self):
        pass

class Description(object):

    def create_run(self, static_config, args):
        return RunBenchmark(static_config, args)

    def create_plot(self, static_config, args):
        pass

if __name__ == '__main__':

    def load_module(static_config, args):
        print("Loading %s..." % args.config)
        mod = importlib.import_module(args.config)
        return mod.Description()

    def createStaticConfig(args):
        if args.outdir is None:
            outdir = args.input
        else:
            outdir = args.outdir
        return StaticConfiguration(args.input, outdir, args.no_insert_waiting, args.message_delay, args.disable_summary, args.disable_bc, args.name, args.waiting_message)

    def run(description, args, staticConfig):
        rb = description.create_run(staticConfig, args)
        rb.doBenchmarks()

    def summary(description, args, staticConfig):
        plotter = description.create_plot(staticConfig, args)
        plotter.plot_summary()

    def throughput(descriptionmod, args, staticConfig):
        plotter = description.create_plot(staticConfig, args)
        plotter.plot_throughput()

    def custom(description, args, staticConfig):
        plot = description.create_plot(staticConfig, args)
        with ChainedAssignent('raise'):
            plot.plot_custom()

    parser = argparse.ArgumentParser(description="Benchmark tool")
    choices = {f.__name__ : f for f in [run, summary, throughput, custom]}
    parser.add_argument('action', choices=choices, nargs='+')

    parser.add_argument('--input', nargs='+', help='data input', required=True)
    parser.add_argument('--outdir', help='output directory')
    parser.add_argument('--no-insert-waiting', type=bool, default=False, dest='no_insert_waiting', help='insert waiting edges')
    parser.add_argument('--max-workers', type=int, default=multiprocessing.cpu_count(), dest='workers_max')
    parser.add_argument('--min-workers', type=int, default=1, dest='workers_min')
    parser.add_argument('--max-threshold', type=int, default=1000000000, dest='threshold_max')
    parser.add_argument('--min-threshold', type=int, default=10000000, dest='threshold_min')
    parser.add_argument('--max-window', type=float, default=16, dest='window_max')
    parser.add_argument('--min-window', type=float, default=.5, dest='window_min')
    parser.add_argument('--override-window', type=float, dest='window_override')
    parser.add_argument('--max-epochs', type=int, default=8, dest='epochs_max')
    parser.add_argument('--min-epochs', type=int, default=1, dest='epochs_min')
    parser.add_argument('--message-delay', type=int, default=None, dest='message_delay')
    parser.add_argument('--no-summary', action='store_true', default=False, dest='disable_summary')
    parser.add_argument('--no-bc', action='store_true', default=False, dest='disable_bc')
    parser.add_argument('--config', type=str, default="config_default")
    parser.add_argument('--name', type=str, default='')
    parser.add_argument('--waiting-message', type=int, default=0)

    args = parser.parse_args()
    static_config = createStaticConfig(args)
    description = load_module(static_config, args)
    for action in set(args.action):
        choices[action](description, args, static_config)
