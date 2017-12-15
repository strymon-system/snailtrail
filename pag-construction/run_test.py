#!/usr/bin/env python3

import argparse
import inspect
import sys
import os
import subprocess
import traceback
import pprint
from collections import namedtuple, defaultdict

ACTIVITIES = {
     1: "Input",
     2: "Buffer",
     3: "Scheduling",
     4: "Processing",
     5: "BarrierProcessing",
     6: "Serialization",
     7: "Deserialization",
     8: "FaultTolerance",
     9: "ControlMessage",
    10: "DataMessage",
    11: "Unknown",
    12: "Waiting",
    13: "Bogus",
}

OUTDIR = "test/"

class Execution(object):
    def __init__(self, infile, threshold, window_size, workers=1, epochs=0, mode=None, binary="construct", outdir="."):
        self.infile = infile
        self.threshold = threshold
        self.window_size = window_size
        self.workers = workers
        self.epochs = epochs
        self.mode = mode
        self.binary = binary
        self.outdir = outdir

    def filename(self):
        return "{ex.outdir}/{ex.infile}_{ex.binary}/{ex.binary}_{ex.window_size:f}_{ex.threshold}_{ex.workers}_{ex.epochs}.txt".format(ex=self)

    def execute(self):
        args_list = ["cargo", "run", "--bin", self.binary, "--"] # TODO add mode
        args_list.extend([
            "--epochs", str(self.epochs),
            str(self.infile),
            str(self.threshold),
            str(self.window_size),
            "--", "-w", str(self.workers)
        ])

        outfile = self.filename()
        outdir = os.path.dirname(outfile)
        if not os.path.isdir(outdir):
            os.makedirs(outdir)

        with open(outfile, 'w') as out:
            subprocess.check_call(args_list, stdout=out)

    # epoch -> (activity, operator) -> bc
    def parse(self):
        data = {}
        with open(self.filename(), 'r') as txt:
            for line in txt:
                if line.startswith("SUMMARY"):
                    # epoch,activity,operator,src,dst,crosses,bc,weighted_bc,count,weight
                    epoch, activity, operator, worker, dst, crosses, bc, weighted_bc, count, weight = line.split(' ')[1].split(',')[0:10]
                    epoch, activity, operator, worker, dst, bc, weighted_bc, count, weight = \
                        map(int, [epoch, activity, operator, worker, dst, bc, weighted_bc, count, weight])
                    epoch_data = data.setdefault(epoch, defaultdict(lambda: 0))
                    epoch_data[(ACTIVITIES[activity], operator)] += bc

        return data

class Pyramid(object):
    def __init__(self, outfile, **kwargs):
        self.outfile = outfile
        self.shape = kwargs

    def generate(self):
        args_list = ["cargo", "run", "--bin", "test-gen", "--"]
        for key, value in self.shape.iteritems():
            args_list.extend(["--{}".format(key), str(value)])
        args_list.append(self.outfile)
        subprocess.check_call(args_list)

def generate(name, pyramid, window_size, threshold=1000):
    if not os.path.isdir(args.outdir):
        os.makedirs(args.outdir)
    name = os.path.join(args.outdir, name) + ".msgpack"

    p = Pyramid(name, **pyramid)
    p.generate()
    ex = Execution(name, threshold, window_size)
    ex.execute()
    data = ex.parse()

    pprint.pprint(data)

    return data

def epoch(n, window=1.0):
    # by default, test-gen starts at 1e15
    return int(1e15 + n * window * 1e9) / int(window * 1e9)

def test_bc_simple():
    """A simple BC test case, a pyramid of size one"""
    shape = {
        "num-phases": 1,
        "num-workers": 2,
        "phase-duration": 1000,
        "activity-infix": "BarrierProcessing",
    }
    data = generate("bc_simple", shape, 1.0)

    # the the parallel edges only have one path
    assert data[epoch(0)][('Processing', 1)] == 1
    assert data[epoch(0)][('BarrierProcessing', 0)] == 1

    # the start/end at worker 0 has two paths (but occurs twice))
    assert data[epoch(0)][('Processing', 0)] == 2 * 2

def test_lowest_activity_bc():
    """We have multiple (non-communicating) workers with different start points"""
    shape = {
        "num-phases": 1,
        "num-workers": 5,
        "phase-duration": 998, # ensures that the main activity stays within the window
        "offset": 1,
        "send-edge": "none",
        "recv-edge": "none",
    }
    data = generate("lowest_activity_bc", shape, 1.0)
    # only worker 0 will three processing events
    assert data[epoch(0)][('Processing', 0)] == 3
    # all other events are zero, thus missing from the summary
    assert all([bc == 0 for (activity, w), bc in data[epoch(0)].iteritems()
                        if activity == 'Processing' and w > 0])

def test_wsa_terminating_in_recv():
    """Tests the insertion of waiting edges before a receive"""
    shape = {
        "num-phases": 2,
        "num-workers": 4,
        "phase-duration": 500,
        "recv-edge": "none",
        "activity-suffix": "Processing",
    }
    data = generate("wsa_terminating_in_recv", shape, 1.0)
    # all gaps end in a terminating edge, thus we must not see any unknowns
    assert all([ty != "Unknown" for ty, w in data[epoch(0)].keys()])

def test_wsa_fill_unknown():
    """Tests the insertion of unknowns for local events"""
    shape = {
        "num-phases": 1,
        "num-workers": 2,
        "phase-duration": 1000,
        "send-edge": "none",
        "recv-edge": "none",
        "activity-infix": "None", # must be replaced by unknown
    }
    data = generate("wsa_fill_unknown", shape, 1.0)
    # all gaps end in a terminating edge, thus we must not see any unknowns
    assert data[epoch(0)][('Processing', 0)] == 2 # occurs twice
    assert data[epoch(0)][('Unknown', 255)] == 1

def test_wsa_single_path():
    """Tests the insertion of waiting stages when waiting for a response"""
    last_worker = 4
    shape = {
        "num-phases": 1,
        "num-workers": last_worker + 1,
        "phase-duration": 1000,
        "activity-infix": "Waiting", # must be replaced by unknown
    }
    data = generate("wsa_single_path", shape, 1.0)

    # the last worker must have the BC of 1
    assert data[epoch(0)][('Processing', last_worker)] == 1
    # the others have a BC of two since they occur twice
    assert all([bc == 2 for (act, w), bc in data[epoch(0)].items() if w < last_worker and act != "Waiting"])

def test_forest():
    """Repeated number of trees, tests total number of paths"""
    k = 5 # number of workers
    r = 1 # repetitions per snapshot
    t = 1000
    shape = {
        "num-phases": r*10,
        "num-workers": k,
        "activity-infix": "Buffer",
        "phase-duration": t/r,
    }
    data = generate("forest", shape, t/1e3)
    for epoch in data.keys():
        # the processing on worker 0 follow the formula k^r
        assert data[epoch][('Processing', 0)] == k**r * 2*r

def test_tree():
    """Tests the number of paths in a single pyramid/tree"""
    k = 5 # number of workers
    t = 1000
    shape = {
        "num-phases": 1,
        "num-workers": k,
        "activity-infix": "Buffer",
        "activity-suffix": "Waiting",
        "phase-duration": t,
    }
    data = generate("tree", shape, 1.0)

    # last worker has a single path
    assert data[epoch(0)][('Processing', k-1)] == 1
    for w in range(k-1):
        assert data[epoch(0)][('Processing', w)] == 2 * (k-w)

    # the infix activities all are only on a single path
    assert all([bc == 1 for (act, w), bc in data[epoch(0)].items() if act == "Buffer"])

def test_bc_inout():
    """Tests the number of paths in a single pyramid/tree"""
    k = 10 # number of workers
    t = 1000
    shape = {
        "num-phases": 1,
        "num-workers": k,
        "activity-infix": "Buffer",
        "activity-prefix": "Serialization",
        "activity-suffix": "Deserialization",
        "phase-duration": t,
    }
    data = generate("bc_inout", shape, 1.0)

    # the prefixes and suffixes must have the same bc
    for w in range(1, k):
        assert data[epoch(0)][('Serialization', w)] == data[epoch(0)][('Deserialization', w)]

def test_bc_inout_cut():
    """Tests the number of paths in a single pyramid/tree"""
    k = 10 # number of workers
    t = 3000
    shape = {
        "num-phases": 1,
        "num-workers": k,
        "activity-infix": "Buffer",
        "activity-prefix": "Serialization",
        "activity-suffix": "Deserialization",
        "phase-duration": t,
    }
    data = generate("bc_inout_cut", shape, 1.0)

    # epoch 0 and 3 are symmetric, thus must have the same BC
    # (for activities fully contained in epoch 3)
    for (activity, worker), bc in data[epoch(2)].iteritems():
        if activity == "Deserialization":
            assert data[epoch(0)][("Serialization", worker)] == bc
        if activity == "Processing" and worker > 0:
            # note: worker 0 is assymetric due to WSA replacing the buffer acitivity with a wait
            assert data[epoch(0)][("Processing", worker)] == bc

if __name__ == '__main__':
    # extract all functions of the form test_*
    tests = {name: obj for name, obj in inspect.getmembers(sys.modules[__name__])
                         if inspect.isfunction(obj) and name.startswith('test_') }

    parser = argparse.ArgumentParser()
    parser.add_argument('test', choices=tests.keys(), nargs='?')
    parser.add_argument('--outdir', type=str, default="test")
    args = parser.parse_args()

    OUTDIR = args.outdir


    if args.test:
        names = [args.test]
    else:
        names = tests.keys()

    total_tests = len(names)
    failed_tests = 0

    for n in names:
        print("============== Testing {} ==============".format(n))
        try:
            tests[n]()
        except:
            print("Test case {} failed!".format(n))
            traceback.print_exc()
            failed_tests += 1

    print("============== Finished ==============".format(n))
    print("   Total number of tests: {}".format(total_tests))
    print("  Failed number of tests: {}".format(failed_tests))
    if failed_tests:
        sys.exit(1)
