#!/usr/bin/env python
# encoding: utf-8

# Generates a text-readable dump of a Tensorflow summary (used for
# introspection with Tensorboard and for the Chrome trace visualizations)
# Recommended reading: https://www.tensorflow.org/get_started/summaries_and_tensorboard

import os

import tensorflow as tf
from tensorflow.core.framework import graph_pb2
from tensorflow.core.protobuf import config_pb2
from tensorflow.core.protobuf import meta_graph_pb2
from tensorflow.python.platform import app
from tensorflow.python.platform import flags
from tensorflow.python.platform import tf_logging as logging
from tensorflow.python.summary import event_file_inspector as efi

logging.set_verbosity(logging.INFO)  # Set threshold for what will be logged

FLAGS = flags.FLAGS

flags.DEFINE_string(
    'event_file', '',
    'The TensorFlow summary file to use as input.')

def main(unused_argv=None):
    event_file = os.path.expanduser(FLAGS.event_file)
    if not event_file:
        msg = ('The path to an event_file must be specified. '
               'Run `inspect_summary.py --help` for usage instructions.')
        logging.error(msg)
        return -1

    for event in tf.train.summary_iterator(event_file):
        # Yields a sequence of `tensorflow.core.util.event_pb2.Event`
        # https://github.com/tensorflow/tensorflow/blob/master/tensorflow/core/util/event.proto

        #print '>>', event
        #print event.wall_time, event.step
        #import IPython; IPython.embed(); exit()

        what = event.WhichOneof('what')
        if what == 'file_version':
            assert event.file_version == "brain.Event:2"
        elif what == 'graph_def':
            # An encoded version of a GraphDef.
            logging.info('=' * 80)
            logging.info('GraphDef[wall_time=%s, step=%s]' % (event.wall_time, event.step))
            graph = graph_pb2.GraphDef()
            graph.ParseFromString(event.graph_def)
            logging.info(str(graph))
            logging.info('=' * 80)
        elif what == 'summary':
            logging.warning('NYI: summary')
        elif what == 'log_message':
            logging.warning('NYI: log_message')  # user output a log message
        elif what == 'session_log':
            logging.warning('NYI: session_log')
        elif what == 'tagged_run_metadata':
            trn = event.tagged_run_metadata
            logging.info('=' * 80)
            logging.info('TaggedRunMetadata [wall_time=%s, step=%s, tag=%s]' % (event.wall_time, event.step, trn.tag))
            # Lazy deserialization from a byte buffer
            run_meta = config_pb2.RunMetadata()
            run_meta.ParseFromString(trn.run_metadata)
            logging.info(str(run_meta))
            logging.info('=' * 80)
        elif what == 'meta_graph_def':
            # An encoded version of a MetaGraphDef.
            logging.info('=' * 80)
            logging.info('MetaGraphDef [wall_time=%s, step=%s]' % (event.wall_time, event.step))
            meta_graph = meta_graph_pb2.MetaGraphDef()
            meta_graph.ParseFromString(event.meta_graph_def)
            logging.info(str(meta_graph))
            logging.info('=' * 80)
        else:
            raise NotImplemented()

if __name__ == "__main__":
    app.run()
