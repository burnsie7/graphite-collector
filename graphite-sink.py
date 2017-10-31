# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
from argparse import ArgumentParser
import cPickle as pickle
import logging
import threading
import time
import struct
import sys

import json
import redis

from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado import netutil, process

log = logging.getLogger(__name__)
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
out_hdlr.setLevel(logging.INFO)
log.addHandler(out_hdlr)
log.setLevel(logging.INFO)

REDIS_HOST = 'localhost'
REDIS_PORT = 6379

METRIC_STORE = {}
METRIC_COUNT = 0


def get_and_clear_store():
    global METRIC_STORE
    temp_store = METRIC_STORE.copy()
    METRIC_STORE = {}
    global METRIC_COUNT
    count = [METRIC_COUNT]
    METRIC_COUNT = 0
    return temp_store, count[0]

class GraphiteServer(TCPServer):

    def __init__(self, io_loop=None, ssl_options=None, uid=None, **kwargs):
        TCPServer.__init__(self, io_loop=io_loop, ssl_options=ssl_options, **kwargs)
        self.uid = uid
        self._queueMetrics()

    def _queueMetrics(self):
        temp_store, count = get_and_clear_store()
        start_time = time.time()
        log.debug(str(temp_store))
        conn = redis.Redis(host=REDIS_HOST, port=REDIS_PORT)
        conn.set("metrics_" + str(self.uid) + "_" + str(start_time), json.dumps(temp_store))
        log.info("sent {} metrics with {} unique names in {} seconds\n".format(str(count), str(len(temp_store)), str(time.time() - start_time)))
        threading.Timer(10, self._queueMetrics).start()

    def handle_stream(self, stream, address):
        GraphiteConnection(stream, address)


class GraphiteConnection(object):

    def __init__(self, stream, address):
        log.info('received a new connection from {}'.format(address))
        self.stream = stream
        self.address = address
        self.stream.set_close_callback(self._on_close)
        self.stream.read_bytes(4, self._on_read_header)

    def _on_read_header(self, data):
        try:
            size = struct.unpack("!L", data)[0]
            log.debug("Receiving a string of size: {}".format(str(size)))
            self.stream.read_bytes(size, self._on_read_line)
        except Exception as e:
            log.error(e)

    def _on_read_line(self, data):
        log.debug('read a new line')
        self._decode(data)

    def _on_close(self):
        log.info('client quit')

    def _processMetric(self, metric, datapoint):
        # Update 'myapp.prefix' with the metric prefix you would like to send to Datadog.
        if metric is not None and metric.startswith('myapp.prefix'):
            global METRIC_COUNT
            METRIC_COUNT += 1
            components = metric.split('.')
            components.pop(4)
            metric = '.'.join(components)
            try:
                val = datapoint[1]
                if metric in METRIC_STORE:
                    current = METRIC_STORE[metric]
                    new_val = current + val
                    METRIC_STORE[metric] = new_val
                else:
                    METRIC_STORE[metric] = val
            except Exception as e:
                log.error(e)

    def _decode(self, data):

        try:
            datapoints = pickle.loads(data)
        except Exception:
            log.exception("Cannot decode grapite points")
            return

        for (metric, datapoint) in datapoints:
            try:
                datapoint = (float(datapoint[0]), float(datapoint[1]))
            except Exception as e:
                log.error(e)
                continue

            self._processMetric(metric, datapoint)

        self.stream.read_bytes(4, self._on_read_header)


def start_graphite_listener(port):

    echo_server = GraphiteServer(uid=port)
    echo_server.listen(port)
    IOLoop.instance().start()

if __name__ == '__main__':

    parser = ArgumentParser(description='run a tornado graphite sink')
    parser.add_argument('port', help='port num')
    args = parser.parse_args()
    port = args.port
    start_graphite_listener(port)
