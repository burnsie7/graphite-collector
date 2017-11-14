# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

# stdlib
import copy
import logging
import os
import threading
import time
import struct
import sys

import json
import redis

from tornado.ioloop import IOLoop
from tornado.tcpserver import TCPServer
from tornado import netutil, process

from datadog import api, initialize, statsd

log = logging.getLogger(__name__)
out_hdlr = logging.StreamHandler(sys.stdout)
out_hdlr.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
out_hdlr.setLevel(logging.INFO)
log.addHandler(out_hdlr)
log.setLevel(logging.INFO)

METRIC_STORE = {}
METRIC_COUNT = 0

DD_API_KEY = os.getenv('DD_API_KEY', '<YOUR_API_KEY>')
DD_APP_KEY = os.getenv('DD_APP_KEY', '<YOUR_APP_KEY>')

options = {
    'api_key': DD_API_KEY,
    'app_key': DD_APP_KEY
}

initialize(**options)


def combine_and_clear_metrics():
    global METRIC_STORE
    conn = redis.Redis('localhost')
    try:
        for thing in conn.scan_iter("metric*"):
            current = conn.get(thing)
            metric_dict = json.loads(current)
            for k, v in metric_dict.items():
                if METRIC_STORE.get(k, None):
                    current_value = METRIC_STORE[k]
                    new_value = current_value + v
                    METRIC_STORE[k] = new_value
                else:
                    METRIC_STORE[k] = v
            conn.delete(thing)
    except Exception as e:
        log.error(e)
    temp_store = METRIC_STORE.copy()
    METRIC_STORE = {}
    return temp_store


def convert_graphite_to_tags(metric):
    tags = []
    components = metric.split('.')

    # Customize to meet the format of you metric
    datacenter = 'datacenter:' + components.pop(2)
    env = 'env:' + components.pop(2)
    instance = 'instance:' + components.pop(2)
    tenant_id = 'tenant_id:' + components.pop(3)
    tags = [datacenter, env, instance, tenant_id]

    metric = '.'.join(components)
    return metric, tags


class MetricCollector(object):

    def __init__(self, **kwargs):
        self._sendMetrics()

    def _sendMetrics(self):
        temp_store = combine_and_clear_metrics()
        all_metrics = []
        start_time = time.time()
        for metric, val in temp_store.items():
            try:
                metric, tags = convert_graphite_to_tags(metric)
                all_metrics.append({'metric': metric, 'points': val, 'tags': tags})
            except Exception as e:
                log.error(e)
        if len(all_metrics):
            for metric in all_metrics:
                statsd.gauge(metric['metric'], metric['points'], tags=metric['tags'])
            log.info("sent {} unique metric names in {} seconds\n".format(str(len(all_metrics)), str(time.time() - start_time)))
        else:
            log.info("no metrics received")
        threading.Timer(60, self._sendMetrics).start()


if __name__ == '__main__':
    log.info('Starting Metric Collector')
    collector = MetricCollector()
