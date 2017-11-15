# (C) Datadog, Inc. 2010-2016
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)
"""Aggregate metrics from one or more graphite sinks and submit to Datadog via dogstatsd"""

# stdlib
import logging
import os
import threading
import time
import sys

import json
import redis

from datadog import statsd

LOGGER = logging.getLogger(__name__)
OUT_HDLR = logging.StreamHandler(sys.stdout)
OUT_HDLR.setFormatter(logging.Formatter('%(asctime)s %(message)s'))
OUT_HDLR.setLevel(logging.INFO)
LOGGER.addHandler(OUT_HDLR)
LOGGER.setLevel(logging.INFO)


def _convert_graphite_to_tags(metric):
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

    def __init__(self):
        self.metric_store = {}
        self._send_metrics()

    def _aggregate_and_clear_metrics(self):
        conn = redis.Redis('localhost')
        try:
            for metric in conn.scan_iter("metric*"):
                current = conn.get(metric)
                metric_dict = json.loads(current)
                for k, val in metric_dict.items():
                    if self.metric_store.get(k, None):
                        current_value = self.metric_store[k]
                        new_value = current_value + val
                        self.metric_store[k] = new_value
                    else:
                        self.metric_store[k] = val
                conn.delete(metric)
        except Exception as ex:
            LOGGER.error(ex)
        temp_store = self.metric_store.copy()
        self.metric_store = {}
        return temp_store

    def _send_metrics(self):
        temp_store = self._aggregate_and_clear_metrics()
        all_metrics = []
        start_time = time.time()
        for metric, val in temp_store.items():
            try:
                metric, tags = _convert_graphite_to_tags(metric)
                all_metrics.append({'metric': metric, 'points': val, 'tags': tags})
            except Exception as ex:
                LOGGER.error(ex)
        if all_metrics:
            for metric in all_metrics:
                statsd.gauge(metric['metric'], metric['points'], tags=metric['tags'])
            LOGGER.info("sent %r unique metric names in %r seconds\n",
                        len(all_metrics), time.time() - start_time)
        else:
            LOGGER.info("no metrics received")
        threading.Timer(60, self._send_metrics).start()


if __name__ == '__main__':
    LOGGER.info('Starting Metric Collector')
    COLLECTOR = MetricCollector()
