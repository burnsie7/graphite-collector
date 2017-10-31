#!/usr/bin/python
"""Copyright 2013 Bryan Irvine
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at
   http://www.apache.org/licenses/LICENSE-2.0
Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License."""

import random
import re
import sys
import time
import socket
import platform
import subprocess
import pickle
import struct

CARBON_SERVER = '127.0.0.1'
CARBON_PICKLE_PORT = 17310
DELAY = 1

def _genMetrics():
    # generate some random metrics names
    d_list = ['abc'] # , 'def']
    t_list = [str(i) for i in range(1000, 4000)]
    i_list = ['hostname_' + str(i) for i in range(100)]
    d_len = len(d_list)
    t_len = len(t_list)
    i_len = len(i_list)
    met_list = []
    for i in range(d_len * t_len * i_len):
        t = t_list[random.randint(0, t_len-1)]
        d = d_list[random.randint(0, d_len-1)]
        n = i_list[random.randint(0, i_len-1)]
        met = "myapp.prefix." + d + ".prod." + n + ".storage." + t + ".save.carbon"
        met_list.append(met)
    return met_list

def run(sock, delay):
    """Make the client go go go"""
    met_list = _genMetrics()
    m_len = len(met_list)
    count = 0
    while True:
        count += 1
        now = int(time.time())
        tuples = ([])
        lines = []
        loadavg = 1
        met = met_list[random.randint(0, m_len-1)]
        tuples.append((met, (now, loadavg)))
        lines.append("%s %s %d" % (met, loadavg, now))
        message = '\n'.join(lines) + '\n'
        package = pickle.dumps(tuples, 1)
        size = struct.pack('!L', len(package))
        sock.sendall(size)
        sock.sendall(package)
        if count % 30000 == 0:
            # create a new connection to allow load balancing
            time.sleep(1)
            sock.close()
            sock = socket.socket()
            try:
                sock.connect( (CARBON_SERVER, CARBON_PICKLE_PORT) )
            except socket.error:
                raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is carbon-cache.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PICKLE_PORT })

def main():
    delay = DELAY
    if len(sys.argv) > 1:
        arg = sys.argv[1]
        if arg.isdigit():
            delay = int(arg)
        else:
            sys.stderr.write("Ignoring non-integer argument. Using default: %ss\n" % delay)

    sock = socket.socket()
    try:
        sock.connect( (CARBON_SERVER, CARBON_PICKLE_PORT) )
    except socket.error:
        raise SystemExit("Couldn't connect to %(server)s on port %(port)d, is carbon-cache.py running?" % { 'server':CARBON_SERVER, 'port':CARBON_PICKLE_PORT })

    try:
        run(sock, delay)
    except KeyboardInterrupt:
        sys.stderr.write("\nExiting on CTRL-c\n")
        sys.exit(0)

if __name__ == "__main__":
    main()
