### Multi-worker threaded graphite sink with redis and backend collector for converting and forwarding large volume graphite / carbon metrics to Datadog.  If you need to process < 1000 metrics / sec this is probably overkill.  See here for a smaller implementation: https://github.com/burnsie7/api-graphite 

### Step 0 - Datadog Agent

This assumes the Datadog Agent, including the dogstatsd service is running on your host.  The metric collector sends metrics to Datadog using dogstatsd.

For information on installing the client see here:  https://docs.datadoghq.com/guides/basic_agent_usage/

### Step 1 - Graphite sink(s) and Metric collector

```
git clone https://github.com/burnsie7/graphite-collector.git
cd graphite-collector
sudo apt-get update
sudo apt-get install supervisor
sudo apt-get install python-pip
sudo apt-get install redis-server
sudo pip install redis
sudo pip install datadog
sudo pip install tornado
```

Navigate to the repo directory and edit graphite_sink.py, updating 'myapp.prefix' with the metric prefix you are sending to datadog.

#### To run from the cli for testing purposes:

`python graphite_sink.py 17310`  
`python metric_collector.py`  

carbon_client.py is included to generate metrics with unique tags and send high throughput to the graphite_sink(s).  

#### To install as a service:

Edit /etc/supervisor/conf.d/supervisor.conf.  Add the following, updating 'numprocs' for graphite_sink only.  metric_collector should always only use one proc.
```
[program:graphite_sink]
command=python /exact/path/to/graphite-collector/graphite_sink.py 1731%(process_num)01d
process_name=%(program_name)s_%(process_num)01d
redirect_stdout=true
user=ubuntu
stdout_logfile=/var/log/gsink_%(process_num)01d.log
numprocs=<NUMBER OF PROCS ALLOCATED>

[program:metric_collector]
command=python /exact/path/to/graphite-collector/metric_collector.py
process_name=%(program_name)s_%(process_num)01d
redirect_stdout=true
user=ubuntu
stdout_logfile=/var/log/metric_collector_%(process_num)01d.log
numprocs=1
```

Update supervisor and restart all services.

```
sudo supervisorctl
update
restart all
```

### Step 2 - Your carbon-relay

Point your carbon relay or haproxy at the graphite sinks specified in step 2.  Note that the number of sinks on an individual host is configured by 'numprocs' in /etc/supervisor/conf.d/supervisor.conf.  The port of the first sink will be 17310 and the port will increment for additional procs.  For example, if numprocs were set to 4:

sink-hostname:17310  
sink-hostname:17311  
sink-hostname:17312  
sink-hostname:17313  

There are different options for distributing carbon relay, whether set with destinations directly in the carbon config or using haproxy.  Distribute the requests across the different sinks you have configured.

If using relay rules it is advantageous to send only the metrics you wish to see in datadog to the sinks.  For example:

```
[datadog]
pattern = ^myapp\.prefix.+
destinations = haproxy:port

[default]
default = true
destinations = 127.0.0.1:2004
```
