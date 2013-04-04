#!/usr/bin/env python
#
# Copyright 2011 Formspring
#
# Licensed under the Apache License, Version 2.0 (the "License"); you may
# not use this file except in compliance with the License. You may obtain
# a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
# WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
# License for the specific language governing permissions and limitations
# under the License.

import httplib
import json
import datetime
import boto
import socket

#import aws keys, you can do it this way or through env variables (check boto doc)
#or through any means you want. You decide.
aws = open('awscreds.conf')
aws_key = aws.readline().split('=')[1].strip()
aws_secret = aws.readline().split('=')[1].strip()
c = boto.connect_cloudwatch(aws_access_key_id=aws_key, aws_secret_access_key=aws_secret)

#get stats
def getstats(host='localhost', port=8098):
    client = httplib.HTTPConnection(host, port)
    client.request('GET', '/stats')
    response = client.getresponse()
    response_body = response.read()
    response.close()
    client.close()
    js = json.loads(response_body)
    return js

def publish(stats):
    metrics = [
      ('Microseconds',
       [('node_get_fsm_time_95', int(stats['node_get_fsm_time_95'])),
        ('node_get_fsm_time_99', int(stats['node_get_fsm_time_99'])),
        ('node_put_fsm_time_95', int(stats['node_put_fsm_time_95'])),
        ('node_put_fsm_time_99', int(stats['node_put_fsm_time_99']))
       ]),
      ('Count/Second',
       [('node_gets', int(stats['node_gets'])/60),
        ('node_puts', int(stats['node_puts'])/60)
       ]),
      ('Megabytes',
       [('mem_total_mb', int(stats['mem_total'])/(1024*1024)),
        ('mem_alloc_mb', int(stats['mem_allocated'])/(1024*1024))
       ]),
      ('Percent',[
        ('mem_alloc_%', int(int(stats['mem_allocated'])*100 / float(stats['mem_total'])))
       ]),
      ('Count',[('nodes', len(stats['connected_nodes']))])
    ]
    ts = datetime.time()
    hostname = socket.gethostname()
    dims={ 'Hostname':hostname }
    for munit,mtrcs in metrics:
      for mname, mval in mtrcs:
        #print("{0} = {1} {2}".format(mname, mval, munit))
        c.put_metric_data('Riak', mname, timestamp=ts, dimensions=dims, value=mval, unit=munit)

if __name__ == '__main__':
    #seems to work on AWS, if you bind to the internal IP, change at will
    host = socket.gethostbyname_ex(socket.gethostname())[2][0]
    stats = getstats(host=host)
    publish(stats)

