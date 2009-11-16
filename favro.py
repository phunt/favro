#!/usr/bin/env python

# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import time
import random
import json

from optparse import OptionParser

import avro.protocol as avpr

start_time = int(time.time())

usage = "usage: %prog [options]"
parser = OptionParser(usage=usage)
parser.add_option("-v", "--verbose",
                  action="store_true", dest="verbose", default=False,
                  help="verbose output, include more detail")
parser.add_option("", "--count", dest="count", type="int",
                  default=1000, help="number of schemas to create, -1 loops forever (default 1000)")

(options, args) = parser.parse_args()

primitive_types = ('string', 'bytes', 'int', 'long', 'float', 'double',
                   'boolean', 'null')

def random_primitive_type():
    type = primitive_types[random.randint(0, 7)]
    form = random.randint(0, 1)
    if form == 0:
        return type
    elif form == 1:
        return type
#        return {'type':type}

def random_primitive_array():
    return [random_primitive_type() for i in xrange(random.randint(0, 5))]

record_count = 0
def generate_record():
    global record_count
    name = "record_%d" % record_count
    record_count += 1
    if random.randint(0, 1) == 0:
        type = random_primitive_type()
    else:
        type = random_primitive_array()
    fields = [{'name':'f%d'%i, 'type':type} for i in xrange(random.randint(0, 5))]
    return {'type':'record', 'name':name, 'fields':fields}

enum_count = 0
def generate_enum():
    global enum_count
    name = "enum_%d" % enum_count
    enum_count += 1
    symbols = [random_primitive_type() for i in xrange(random.randint(0, 5))]
    return {'type':'enum', 'name':name, 'symbols':symbols}

def generate_array():
    return {'type':'array', 'items':random_primitive_type()}

def generate_map():
    return {'type':'map', 'values':random_primitive_type()}

def generate_union():
    return [random_primitive_type() for i in xrange(random.randint(0, 5))]

fixed_count = 0
def generate_fixed():
    global fixed_count
    name = "fixed_%d" % fixed_count
    fixed_count += 1
    return {'type':'fixed', 'size':random.randint(0,32), 'name':name}

type_funcs = (random_primitive_type, random_primitive_array, generate_record,
              generate_enum, generate_array, generate_map, generate_union,
              generate_fixed)

def random_type():
    func = type_funcs[random.randint(0, 7)]
    return func()

class FSchema(object):
    def __init__(self, name):
        self.protocol = {}
        self.protocol['protocol'] = name
        self.protocol['namespace'] = 'favro'
        self.types = []
        self.gen_messages()
        self.protocol['types'] = self.types
        self.protocol['messages'] = self.messages

    def __repr__(self):
        return json.dumps(self.protocol)

    def gen_messages(self):
        self.messages = {}
        for i in xrange(random.randint(0, 3)):
            name = "m%d" % i
            self.messages[name] = self.gen_message()

    def gen_message(self):
        message = {}
        request = []
        for i in xrange(random.randint(0, 3)):
            param = "p%d" % i
            request.append({'name':param, 'type':random_type()})
        message['request'] = request
        message['response'] = random_type()
        return message

def fschema_gen(count):
    global start_time
    i = 0
    while count != 0:
        yield FSchema("favro%d_%08d" % (start_time, i))
        i += 1
        if count > 0:
            count -= 1

if __name__ == '__main__':
    for s in fschema_gen(options.count):
        if options.verbose:
            print(s)

        try:
            avpr.parse(repr(s))
        except Exception as e:
            print(s)
            print(e)
