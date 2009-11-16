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

import os
import time
import random
import json
import subprocess

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
    type = primitive_types[random.randint(0, len(primitive_types) - 1)]
    form = random.randint(0, 1)
    if form == 0:
        return type
    elif form == 1:
        return type
#        return {'type':type}

record_count = 0
def generate_record():
    global record_count
    name = "Record_%d" % record_count
    record_count += 1
    fields = []
    for i in xrange(random.randint(0, 7)):
        fields.append({'name':'f%d'%i, 'type':random_type()})
    return {'type':'record', 'name':name, 'fields':fields}

enum_count = 0
def generate_enum():
    global enum_count
    name = "Enum_%d" % enum_count
    enum_count += 1
    # AAAA, BBBB, etc...
    symbols = [chr(0x41 + i)*4 for i in xrange(random.randint(0, 5))]
    return {'type':'enum', 'name':name, 'symbols':symbols}

def generate_array():
    return {'type':'array', 'items':random_type()}

def generate_map():
    return {'type':'map', 'values':random_type()}

def generate_union():
    union =  [random_primitive_type() for i in xrange(random.randint(0, 5))]
    # uniqify
    # fixme
    result = {}
    for e in union:
        if type(e) == dict:
            result[e['type']] = e
        else:
            result[e] = e
    return result.values()

fixed_count = 0
def generate_fixed():
    global fixed_count
    name = "Fixed_%d" % fixed_count
    fixed_count += 1
    return {'type':'fixed', 'size':random.randint(0,32), 'name':name}

type_funcs = (random_primitive_type, generate_record, generate_enum,
              generate_array, generate_map, generate_union, generate_fixed)

def random_type():
    func = type_funcs[random.randint(0, len(type_funcs) - 1)]
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
        for i in xrange(random.randint(0, 20)):
            name = "M%d" % i
            self.messages[name] = self.gen_message()

    def gen_message(self):
        message = {}
        request = []
        for i in xrange(random.randint(0, 7)):
            param = "p%d" % i
            request.append({'name':param, 'type':random_type()})
        message['request'] = request
        message['response'] = random_type()
        return message

def fschema_gen(count):
    global start_time
    i = 0
    while count != 0:
        yield FSchema("Favro%d_%08d" % (start_time, i))
        i += 1
        if count > 0:
            count -= 1

class BadJava(Exception):
    def __init__(self, returncode, stdout, stderr):
        self.returncode = returncode
        self.stdout = stdout
        self.stderr = stderr

def java_parse(s):
    f = open('/tmp/in', 'w')
    f.write(s)
    f.close()

    p = subprocess.Popen("rm -fr /tmp/out", shell=True)
    sts = os.waitpid(p.pid, 0)[1]

    root = '../avro/build/lib'
    dirList = os.listdir(root)
    jars = [os.path.join(root, fname) for fname in dirList if fname.endswith('.jar')]
    cp = ':'.join(jars)

    avro_compiler = subprocess.Popen(['java', '-cp', '../avro/build/classes:%s' % cp,
                                      'org.apache.avro.specific.SpecificCompiler',
                                      '/tmp/in', '/tmp/out'],
                                stdout=subprocess.PIPE)
    while avro_compiler.returncode == None:
        stdout, stderr = avro_compiler.communicate()
    if avro_compiler.returncode != 0:
        raise BadJava(avro_compiler.returncode, stdout or '', stderr or '')

    root = '/tmp/out/favro'
    dirList = os.listdir(root)
    srcs = [os.path.join(root, fname) for fname in dirList if fname.endswith('.java')]
    cmd = ['javac', '-cp', '../avro/build/classes:%s' % cp]
    cmd.extend(srcs)
    javac = subprocess.Popen(cmd, stdout=subprocess.PIPE)
    while javac.returncode == None:
        stdout, stderr = javac.communicate()
    if javac.returncode != 0:
        raise BadJava(javac.returncode, stdout or '', stderr or '')

if __name__ == '__main__':
    for s in fschema_gen(options.count):
        if options.verbose:
            print(s)

        try:
            java_parse(repr(s))
            avpr.parse(repr(s))
        except Exception as e:
            print(s)
            print(e)
