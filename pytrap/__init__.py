"""
Python Tracing and Profiling

Copyright (C) Michael Permana <mpermana@hotmail.com>
"""
import elasticsearch
import pymongo
import ssl
import traceback
from collections import defaultdict
from os import getenv
from pprint import pformat
from socket import gethostname
from uuid import uuid1
from datetime import datetime

print __name__, 'mpermana instrumented pymongo'
execution_number = 0
total_seconds = defaultdict(int)
memory_output = []

def get_string_stack():
    stack = filter(lambda s: 'decorator' not in str(s), traceback.extract_stack())
    return [ str(i) for i in stack[-20:-1] ]


try:
    es = elasticsearch.Elasticsearch()
    es.info()
    print 'es', es
except:
    from mock import MagicMock
    es = MagicMock()

session_id = str(uuid1())
hostname = gethostname()

def output(data):
    memory_output.append(data)
    mode = 'xtime'
    if mode == 'time':
        print pformat(data)
    elif mode == 'stack':
        print data['execution_number'], data.get('total_seconds'), pformat(data.get('collection')), pformat(data.get('query')), pformat(data['stack'])
    if 'es' in mode:
        es.index(index='mpermana', doc_type='mongo-query', body=data)


def make_method_proxy(method_name):
    method = eval(method_name)

    def __method_proxy(*args, **kwargs):
        global execution_number
        data = {'method_name': method_name,
         'start_time': datetime.now(),
         'args': str(args),
         'kwargs': str(kwargs),
         'stack': get_string_stack(),
         'execution_number': execution_number,
         'session_id': session_id,
         'hostname': hostname,
         'query': str(args[1:2])}
        execution_number += 1
        if args:
            if isinstance(args[0], pymongo.collection.Collection):
                collection = args[0]
                data['collection'] = collection.name
                data['database_name'] = collection.database.name
            elif isinstance(args[0], elasticsearch.transport.Transport):
                data['collection'] = args[2]
                data['query'] = kwargs['body']
        result = method(*args, **kwargs)
        now = datetime.now()
        if isinstance(result, pymongo.cursor.Cursor):
            result.mpermana_data = data
            data['first_cursor_time'] = now
        data['stop_time'] = now
        data['total_seconds'] = (data['stop_time'] - data['start_time']).total_seconds()
        total_seconds[method_name] += data['total_seconds']
        output(data)
        return result

    return __method_proxy


_next = pymongo.cursor.Cursor.next
def __next(self):
    data = self.mpermana_data
    try:
        data.pop('stop_time', None)
        result_next = _next(self)
        return result_next
    except StopIteration:
        data['stop_time'] = datetime.now()
        data['total_seconds'] = (data['stop_time'] - data['start_time']).total_seconds()
        output(data)
        raise StopIteration
    return


instrument = getenv('INSTRUMENT', 'pymongo').split(',')
if 'pymongo' in instrument:
    pymongo.cursor.Cursor.next = __next
    pymongo.collection.Collection.find = make_method_proxy('pymongo.collection.Collection.find')
    pymongo.collection.Collection.save = make_method_proxy('pymongo.collection.Collection.save')
    pymongo.collection.Collection.update = make_method_proxy('pymongo.collection.Collection.update')
    pymongo.collection.Collection.insert = make_method_proxy('pymongo.collection.Collection.insert')
if 'ssl' in instrument:
    ssl.SSLSocket.read = make_method_proxy('ssl.SSLSocket.read')
if 'elasticsearch' in instrument:
    elasticsearch.transport.Transport.perform_request = make_method_proxy('elasticsearch.transport.Transport.perform_request')
