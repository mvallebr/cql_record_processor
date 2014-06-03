import argparse
import logging
from multiprocessing import Semaphore
import sys
import traceback
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from s1base.s1pool import S1Pool

session = None

def process(list_items, host):
    global session
    if not session:
        keyspace = "identification"
        cluster = Cluster([host], port=9042, control_connection_timeout=60)
        session = cluster.connect(keyspace)
        session.default_timeout=100

    print "processing..."
    query_str = ['BEGIN UNLOGGED BATCH']
    params = []
    for item in list_items:
        query_str.append("insert into entity_lookup (name, value, entity_id) values(%s, %s, %s)")
        params.extend([item['name'], item['value'], item['entity_id']])
    query_str.append('APPLY BATCH;')

    try:
        query = '\n'.join(query_str)
        query = SimpleStatement(query, consistency_level=ConsistencyLevel.ALL)
        session.execute(query, params, timeout=100)

        print "Executed %d inserts" % len(list_items)
    except:
        print "Exception applying batch %s" % query_str
        logging.error("Exception in user code:")
        logging.error('-' * 60)
        e = sys.exc_info()
        logging.error(e)
        logging.error(traceback.format_exc())
        logging.error('-' * 60)
        raise
