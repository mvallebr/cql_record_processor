import argparse
import logging
import sys
import traceback
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

session = None


def parse_arguments(arguments):
    global config
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", default="localhost", help="destination cassandra host where to copy the rows to")
    parser.add_argument("-ks", "--keyspace", default="identification", help="destination keyspace")
    parser.add_argument("-cf", "--column-family", default="entity_lookup", help="destination column family")

    args = parser.parse_args(arguments)

    return args

def process(rows, host, module_arguments, parsed_args):
    global session
    if not session:
        cluster = Cluster([parsed_args.host], port=9042, control_connection_timeout=60)
        session = cluster.connect(parsed_args.keyspace)
        session.default_timeout=100

    logging.debug("processing...")
    query_str = ['BEGIN UNLOGGED BATCH']
    params = []
    for row in rows:
        query_str.append("insert into entity_lookup (name, value, entity_id) values(%s, %s, %s)")
        params.extend([row[0], row[1], row[2]])
    query_str.append('APPLY BATCH;')

    try:
        query = '\n'.join(query_str)
        query = SimpleStatement(query, consistency_level=ConsistencyLevel.ALL)
        session.execute(query, params, timeout=100)

        logging.debug("Executed %d inserts" % len(rows) )
    except:
        logging.error("Exception applying batch %s" % query_str)
        logging.error("Exception in user code:")
        logging.error('-' * 60)
        e = sys.exc_info()
        logging.error(e)
        logging.error(traceback.format_exc())
        logging.error('-' * 60)
        raise
