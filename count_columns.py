#!/usr/bin/env python
import argparse
import logging
import sys
import traceback
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement

# Check http://www.datastax.com/dev/blog/thrift-to-cql3
def count(host, keyspace, cf):
    cluster = Cluster([host], port=9042, control_connection_timeout=None)
    session = cluster.connect(keyspace)
    session.default_timeout=None

    st = SimpleStatement("SELECT count(*) FROM %s" % cf, consistency_level=ConsistencyLevel.ALL)
    for row in session.execute(st, timeout=None):
        print "count for cf %s = %s " % (cf, str(row))


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-cf", "--column-family", help="Column Family to query")
    parser.add_argument("-ks", "--keyspace", help="Cassandra Keyspace")
    parser.add_argument("-H", "--host", default="localhost", help="Cassandra host")    
    args = parser.parse_args()

    try:
        count(args.host, args.keyspace, args.column_family)
    except:
        logging.error("Exception in user code:")
        logging.error('-' * 60)
        e = sys.exc_info()
        logging.error(e)
        logging.error(traceback.format_exc())
        logging.error('-' * 60)

    print "fim"