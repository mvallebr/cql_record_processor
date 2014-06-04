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
    parser.add_argument("-d", "--delimiter", default="|", help="delimiter to split columns")
    parser.add_argument("-q", "--quotes", default=None, help="quotes character")
    
    args = parser.parse_args(arguments)

    return args

def process(rows, host, module_arguments, parsed_args):
    quotes = parsed_args.quotes if parsed_args.quotes is not None else ""
    for row in rows:
        first = True
        for column in row:
            if not first:
                sys.stdout.write (parsed_args.delimiter)
            else:
                first = False
            sys.stdout.write ("%s%s%s" % (quotes, column, quotes))
        sys.stdout.write ("\n")