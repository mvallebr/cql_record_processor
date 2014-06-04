#!/usr/bin/env python
import argparse
from importlib import import_module
import logging
import sys
import traceback
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import SimpleStatement
from multiprocessing import Semaphore, Pool

workers_semaphore = None


def default_process(rows, host, module_arguments, parsed_args):
    print (str(rows))

def task_done(result):
    workers_semaphore.release()
    sys.stdout.flush()


def secure_process(process_method, rows, *kargs):
    try:
        process_method(rows, *kargs)
    except:
        print "Exception in process method:"
        print '-' * 60
        e = sys.exc_info()
        print e
        print traceback.format_exc()
        print '-' * 60


def add_task(dump_pool, process_method, rows, *kargs):
    workers_semaphore.acquire()
    arguments = [process_method, rows]
    for arg in kargs:
        arguments.append(arg)

    dump_pool.apply_async(secure_process, args=arguments, callback=task_done)


def parallel_process(host, workers, step, quiet, keyspace, column_family, limit, process_method, module_arguments, parsed_args):
    if not quiet:
        logging.info("parallel_dump started using %d workers" % workers)

    dump_pool = Pool(
        processes=workers,
        initializer=None,
        initargs=None,
        maxtasksperchild=None
    )  # start NUM_PROCESSES worker processes
    cluster = Cluster([host], port=9042, control_connection_timeout=60)
    session = cluster.connect(keyspace)
    session.default_timeout = 100

    cql_query = "SELECT * FROM %s %s" % (column_family, "limit %s" % limit if limit is not None else "")
    if not quiet:
        logging.info("Executing cql query '%s'" % (cql_query))
    st = SimpleStatement(cql_query, consistency_level=ConsistencyLevel.ALL)
    step_rows = []
    total_rows = 0
    for row in session.execute(st):
        step_rows.append(row[:])
        if len(step_rows) >= step:
            total_rows += len(step_rows)
            if not quiet:
                logging.info("Processed %d rows so far" % (total_rows))
            rows = step_rows[:]
            add_task(dump_pool, process_method, rows, host, module_arguments, parsed_args)
            step_rows = []

    if step_rows:
        add_task(dump_pool, process_method, step_rows, host, module_arguments, parsed_args)

    if not quiet:
        logging.info("parallel_dump ended using %d workers" % workers)

    dump_pool.close()
    dump_pool.join()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", default="localhost", help="Cassandra host")
    parser.add_argument("-w", "--workers", type=int, default=1, help="Amount of worker processes")
    parser.add_argument("-ks", "--keyspace", type=str, default="identification", help="Cassandra Origin Keyspace")
    parser.add_argument("-cf", "--column-family", type=str, default="entitylookup",
                        help="Cassandra Origin Column Family")
    parser.add_argument("-l", "--limit", type=int, default=None, help="If provided, limit amount of rows queried")
    parser.add_argument("-s", "--step", type=int, default=100, help="Process STEP rows at a time")
    parser.add_argument("-q", "--quiet", type=bool, default=False, help="Quiet output")
    parser.add_argument("-m", "--module", type=str, nargs=argparse.REMAINDER, default=None,
                        help="Python Module that contains the process method")

    args = parser.parse_args()
    if not args.quiet:
        logging.basicConfig(level=logging.INFO)
    else:
        logging.basicConfig(level=logging.ERROR)
    workers_semaphore = Semaphore(args.workers)

    if not args.quiet:
        logging.info("cf row processor started with %d processes, step = %d" % (args.workers, args.step))
    try:
        process_method = default_process
        parsed_args = None
        if args.module is not None:
            python_module = import_module(args.module[0])
            logging.info("Module '%s' arguments: '%s' \n" % (args.module[0], str(args.module[1:])))
            if hasattr(python_module, 'parse_arguments'):
                parsed_args = python_module.parse_arguments(args.module[1:])
            if hasattr(python_module, 'process'):
                process_method = python_module.process

        module_arguments = args.module[1:] if args.module is not None else None
        parallel_process(args.host, args.workers, args.step, args.quiet, args.keyspace, args.column_family, args.limit,
                         process_method, module_arguments, parsed_args)
    except SystemExit:
        pass
    except:
        logging.error("Exception in user code:")
        logging.error('-' * 60)
        e = sys.exc_info()
        logging.error(e)
        logging.error(traceback.format_exc())
        logging.error('-' * 60)

    if not args.quiet:
        logging.info("cf row processor ended")
