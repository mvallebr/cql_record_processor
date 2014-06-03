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

def default_process(rows, host, module_arguments):
    print (str(rows))
    #for row in rows:
    #   print row['name'], row['value'], row['entity_id']

def task_done(result):
    workers_semaphore.release()
    logging.debug("\t***\tDequeueing finished task from process pool")
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
    logging.debug("\t***\tEnqueuing task on process pool")
    arguments = [process_method, rows]
    for arg in kargs:
        arguments.append(arg)

    dump_pool.apply_async(secure_process, args=arguments, callback=task_done)

def parallel_process(host, workers, step, keyspace, process_method, module_arguments):
    logging.info("parallel_dump started using %d workers" % workers)

    dump_pool = Pool(
        processes=workers,
        initializer=None,
        initargs=None,
        maxtasksperchild=None
    )  # start NUM_PROCESSES worker processes
    cluster = Cluster([host], port=9042, control_connection_timeout=60)
    session = cluster.connect(keyspace)
    session.default_timeout=100

    st = SimpleStatement("SELECT * FROM entitylookup limit 10", consistency_level=ConsistencyLevel.ALL)
    step_rows = []

    for row in session.execute(st):
        step_rows.append(row[:])
        if len(step_rows) >= step:
            rows = step_rows[:]
            add_task(dump_pool, process_method, rows, host, module_arguments)
            step_rows = []

    if step_rows:
        add_task(dump_pool, process_method, step_rows, host, module_arguments)

    logging.info("parallel_dump ended using %d workers" % workers)

    dump_pool.close()
    dump_pool.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", default="localhost", help="Cassandra host")
    parser.add_argument("-w", "--workers", type=int, default=1, help="Amount of worker processes")
    parser.add_argument("-ks", "--keyspace", type=str, default="identification", help="Cassandra Origin Keyspace")
    parser.add_argument("-s", "--step", type=int, default=100, help="Process STEP rows at a time")
    parser.add_argument("-m", "--module", type=str, nargs=argparse.REMAINDER, default=None,
                        help="Python Module that contains the process method")

    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO)
    workers_semaphore = Semaphore(args.workers)

    logging.info("cf row processor started with %d processes, step = %d" % (args.workers, args.step))
    try:
        process_method = default_process
        if args.module is not None:
            python_module = import_module(args.module[0])
            print "Module '%s' arguments: '%s' \n" % (args.module[0], str(args.module[1:]))
            if hasattr(python_module, 'validate_arguments'):
                python_module.validate_arguments(args, args.module[1:])
            if hasattr(python_module, 'process'):
                process_method = python_module.process

        module_arguments = args.module[1:] if args.module is not None else None
        parallel_process(args.host, args.workers, args.step, args.keyspace, process_method, module_arguments)

    except:
        logging.error("Exception in user code:")
        logging.error('-' * 60)
        e = sys.exc_info()
        logging.error(e)
        logging.error(traceback.format_exc())
        logging.error('-' * 60)

    logging.info("cf row processor ended")
