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

def default_process(list_items, host):
    for item in list_items:
        print item['name'], item['value'], item['entity_id']

def do_nothing():
    pass

def task_done(result):
    workers_semaphore.release()
    logging.debug("\t***\tDequeueing finished task from process pool")
    sys.stdout.flush()

def parallel_process(host, workers, process_method, keyspace):
    def send():
        workers_semaphore.acquire()
        dump_list = list_items[:]
        list_items[:] = []
        logging.debug("\t***\tEnqueuing task on process pool")
        dump_pool.apply_async(process_method, args=[dump_list, host], callback=task_done)
        sys.stdout.flush()

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

    st = SimpleStatement("SELECT name, value, entity_id FROM entitylookup ", consistency_level=ConsistencyLevel.ALL)
    list_items = []    

    for row in session.execute(st):
        list_items.append({
            "name": row.name,
            "value": row.value,
            "entity_id": row.entity_id
        })
        if len(list_items) >= 100:
            send()

    if list_items:
        send()
    dump_pool.close()
    dump_pool.join()

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-H", "--host", default="localhost", help="Cassandra host")
    parser.add_argument("-w", "--workers", type=int, default=1, help="Amount of worker processes")
    parser.add_argument("-ks", "--keyspace", type=str, default="identification", help="Cassandra Origin Keyspace")
    parser.add_argument("-m", "--module", type=str, nargs=argparse.REMAINDER, default=None,
                        help="Python Module that contains the process method")

    args = parser.parse_args()

    workers_semaphore = Semaphore(args.workers * 10)

    try:
        parallel_process(args.host, args.workers, args.keyspace)
    except:
        logging.error("Exception in user code:")
        logging.error('-' * 60)
        e = sys.exc_info()
        logging.error(e)
        logging.error(traceback.format_exc())
        logging.error('-' * 60)
    logging.info("cf copier started with %d processes" % args.workers)
    try:
        process_method = default_process
        finalizer = do_nothing
        if args.module is not None:
            python_module = import_module(args.module[0])
            print "Module '%s' arguments: '%s' \n" % (args.module[0], str(args.module[1:]))
            if hasattr(python_module, 'validate_arguments'):
                python_module.validate_arguments(args, args.module[1:])
            if hasattr(python_module, 'process'):
                process_method = python_module.process
            if hasattr(python_module, 'finish'):
                finalizer = python_module.finish

        module_arguments = args.module[1:] if args.module is not None else None
        parallel_process(args.host, args.workers, args.keyspace, process_method, module_arguments, finalizer)

    except:
        logging.error("Exception in user code:")
        logging.error('-' * 60)
        e = sys.exc_info()
        logging.error(e)
        logging.error(traceback.format_exc())
        logging.error('-' * 60)

    logging.info("cf copier ended")
