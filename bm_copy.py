#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
bm_copy

Author: Michael P Laing

A preview version from the nyt⨍aбrik 'rabbit_helpers' framework.

Apache License Version 2.0, January 2004 http://www.apache.org/licenses/

"""
import os
import sys
import json
import uuid
import signal
import argparse
import traceback
from time import time
import multiprocessing
from datetime import datetime
from collections import deque
from threading import Event, Lock, Thread
from multiprocessing import Manager, Process

from pyev import Loop
from cassandra.cluster import Cluster

from cassandra.policies import (
    RetryPolicy,
    RoundRobinPolicy,
    TokenAwarePolicy,
    DCAwareRoundRobinPolicy,
    DowngradingConsistencyRetryPolicy
)

import logging

LOG_FORMAT = (
    '%(levelname) -10s %(asctime)s %(name) -30s %(process)d '
    '%(funcName) -35s %(lineno) -5d: %(message)s'
)

logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
logger = logging.getLogger(__name__)

JSON_DUMPS_ARGS = {'ensure_ascii': False, 'indent': 4}
TOKEN_HI = 2 ** 63 - 1
TOKEN_LO = -2 ** 63
TOKEN_SPAN = TOKEN_HI - TOKEN_LO


class CopyService(object):

    def __init__(self, args, tokens):
        self._args = {
            "source": {
                k[7:]: v for k, v in args.__dict__.items()
                if k[:7] == "source_"
            },
            "dest": {
                k[5:]: v for k, v in args.__dict__.items()
                if k[:5] == "dest_"
            }
        }

        self._source_fetch_size = args.source_fetch_size
        self._dest_concurrency = args.dest_concurrency

        self._tokens = tokens

        self._cql_cluster = {}
        self._cql_session = {}
        self._future = {}

        self._finished_event = Event()
        self._lock = Lock()
        self._concurrency = 0
        self._stopped = False
        self._page = 0
        self._row_count = 0
        self._rows = deque()

        self._start_time = 0
        self._stop_time = 0
        self._os_times_start = None
        self._os_times_stop = None

        self._query = {}

        self._stmt = {
            'source': u""" -- these ?'s will be filled in automatically
                SELECT *
                FROM identification.entitylookup
                WHERE TOKEN(name) > ? AND TOKEN(name) <= ?
            """,
            'dest': u"""  -- these ?'s must be mapped by 'map_fields' below
                insert into entity_lookup (name, value, entity_id) values(?, ?, ?)
            """
        }

    def map_fields(self, source_row):
        return (
            source_row.name,
            source_row.value,
            source_row.entity_id
        )

    def update_or_finish(self, _):
        logger.debug(
            "len(self._rows): {0}; "
            "self._concurrency: {1}; ".format(
                len(self._rows),
                self._concurrency
            )
        )

        try:
            self._row_count += 1
            self._concurrency -= 1

            # is work available or queued?
            if self._future['source'].has_more_pages or self._rows:
                if (  # is the current row within the latest page?
                    (self._row_count - 1)
                    /
                    self._source_fetch_size
                    + 1
                    > self._page
                ):  # if so, prefetch the next page, overlapping processing
                    logger.info(
                        'page: {0}; row_count: {1}'.format(
                            self._page, self._row_count
                        )
                    )

                    self._page += 1

                    # if work is available, start fetching it
                    if self._future['source'].has_more_pages:
                        self._future['source'].start_fetching_next_page()
                if self._rows:  # work is queued
                    self.update_dest_table()
            elif self._concurrency:  # work is in progress
                pass
            else:  # no work left
                self.finish()
        except Exception as exc:
            self.stop_and_raise(exc)

    def update_dest_table(self):
        with self._lock:
            while self._rows:
                logger.debug(
                    "len(self._rows): {0}; "
                    "self._concurrency: {1}; "
                    "self._dest_concurrency: {2}".format(
                        len(self._rows),
                        self._concurrency,
                        self._dest_concurrency
                    )
                )

                if self._concurrency >= self._dest_concurrency:
                    break
                else:
                    self._concurrency += 1
                    row = self._rows.pop()
                    subvars = self.map_fields(row)

                    self._future['dest'] = self._cql_session[
                        'dest'
                    ].execute_async(self._query['dest'], subvars)

                    self._future['dest'].add_callback(self.update_or_finish)
                    self._future['dest'].add_errback(self.stop_and_raise)

    def stop_and_raise(self, exc):
        error_msg = 'traceback: {}'.format(traceback.format_exc(exc))
        logger.error(error_msg)
        self.stop()
        raise exc

    def more_rows_or_finish(self, new_rows):
        logger.debug(
            "len(new_rows): {0}; "
            "len(self._rows): {1}; "
            "self._concurrency: {2}; ".format(
                len(new_rows),
                len(self._rows),
                self._concurrency
            )
        )

        try:
            if new_rows:  # new work has arrived
                self._rows.extend(new_rows)
                self.update_dest_table()
            elif self._rows:  # work is queued
                self.update_dest_table()
            elif self._concurrency:  # work is in progress
                pass
            else:  # no work left
                self.finish()
        except Exception as exc:
            self.stop_and_raise(exc)

    def select_from_source_table(self):
        self._start_time = time()
        self._os_times_start = os.times()

        try:
            logger.debug("self._tokens: {}".format(self._tokens))

            self._future['source'] = self._cql_session[
                'source'
            ].execute_async(self._query['source'], self._tokens)

            self._future['source'].add_callback(self.more_rows_or_finish)
            self._future['source'].add_errback(self.stop_and_raise)
        except Exception as exc:
            self.stop_and_raise(exc)

    def stop(self):
        if self._stopped:
            return

        self._stop_time = time()
        self._os_times_stop = os.times()

        logger.info("Stopping service.")

        for source_or_dest in ['source', 'dest']:
            if self._cql_cluster[source_or_dest]:
                try:
                    self._cql_cluster[source_or_dest].shutdown()
                except Exception as exc:
                    error_msg = traceback.format_exc(exc)

                    logger.info(
                        "Exception on cql_cluster.shutdown(): {0}".format(
                            error_msg
                        )
                    )

                    raise

        logger.info("Stopped.")

    def connection(self, source_or_dest):
        params = self._args[source_or_dest]
        logger.debug(
            "source_or_dest: {0}; params: {1}".format(
                source_or_dest,
                params))

        try:
            # defaults
            load_balancing_policy = RoundRobinPolicy()
            retry_policy = RetryPolicy()

            if params['dc_aware']:
                load_balancing_policy = DCAwareRoundRobinPolicy(
                    self._local_dc,
                    used_hosts_per_remote_dc=self._remote_dc_hosts
                )

            if params['token_aware']:
                load_balancing_policy = TokenAwarePolicy(
                    load_balancing_policy
                )

            if params['retry']:
                retry_policy = DowngradingConsistencyRetryPolicy()

            self._cql_cluster[source_or_dest] = Cluster(
                params['cql_host_list'],
                load_balancing_policy=load_balancing_policy,
                default_retry_policy=retry_policy
            )

            self._cql_session[source_or_dest] = self._cql_cluster[
                source_or_dest
            ].connect()

            if 'fetch_size' in params:
                self._cql_session[source_or_dest].default_fetch_size = params[
                    'fetch_size'
                ]

            self._query[source_or_dest] = self._cql_session[
                source_or_dest
            ].prepare(self._stmt[source_or_dest])
        except Exception as exc:
            error_msg = 'Cassandra init error; traceback: {}'.format(
                traceback.format_exc(exc)
            )

            logger.error(error_msg)
            self.stop()
            raise

        logger.info('Connected to Cassandra {}'.format(source_or_dest))

    def finish(self):
        logger.info("Finished")
        self._finished_event.set()

    def interrupt(self, signal_watcher, libev_events):
        logger.info("Interrupt")
        self.finish()

    def set_interrupt(self):
        ioloop = Loop()
        sigint_watcher = ioloop.signal(signal.SIGINT, self.interrupt)
        sigint_watcher.start()
        ioloop.start()

    def enable_interrupt(self):
        t = Thread(target=self.set_interrupt)
        t.daemon = True
        t.start()

    def run(self):
        logger.info("Starting service")
        self.connection('source')
        self.connection('dest')
        self.enable_interrupt()
        self.select_from_source_table()
        self._finished_event.wait()
        logger.info("Stopping service")


def main(args, worker_index, tokens, results):
    logger.info("Initializing...")
    exitcode = 0

    try:
        service = CopyService(args, tokens)
        service.run()
    except KeyboardInterrupt:
        logger.info("Service terminated by SIGINT.")
    except Exception as exc:
        error_msg = traceback.format_exc(exc)
        logger.error("Runtime exception: {0}".format(error_msg))
        exitcode = 1

    service.stop()
    logger.info("Terminated.")

    if service._row_count:
        elapsed = service._stop_time - service._start_time
        rate = int(service._row_count // elapsed)
    else:
        elapsed = 0
        rate = 0

    os_times_diff = map(
        lambda stop, start: stop - start,
        service._os_times_stop,
        service._os_times_start
    )

    results[worker_index] = {  # add results to the shared special dict
        'row_count': service._row_count,
        'start_time': service._start_time,
        'stop_time': service._stop_time,
        'elapsed': elapsed,
        'rate': rate,
        'os_times_stop': service._os_times_stop,
        'os_times_start': service._os_times_start,
        'os_times_diff': os_times_diff
    }

    sys.exit(exitcode)


def print_results(results):
    print("\nWorker  Rows      Elapsed   Rows/sec")

    for worker_index in sorted(results.keys()):
        r = results[worker_index]

        print(
            "{0:6d}{1:10d}{2:9.3f}{3:11d}".format(
                worker_index,
                r['row_count'],
                r['elapsed'],
                r['rate']
            )
        )

    print("CPU: {:4.0f}%".format(results[99]['cpu'] * 100))


def print_arguments(args):
    print("arguments:")

    for k, v in sorted(args.__dict__.items()):
        print("    {0}: {1}".format(k, v))


def print_json(args, results):
    output = {
        "uuid": str(uuid.uuid1()),
        "timestamp": datetime.utcnow().isoformat() + 'Z',
        "args": args.__dict__,
        "results": results
    }

    print(json.dumps(output, **JSON_DUMPS_ARGS))


def analyze_results(results, os_times_stop, os_times_start):
    worker_indices = [worker_index for worker_index in results.keys()]

    row_count = sum([
        results[worker_index]['row_count']
        for worker_index in worker_indices
    ])

    start_time = min([
        results[worker_index]['start_time']
        for worker_index in worker_indices
        if results[worker_index]['start_time'] != 0
    ])

    stop_time = max([
        results[worker_index]['stop_time']
        for worker_index in worker_indices
        if results[worker_index]['start_time'] != 0
    ])

    if row_count:
        elapsed = stop_time - start_time
        rate = int(row_count // elapsed)
    else:
        elapsed = 0
        rate = 0

    os_times_diff = map(
        lambda stop, start: stop - start,
        os_times_stop,
        os_times_start
    )

    compute = (os_times_diff[2] + os_times_diff[3])
    cpu = compute / os_times_diff[4]

    results[99] = {  # add summary results
        'row_count': row_count,
        'start_time': start_time,
        'stop_time': stop_time,
        'elapsed': elapsed,
        'rate': rate,
        'cpu': cpu,
        'os_times_stop': os_times_stop,
        'os_times_start': os_times_start,
        'os_times_diff': os_times_diff
    }

    return {k: v for k, v in results.items()}  # return a standard dict


def multiprocess(args):
    token_list = range(
        TOKEN_LO, TOKEN_HI, TOKEN_SPAN / args.worker_count + 1
    )

    token_list.append(TOKEN_HI)

    token_tuples = [
        (token_list[i], token_list[i + 1])
        for i, n in enumerate(token_list[:-1])
    ]

    multiprocessing.log_to_stderr().setLevel(logging.INFO)
    manager = Manager()
    results = manager.dict()  # create a special shared dict to gather results

    workers = [
        Process(
            target=main,
            args=(args, worker_index, token_tuples[worker_index], results)
        )

        for worker_index in range(args.worker_count)
    ]

    os_times_start = os.times()

    for worker in workers:
        worker.start()

    for worker in workers:
        worker.join()

    os_times_stop = os.times()
    exitcode = 0

    for worker in workers:
        if worker.exitcode:
            exitcode = worker.exitcode  # fail fast

    # transform the special dict
    results_dict = analyze_results(results, os_times_stop, os_times_start)

    if args.json_output:
        print_json(args, results_dict)
    else:
        print_arguments(args)
        print_results(results_dict)

    return(exitcode)


if __name__ == "__main__":
    description = """
    Push messages from a RabbitMQ queue into a Cassandra table.

    """

    parser = argparse.ArgumentParser(description=description)

    parser.add_argument(
        "--source_cql_host_list",
        default=['localhost'],
        nargs='*',
        metavar='CQL_HOST',
        help="source: the initial cql hosts to contact (default=['localhost'])"
    )

    parser.add_argument(
        "--source_local_dc",
        default='',
        help="source: the local datacenter (default='')"
    )

    parser.add_argument(
        "--source_remote_dc_hosts",
        type=int,
        default=0,
        help=(
            "source: the number of hosts to be connected to as remote hosts "
            "(default=0)"
        )
    )

    parser.add_argument(
        "--source_dc_aware",
        dest="source_dc_aware",
        action="store_true",
        help="source: favor hosts in the local datacenter (default=False)"
    )

    parser.add_argument(
        "--source_token_aware",
        dest="source_token_aware",
        action="store_true",
        help=(
            "source: route queries to known replicas by murmur3-hashed "
            "routing_keys (default=False)"
        )
    )

    parser.add_argument(
        "--source_retry",
        dest="source_retry",
        action="store_true",
        help="source: downgrade consistency level and retry (default=False)"
    )

    parser.add_argument(
        "--source_fetch_size",
        type=int,
        default=1000,
        help="source: the number of rows to fetch in each page (default=1000)"
    )

    parser.add_argument(
        "--dest_cql_host_list",
        default=['localhost'],
        nargs='*',
        metavar='CQL_HOST',
        help="dest: the initial cql hosts to contact (default=['localhost'])"
    )

    parser.add_argument(
        "--dest_local_dc",
        default='',
        help="dest: the local datacenter (default='')"
    )

    parser.add_argument(
        "--dest_remote_dc_hosts",
        type=int,
        default=0,
        help=(
            "dest: the number of hosts to be connected to as remote hosts "
            "(default=0)"
        )
    )

    parser.add_argument(
        "--dest_dc_aware",
        dest="dest_dc_aware",
        action="store_true",
        help="dest: favor hosts in the local datacenter (default=False)"
    )

    parser.add_argument(
        "--dest_token_aware",
        dest="dest_token_aware",
        action="store_true",
        help=(
            "dest: route queries to known replicas by murmur3-hashed "
            "routing_keys (default=False)"
        )
    )

    parser.add_argument(
        "--dest_retry",
        dest="dest_retry",
        action="store_true",
        help="dest: downgrade consistency level and retry (default=False)"
    )

    parser.add_argument(
        "--dest_concurrency",
        type=int,
        default=10,
        help=(
            "the number of updates to launch concurrently in each process "
            "using callback chaining (default=10)"
        )
    )

    parser.add_argument(
        "-w",
        "--worker_count",
        type=int,
        default=2,
        help=(
            "the number of asynchronous worker processes to spawn - each "
            "will handle an equal range of partition key tokens (default=2)"
        )
    )

    parser.add_argument(
        "-j",
        "--json_output",
        dest="json_output",
        action="store_true",
        help=(
            "suppress formatted printing; output args and results as json "
            "(default=False)"
        )
    )

    parser.set_defaults(source_dc_aware=False)
    parser.set_defaults(source_token_aware=False)
    parser.set_defaults(source_retry=False)
    parser.set_defaults(dest_dc_aware=False)
    parser.set_defaults(dest_token_aware=False)
    parser.set_defaults(dest_retry=False)
    parser.set_defaults(json_output=False)
    args = parser.parse_args(args=sys.argv[1:])
    sys.exit(multiprocess(args))