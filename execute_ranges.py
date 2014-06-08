#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import json
from os import environ
import traceback
from boto.sqs.connection import SQSConnection
import sys
from bm_copy import multiprocess, add_bm_copy_arguments

__author__ = 'mvalle'

AWS_ACCESS_ID = environ['AWS_ACCESS_ID']
AWS_ACCESS_SECRET_KEY = environ['AWS_ACCESS_SECRET_KEY']


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-q", "--queue", type=str, default=None, help="SQS Queue. If informed, will send the ranges to a SQS queue.")

    args = add_bm_copy_arguments(parser)


    if args.queue is not None:
        print "Connection to queue '%s'..." % args.queue
        conn = SQSConnection(AWS_ACCESS_ID, AWS_ACCESS_SECRET_KEY)
        boto_queue = conn.create_queue(args.queue)
        print "Connected! "

    while True:
        m = boto_queue.read()
        if m is None:
            print "Finished processing ALL messages"
            break
        print "BEGIN PROCESSING - Message '%s'" % str(m.get_body())
        range_msg = json.loads(m.get_body())
        range = [range_msg['start'], range_msg['end']]
        try:
            args.token_hi = range_msg['end']
            args.token_lo = range_msg['start']
            result = multiprocess(args)
            if result == 0:
                print "ENDED PROCESSING - Message %s was processed ok and will be deleted" % str(range)
                boto_queue.delete_message(m)
            else:
                raise Exception("Message %s was processed with ERROR and will be kept in the queue" % str(range))
        except:
            print "Exception in user code:"
            print '-' * 60
            e = sys.exc_info()
            print str(e)
            print  traceback.format_exc()
            print '-' * 60


