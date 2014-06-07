#!/usr/bin/env python
# -*- coding: utf-8 -*-
import argparse
import base64
import json
from boto.sqs.connection import SQSConnection
from boto.sqs.message import Message

__author__ = 'mvalle'

DEFAULT_TOKEN_HI = 2 ** 63 - 1
DEFAULT_TOKEN_LO = -2 ** 63
AWS_ACCESS_ID = "AKIAJBWUTS5FGZGEWN2Q"
AWS_ACCESS_SECRET_KEY = "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"

def calculate_ranges(token_hi, token_lo, number_of_pieces):
    step = ((token_hi - token_lo) / number_of_pieces) + 1
    tr1 = range(token_lo, token_hi, step)  # intermediate points
    tr2 = [(t, t + 1) for t in tr1[1:]]  # add adjacent points
    tr3 = [t for st in tr2 for t in st]  # flatten
    tr4 = [token_lo] + tr3 + [token_hi]  # add end points
    token_ranges = [tr4[i:i + 2] for i in range(0, len(tr4), 2)]  # make pairs
    return token_ranges

messages = []
msg_count = 0
def send_sqs_msg(msg, boto_queue, use_batch=True):
    if use_batch:
        global messages, msg_count
        messages.append((str(msg_count), base64.b64encode(json.dumps(msg)), 0))
        msg_count += 1
        if msg_count % 10 == 0:
            boto_queue.write_batch(messages)
            messages = []
    else:
        m = Message()
        m.set_body(json.dumps(msg))
        boto_queue.write(m)

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-th", "--token-hi", type=int, default=DEFAULT_TOKEN_HI, help="High bound for the range")
    parser.add_argument("-tl", "--token-lo", type=int, default=DEFAULT_TOKEN_LO, help="Low bound for the range")
    parser.add_argument("-n", "--number-of-pieces", type=int, default=1024, help="How many ranges do you want?")
    parser.add_argument("-q", "--queue", type=str, default=None, help="SQS Queue. If informed, will send the ranges to a SQS queue.")

    args = parser.parse_args()

    ranges = calculate_ranges(args.token_hi, args.token_lo, args.number_of_pieces)

    if args.queue is not None:
        print "Connection to queue '%s'..." % args.queue
        conn = SQSConnection(AWS_ACCESS_ID, AWS_ACCESS_SECRET_KEY)
        boto_queue = conn.create_queue(args.queue)
        print "Connected! "

    for r in ranges:
        print "Sending %d\t%d to %s" % (r[0], r[1], args.queue)
        if args.queue is not None:
            msg = {'start': r[0], 'end': r[1]}
            send_sqs_msg(msg, boto_queue)


