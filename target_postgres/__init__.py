#!/usr/bin/env python3

import argparse
import io
import os
import sys
import json
import threading
import http.client
import urllib
from datetime import datetime
import collections
from tempfile import TemporaryFile

import pkg_resources
from jsonschema.validators import Draft4Validator
import singer
from target_postgres.db_sync import DbSync

logger = singer.get_logger()


def emit_state(state):
    if state is not None:
        line = json.dumps(state)
        logger.debug('Emitting state {}'.format(line))
        sys.stdout.write("{}\n".format(line))
        sys.stdout.flush()


def persist_lines(config, lines):
    state = None
    schemas = {}
    key_properties = {}
    headers = {}
    validators = {}
    csv_files_to_load = {}
    row_counts = {}
    flushed_row_counts = {}
    stream_to_sync = {}
    primary_key_exists = {}
    batch_size = config['batch_size'] if 'batch_size' in config else 10000

    now = datetime.now().strftime('%Y%m%dT%H%M%S')

    # Loop over lines from stdin
    for line in lines:
        try:
            object = json.loads(line)
        except json.decoder.JSONDecodeError:
            logger.error("Unable to parse:\n{}".format(line))
            raise

        if 'type' not in object:
            raise Exception("Line is missing required key 'type': {}".format(line))
        object_type = object['type']

        if object_type == 'RECORD':
            # Get schema for this record's stream
            if 'stream' not in object:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = object['stream']

            if stream not in schemas:
                raise Exception(
                    "A record for stream {} was encountered before a corresponding schema".format(object['stream']))

            # Validate record
            validators[stream].validate(object['record'])
            sync = stream_to_sync[stream]

            primary_key_string = sync.record_primary_key_string(object['record'])
            if stream not in primary_key_exists:
                primary_key_exists[stream] = {}

            # flush prepared records before a new row with the PK existing in this bath
            if primary_key_string and primary_key_string in primary_key_exists[stream]:
                logger.debug("Encountered a PK value %s which exists in the bath, flushing", primary_key_string)
                flush_records(stream, csv_files_to_load, row_counts, primary_key_exists,
                              sync, state, flushed_row_counts)

            # add the record to a temp file
            csv_line = sync.record_to_csv_line(object['record'])
            csv_files_to_load[stream].write(bytes(csv_line + '\n', 'UTF-8'))
            row_counts[stream] += 1

            if primary_key_string:
                primary_key_exists[stream][primary_key_string] = True

            # in case we reached the bath size limit
            if row_counts[stream] >= batch_size:
                flush_records(stream, csv_files_to_load, row_counts, primary_key_exists,
                              sync, state, flushed_row_counts)

            # state = None
        elif object_type == 'STATE':
            logger.debug('Setting state to {}'.format(object['value']))
            state = object['value']

        elif object_type == 'SCHEMA':
            if 'stream' not in object:
                raise Exception("Line is missing required key 'stream': {}".format(line))
            stream = object['stream']

            schemas[stream] = object
            validators[stream] = Draft4Validator(object['schema'])
            if 'key_properties' not in object:
                raise Exception("key_properties field is required")
            key_properties[stream] = object['key_properties']
            stream_to_sync[stream] = DbSync(config, object)
            stream_to_sync[stream].create_schema_if_not_exists()
            stream_to_sync[stream].sync_table()
            row_counts[stream] = 0
            flushed_row_counts[stream] = 0
            csv_files_to_load[stream] = TemporaryFile(mode='w+b')

        elif object_type == 'ACTIVATE_VERSION':
            logger.debug('ACTIVATE_VERSION message')

        else:
            raise Exception("Unknown message type {} in message {}"
                            .format(object['type'], object))

    logger.info("Finished reading input, flushing remaining rows")
    for (stream_name, count) in row_counts.items():
        if count > 0:
            flush_records(stream_name, csv_files_to_load, row_counts, primary_key_exists,
                          stream_to_sync[stream_name], state, flushed_row_counts)

    logger.info("Processed records:")
    for (stream_name, count) in flushed_row_counts.items():
        logger.info("%s - %d", stream_name, count)

    return state


def flush_records(stream, csv_files_to_load, row_counts, primary_key_exists, sync, state, flushed_row_counts):
    stream_row_count = row_counts[stream]
    sync.load_csv(csv_files_to_load[stream], stream_row_count)
    row_counts[stream] = 0
    flushed_row_counts[stream] += stream_row_count

    primary_key_exists[stream] = {}
    csv_files_to_load[stream] = TemporaryFile(mode='w+b')
    logger.info("Flushed %d row to the stream %s", stream_row_count, stream)
    emit_state(state)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-c', '--config', help='Config file')
    args = parser.parse_args()

    if args.config:
        with open(args.config) as input:
            config = json.load(input)
    else:
        config = {}

    input = io.TextIOWrapper(sys.stdin.buffer, encoding='utf-8')
    state = persist_lines(config, input)

    emit_state(state)
    logger.debug("Exiting normally")


if __name__ == '__main__':
    main()
