import json
import sys
import singer

from singer import metadata
from tap_intacct.discover import discover_streams
from tap_intacct.sync import sync_stream
from tap_intacct import s3

LOGGER = singer.get_logger()

REQUIRED_CONFIG_KEYS = ["start_date", "bucket", "company_id"]

def do_discover(config):
    LOGGER.info("Starting discover")
    streams = discover_streams(config)
    if not streams:
        raise Exception("No streams found")
    catalog = {"streams": streams}
    json.dump(catalog, sys.stdout, indent=2)
    LOGGER.info("Finished discover")


def stream_is_selected(mdata):
    # Select broken in current implementation. Not needed since we want to get all objects from S3 (our object filtering is at the DDS level)
    return True # mdata.get((), {}).get('selected', False)


def do_sync(config, catalog, state):
    LOGGER.info('Starting sync.')
    target = config.get("only_stream")
    print(target)

    for stream in catalog['streams']:
        stream_name = stream['tap_stream_id']
        mdata = metadata.to_map(stream['metadata'])

        if target and stream_name != target:
            LOGGER.info("%s: Skipping - not selected by config", stream_name)
            continue

        singer.write_state(state)
        key_properties = metadata.get(mdata, (), 'table-key-properties') or []
        singer.write_schema(stream_name, stream['schema'], key_properties)

        LOGGER.info("%s: Starting sync", stream_name)
        counter_value = sync_stream(config, state, stream)
        LOGGER.info("%s: Completed sync (%s rows)", stream_name, counter_value)

    LOGGER.info('Done syncing.')


@singer.utils.handle_top_exception(LOGGER)
def main():
    args = singer.utils.parse_args(REQUIRED_CONFIG_KEYS)
    config = args.config

    try:
        for page in s3.list_files_in_bucket(config['bucket']):
            break
        LOGGER.warning("I have direct access to the bucket without assuming the configured role.")
    except:
        s3.setup_aws_client(config)

    if args.discover:
        do_discover(args.config)
    else:
        streams = discover_streams(config)
        if not streams:
            raise Exception("No streams found")
        catalog = {"streams": streams}
        do_sync(config, catalog, args.state)


if __name__ == '__main__':
    main()
