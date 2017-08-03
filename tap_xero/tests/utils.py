import singer
import singer.bookmarks
import os
import json

LOGGER = singer.get_logger()

caught_records = {}
caught_bookmarks = []
caught_state = {}
caught_schema = {}
caught_pks = {}


def verify_environment_vars():
    req_vars = ["TAP_XERO_CONFIG_FILE"]
    missing_vars = [x for x in req_vars if not os.getenv(x)]
    if missing_vars:
       raise Exception("these environment variables are required: " +
                       ", ".join(missing_vars))


def get_test_config():
    with open(os.getenv("TAP_XERO_CONFIG_FILE")) as f:
        config = json.loads(f.read())
    config["start_date"] = "2001-01-01"
    return config


def metric_log(*args, **kwargs):
    pass
