from base64 import b64encode
import json
import decimal
from os.path import join
from datetime import datetime, date, time
import requests
import xero.utils
from singer.utils import strftime
import six
import pytz

BASE_URL = "https://api.xero.com/api.xro/2.0"


def _json_load_object_hook(_dict):
    """Hook for json.parse(...) to parse Xero date formats."""
    # This was taken from the pyxero library and modified
    # to format the dates according to RFC3339
    for key, value in _dict.items():
        if isinstance(value, six.string_types):
            value = xero.utils.parse_date(value)
            if value:
                if isinstance(value, date):
                    value = datetime.combine(value, time.min)
                value = value.replace(tzinfo=pytz.UTC)
                _dict[key] = strftime(value)
    return _dict

def update_config_file(config, config_path):
    with open(config_path, 'w') as config_file:
        json.dump(config, config_file, indent=2)

class XeroClient():
    def __init__(self, config):
        self.session = requests.Session()
        self.user_agent = config.get("user_agent")
        self.tenant_id = None
        self.access_token = None

    def refresh_credentials(self, config, config_path):

        header_token = b64encode((config["client_id"] + ":" + config["client_secret"]).encode('utf-8'))

        headers = {
            "Authorization": "Basic " + header_token.decode('utf-8'),
            "Content-Type": "application/x-www-form-urlencoded"
        }

        post_body = {
            "grant_type": "refresh_token",
            "refresh_token": config["refresh_token"],
        }
        resp = self.session.post("https://identity.xero.com/connect/token", headers=headers, data=post_body)
        resp.raise_for_status()
        resp = resp.json()

        # Write to config file
        config['refresh_token'] = resp["refresh_token"]
        update_config_file(config, config_path)
        self.access_token = resp["access_token"]
        self.tenant_id = config['tenant_id']

    def filter(self, tap_stream_id, since=None, **params):
        xero_resource_name = tap_stream_id.title().replace("_", "")
        url = join(BASE_URL, xero_resource_name)
        headers = {"Accept": "application/json",
                   "Authorization": "Bearer " + self.access_token,
                   "Xero-tenant-id": self.tenant_id}
        if self.user_agent:
            headers["User-Agent"] = self.user_agent
        if since:
            headers["If-Modified-Since"] = since

        request = requests.Request("GET", url, headers=headers, params=params)
        response = self.session.send(request.prepare())
        response.raise_for_status()
        response_meta = json.loads(response.text,
                                   object_hook=_json_load_object_hook,
                                   parse_float=decimal.Decimal)
        response_body = response_meta.pop(xero_resource_name)
        return response_body
