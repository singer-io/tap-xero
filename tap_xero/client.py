from base64 import b64encode
import re
import json
import decimal
import sys
import math
from os.path import join
from datetime import datetime, date, time, timedelta
import requests
from singer.utils import strftime, strptime_to_utc
import six
import pytz
import backoff
import singer

LOGGER = singer.get_logger()

BASE_URL = "https://api.xero.com/api.xro/2.0"


class XeroError(Exception):
    def __init__(self, message=None, response=None):
        super().__init__(message)
        self.message = message
        self.response = response


class XeroBadRequestError(XeroError):
    pass


class XeroUnauthorizedError(XeroError):
    pass


class XeroForbiddenError(XeroError):
    pass


class XeroNotFoundError(XeroError):
    pass


class XeroPreConditionFailedError(XeroError):
    pass


class XeroTooManyError(XeroError):
    pass


class XeroTooManyInMinuteError(XeroError):
    pass


class XeroInternalError(XeroError):
    pass


class XeroNotImplementedError(XeroError):
    pass


class XeroNotAvailableError(XeroError):
    pass


ERROR_CODE_EXCEPTION_MAPPING = {
    400: {
        "raise_exception": XeroBadRequestError,
        "message": "A validation exception has occurred."
    },
    401: {
        "raise_exception": XeroUnauthorizedError,
        "message": "Invalid authorization credentials."
    },
    403: {
        "raise_exception": XeroForbiddenError,
        "message": "User doesn't have permission to access the resource."
    },
    404: {
        "raise_exception": XeroNotFoundError,
        "message": "The resource you have specified cannot be found."
    },
    412: {
        "raise_exception": XeroPreConditionFailedError,
        "message": "One or more conditions given in the request header fields were invalid."
    },
    429: {
        "raise_exception": XeroTooManyError,
        "message": "The API rate limit for your organisation/application pairing has been exceeded"
    },
    500: {
        "raise_exception": XeroInternalError,
        "message": "An unhandled error with the Xero API. Contact the Xero API team if problems persist."
    },
    501: {
        "raise_exception": XeroNotImplementedError,
        "message": "The method you have called has not been implemented."
    },
    503: {
        "raise_exception": XeroNotAvailableError,
        "message": "API service is currently unavailable."
    }
}


def parse_date(value):
    # Xero datetimes can be .NET JSON date strings which look like
    # "/Date(1419937200000+0000)/"
    # https://developer.xero.com/documentation/api/requests-and-responses
    pattern = r'Date\((\-?\d+)([-+])?(\d+)?\)'
    match = re.search(pattern, value)

    iso8601pattern = r'((\d{4})-([0-2]\d)-0?([0-3]\d)T([0-5]\d):([0-5]\d):([0-6]\d))'

    if not match:
        iso8601match = re.search(iso8601pattern, value)
        if iso8601match:
            try:
                return strptime_to_utc(value)
            except Exception:
                return None
        else:
            return None

    millis_timestamp, offset_sign, offset = match.groups()
    if offset:
        if offset_sign == '+':
            offset_sign = 1
        else:
            offset_sign = -1
        offset_hours = offset_sign * int(offset[:2])
        offset_minutes  = offset_sign * int(offset[2:])
    else:
        offset_hours = 0
        offset_minutes = 0

    return datetime.utcfromtimestamp((int(millis_timestamp) / 1000)) \
        + timedelta(hours=offset_hours, minutes=offset_minutes)


def _json_load_object_hook(_dict):
    """Hook for json.parse(...) to parse Xero date formats."""
    # This was taken from the pyxero library and modified
    # to format the dates according to RFC3339
    for key, value in _dict.items():
        if isinstance(value, six.string_types):
            value = parse_date(value)
            if value:
                # NB> Pylint disabled because, regardless of idioms, this is more explicit than isinstance.
                if type(value) is date: # pylint: disable=unidiomatic-typecheck
                    value = datetime.combine(value, time.min)
                value = value.replace(tzinfo=pytz.UTC)
                _dict[key] = strftime(value)
    return _dict

def update_config_file(config, config_path):
    with open(config_path, 'w') as config_file:
        json.dump(config, config_file, indent=2)

def is_not_status_code_fn(status_code):
    def gen_fn(exc):
        if getattr(exc, 'response', None) and getattr(exc.response, 'status_code', None) and exc.response.status_code not in status_code:
            return True
        # Retry other errors up to the max
        return False
    return gen_fn

def retry_after_wait_gen():
    while True:
        # This is called in an except block so we can retrieve the exception
        # and check it.
        exc_info = sys.exc_info()
        resp = exc_info[1].response
        sleep_time_str = resp.headers.get('Retry-After')
        LOGGER.info("API rate limit exceeded -- sleeping for %s seconds", sleep_time_str)
        yield math.floor(float(sleep_time_str))

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

        if resp.status_code != 200:
            raise_for_error(resp)
        else:
            resp = resp.json()

            # Write to config file
            config['refresh_token'] = resp["refresh_token"]
            update_config_file(config, config_path)
            self.access_token = resp["access_token"]
            self.tenant_id = config['tenant_id']


    @backoff.on_exception(backoff.expo, (json.decoder.JSONDecodeError, XeroInternalError), max_tries=3)
    @backoff.on_exception(retry_after_wait_gen, XeroTooManyInMinuteError, giveup=is_not_status_code_fn([429]), jitter=None, max_tries=3)
    def check_platform_access(self, config, config_path):

        # Validating the authentication of the provided configuration
        self.refresh_credentials(config, config_path)

        headers = {
            "Authorization": "Bearer " + self.access_token,
            "Xero-Tenant-Id": self.tenant_id,
            "Content-Type": "application/json"
        }

        # Validating the authorization of the provided configuration
        currencies_url = join(BASE_URL, "Currencies")
        request = requests.Request("GET", currencies_url, headers=headers)
        response = self.session.send(request.prepare())

        if response.status_code != 200:
            raise_for_error(response)


    @backoff.on_exception(backoff.expo, (json.decoder.JSONDecodeError, XeroInternalError), max_tries=3)
    @backoff.on_exception(retry_after_wait_gen, XeroTooManyInMinuteError, giveup=is_not_status_code_fn([429]), jitter=None, max_tries=3)
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

        if response.status_code != 200:
            raise_for_error(response)
            return None
        else:
            response_meta = json.loads(response.text,
                                    object_hook=_json_load_object_hook,
                                    parse_float=decimal.Decimal)
            response_body = response_meta.pop(xero_resource_name)
            return response_body


def raise_for_error(resp):
    try:
        resp.raise_for_status()
    except (requests.HTTPError, requests.ConnectionError) as error:
        try:
            error_code = resp.status_code

            # Handling status code 429 specially since the required information is present in the headers
            if error_code == 429:
                resp_headers = resp.headers
                api_rate_limit_message = ERROR_CODE_EXCEPTION_MAPPING[429]["message"]
                message = "HTTP-error-code: 429, Error: {}. Please retry after {} seconds".format(api_rate_limit_message, resp_headers.get("Retry-After"))

                #Raise XeroTooManyInMinuteError exception if minute limit is reached
                if resp_headers.get("X-Rate-Limit-Problem") == 'minute':
                    raise XeroTooManyInMinuteError(message, resp) from None
            # Handling status code 403 specially since response of API does not contain enough information
            elif error_code in (403, 401):
                api_message = ERROR_CODE_EXCEPTION_MAPPING[error_code]["message"]
                message = "HTTP-error-code: {}, Error: {}".format(error_code, api_message)
            else:
                # Forming a response message for raising custom exception
                try:
                    response_json = resp.json()
                except Exception:
                    response_json = {}

                message = "HTTP-error-code: {}, Error: {}".format(
                    error_code,
                    response_json.get(
                        "error", response_json.get(
                            "Title", response_json.get(
                                "Detail", ERROR_CODE_EXCEPTION_MAPPING.get(
                                    error_code, {}).get("message", "Unknown Error")
                                ))))

            exc = ERROR_CODE_EXCEPTION_MAPPING.get(error_code, {}).get("raise_exception", XeroError)
            raise exc(message, resp) from None

        except (ValueError, TypeError):
            raise XeroError(error) from None
