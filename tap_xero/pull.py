import singer
from singer import metrics
from xero import Xero
from xero.auth import PartnerCredentials
from xero.exceptions import XeroException, XeroRateLimitExceeded
from xero.manager import Manager
import pendulum
import time
import datetime

LOGGER = singer.get_logger()

CREDENTIALS_KEYS = ["consumer_key",
                    "consumer_secret",
                    "rsa_key",
                    "oauth_token",
                    "oauth_token_secret",
                    "oauth_session_handle"]

# With MAX_CONSECUTIVE_RATE_LIMITS = 10 then the total
# sleeping time will be about a half hour.
MAX_CONSECUTIVE_RATE_LIMITS = 10
XERO_ORDERER = "UpdatedDateUTC ASC"


def get_xero_client(config):
    kwargs = dict((attr, config[attr]) for attr in CREDENTIALS_KEYS)
    kwargs["verified"] = True
    credentials = PartnerCredentials(**kwargs)
    xero = Xero(credentials, user_agent=config.get("user_agent"))
    if not hasattr(xero, "linkedtransactions"):
        # Xero API version 0.9 does not have this resources, so add it
        # ourselves. See http://tinyurl.com/y7w9bwhj and
        # http://tinyurl.com/y96mnm49
        setattr(xero,
                "linkedtransactions",
                Manager("LinkedTransactions",
                        credentials=credentials))
    return xero


def _request_with_timer(tap_stream_id, xero, options):
    xero_resource = getattr(xero, tap_stream_id.replace("_", ""))
    with metrics.http_request_timer(tap_stream_id) as timer:
        try:
            resp = xero_resource.filter(**options)
            timer.tags[metrics.Tag.http_status_code] = 200
            return resp
        except XeroException as e:
            timer.tags[metrics.Tag.http_status_code] = e.response.status_code
            raise


def make_request(tap_stream_id, xero, options={}, num_rate_limits=0):
    # https://developer.xero.com/documentation/auth-and-limits/xero-api-limits
    try:
        return _request_with_timer(tap_stream_id, xero, options)
    except XeroRateLimitExceeded as e:
        # rate_limit_type = e.response.headers["X-Rate-Limit-Problem"]
        # ^ Daily or Minute
        # But for now treat them the same
        num_rate_limits += 1
        if num_rate_limits > MAX_CONSECUTIVE_RATE_LIMITS:
            raise
        sleep_secs = 2**num_rate_limits
        LOGGER.debug("Rate limited, # {}, sleeping {} secs"
                     .format(num_rate_limits, sleep_secs))
        time.sleep(sleep_secs)
        return make_request(tap_stream_id, xero, options, num_rate_limits + 1)


class Puller(object):
    def __init__(self, config, state, tap_stream_id):
        self.config = config
        self.state = state
        self.tap_stream_id = tap_stream_id
        self.xero = get_xero_client(config)

    @property
    def _bookmark(self):
        if "bookmarks" not in self.state:
            self.state["bookmarks"] = {}
        if self.tap_stream_id not in self.state["bookmarks"]:
            self.state["bookmarks"][self.tap_stream_id] = {}
        return self.state["bookmarks"][self.tap_stream_id]

    def _set_last_updated(self, updated_at):
        if isinstance(updated_at, datetime.datetime):
            updated_at = updated_at.isoformat()
        self._bookmark["updated_at"] = updated_at

    def _update_start_state(self):
        if not self._bookmark.get("updated_at"):
            self._set_last_updated(self.config["start_date"])
        return pendulum.parse(self._bookmark["updated_at"])

    def yield_pages(self):
        raise NotImplemented()


class IncrementingPull(Puller):
    def yield_pages(self):
        start = self._update_start_state()
        now = datetime.datetime.utcnow()
        page = make_request(self.tap_stream_id, self.xero, dict(since=start))
        yield page
        self._set_last_updated(now)


class PaginatedPull(Puller):
    def _paginate(self,
                  options={},
                  first_page_num=1,
                  pagination_key="page",
                  get_next_page_num=lambda curr_page_num, _: curr_page_num + 1):
        num_pages_yielded = 0
        curr_page_num = first_page_num
        while True:
            if num_pages_yielded > 1e6:
                raise Exception("1 million pages doesn't seem realistic")
            options[pagination_key] = curr_page_num
            page = make_request(self.tap_stream_id, self.xero, options)
            if not page:
                break
            next_page_num = get_next_page_num(curr_page_num, page)
            yield page, next_page_num
            curr_page_num = next_page_num
            num_pages_yielded += 1

    def yield_pages(self):
        start = self._update_start_state()
        now = datetime.datetime.utcnow()
        first_num = self._bookmark.get("page") or 1
        options = dict(since=start, order=XERO_ORDERER)
        for page, next_num in self._paginate(options, first_page_num=first_num):
            self._bookmark["page"] = next_num
            yield page
        self._bookmark["page"] = None
        self._set_last_updated(now)


class JournalPull(PaginatedPull):
    """The Journals endpoint is a special case. It has its own way of ordering
    and paging the data. See
    https://developer.xero.com/documentation/api/journals"""
    pagination_key = "offset"

    def yield_pages(self):
        first_num = self._bookmark.get("journal_number") or 0
        next_page_fn = lambda _, page: page[-1]["JournalNumber"]
        for page, next_num in self._paginate(first_page_num=first_num,
                                             pagination_key="offset",
                                             get_next_page_num=next_page_fn):
            self._bookmark["journal_number"] = next_num
            yield page


class LinkedTransactionsPull(PaginatedPull):
    """The Linked Transactions endpoint is a special case. It supports
    pagination, but not the Modified At header, but the objects returned have
    the UpdatedDateUTC timestamp in them. Therefore we must always iterate over
    all of the data, but we can manually omit records based on the
    UpdatedDateUTC property."""
    def yield_pages(self):
        start = self._update_start_state()
        now = datetime.datetime.utcnow()
        first_num = self._bookmark.get("page") or 1
        for page, next_page_num in self._paginate(first_page_num=first_num):
            self._bookmark["page"] = next_page_num
            yield [x for x in page if x["UpdatedDateUTC"] >= start]
        self._bookmark["page"] = None
        self._set_last_updated(now)


class EverythingPull(Puller):
    def yield_pages(self):
        yield make_request(self.tap_stream_id, self.xero)
