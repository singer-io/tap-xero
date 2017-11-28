import boto3
import json
import backoff
import singer
from botocore.exceptions import ClientError as BotoClientError
from requests_oauthlib import OAuth1
from oauthlib.oauth1 import SIGNATURE_RSA
from xero.auth import PartnerCredentials

LOGGER = singer.get_logger()

# Behavior:
# If the key does not exist: NoSuchKey
# If not valid JSON: JSONDecodeError

BASE_KEYS = ["oauth_token", "oauth_token_secret"]
REFRESHABLE_KEYS = BASE_KEYS + ["oauth_session_handle"]


class CredentialsException(Exception):
    pass


def can_use_s3(config):
    return "oauth_s3_bucket" in config and "oauth_s3_path" in config


def _s3_obj(config):
    s3 = boto3.resource("s3")
    return s3.Object(config["oauth_s3_bucket"], config["oauth_s3_path"])


def download_from_s3(config):
    try:
        response = _s3_obj(config).get()
    except BotoClientError as ex:
        if ex.response['Error']['Code'] == "NoSuchKey":
            return None
        else:
            raise ex

    body = json.loads(response["Body"].read().decode("utf-8"))
    missing_keys = [k for k in REFRESHABLE_KEYS if k not in body]
    if missing_keys:
        raise CredentialsException("Keys missing from S3 file: " +
                                   str(missing_keys))
    return body


def build_oauth(config):
    return OAuth1(
        config["consumer_key"],
        client_secret=config["consumer_secret"],
        resource_owner_key=config["oauth_token"],
        resource_owner_secret=config["oauth_token_secret"],
        rsa_key=config["rsa_key"],
        signature_method=SIGNATURE_RSA,
    )


def _on_giveup(details):
    _, body = details["args"]
    LOGGER.error("Credentials could not be saved to S3. " +
                 "You will need to re-authorize the application.")


@backoff.on_exception(backoff.expo,
                      Exception,
                      max_tries=5,
                      on_giveup=_on_giveup,
                      factor=2)
def _upload(obj, body):
    obj.put(Body=body.encode())


def _write_to_s3(config):
    creds = {x: config[x] for x in REFRESHABLE_KEYS}
    _upload(_s3_obj(config), json.dumps(creds))


def refresh(config):
    LOGGER.info("Refreshing credentials")
    if not can_use_s3(config):
        raise CredentialsException("S3 not configured, refresh not supported")

    partner_creds = PartnerCredentials(
        config["consumer_key"],
        config["consumer_secret"],
        config["rsa_key"],
        verified=True,
        oauth_token=config["oauth_token"],
        oauth_token_secret=config["oauth_token_secret"],
        oauth_session_handle=config["oauth_session_handle"],
    )
    partner_creds.refresh()
    config["oauth_token"] = partner_creds.oauth_token
    config["oauth_token_secret"] = partner_creds.oauth_token_secret
    config["oauth_session_handle"] = partner_creds.oauth_session_handle
    _write_to_s3(config)

    return config
