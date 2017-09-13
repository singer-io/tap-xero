#!/usr/bin/env python3
from xero.auth import PartnerCredentials
from xero.exceptions import XeroUnauthorized
import json

with open("config.json") as f:
    config = json.loads(f.read())


def new_token():
    credentials = PartnerCredentials(config["consumer_key"],
                                     config["consumer_secret"],
                                     config["rsa_key"])
    print(credentials.url)
    verifier = input("verifier: ")
    credentials.verify(verifier)
    return credentials


def refreshed_token():
    credentials = PartnerCredentials(
        config["consumer_key"],
        config["consumer_secret"],
        config["rsa_key"],
        verified=True,
        oauth_token=config["oauth_token"],
        oauth_token_secret=config["oauth_token_secret"],
        oauth_session_handle=config["oauth_session_handle"],
    )
    credentials.refresh()
    return credentials

if "oauth_token" not in config:
    credentials = new_token()
else:
    try:
        credentials = refreshed_token()
    except XeroUnauthorized:
        credentials = new_token()

state = credentials.state
for k in ["oauth_expires_at", "oauth_authorization_expires_at", "verified"]:
    state.pop(k)
for k, v in state.items():
    config[k] = v
with open("config.json", "w") as f:
    f.write(json.dumps(config, indent=2))
    f.write("\n")
