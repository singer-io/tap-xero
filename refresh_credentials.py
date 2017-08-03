#!/usr/bin/env python3
from xero.auth import PartnerCredentials
import json

with open("config.json") as f:
    config = json.loads(f.read())

if "oauth_token" not in config:
    credentials = PartnerCredentials(config["consumer_key"],
                                     config["consumer_secret"],
                                     config["rsa_key"])
    print(credentials.url)
    verifier = input("verifier: ")
    credentials.verify(verifier)
else:
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

state = credentials.state
for k in ["oauth_expires_at", "oauth_authorization_expires_at", "verified"]:
    state.pop(k)
for k, v in state.items():
    config[k] = v
with open("config.json", "w") as f:
    f.write(json.dumps(config, indent=2))
    f.write("\n")
