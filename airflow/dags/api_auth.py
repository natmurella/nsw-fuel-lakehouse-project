import base64
import json
import os
from datetime import datetime, timezone
import uuid

import requests

def build_ts_string(**context) -> str:
    """
    Build a timestamp string in specific format:
    YYYYMMDD_HHMMSS (UTC).
    """
    logical_date = context["logical_date"]
    ts = logical_date.astimezone(timezone.utc).strftime("%Y%m%d_%H%M%S")
    return ts

def build_auth_header(api_key: str, api_secret: str) -> str:
    """
    Build the Basic Auth header value from API key and secret.
    spec: Base64 encoded value of api_key:api_secret. Eg 'Basic {Base64 encoded value of api_key:api_secret}'
    """
    token_bytes = f"{api_key}:{api_secret}".encode("utf-8")
    b64 = base64.b64encode(token_bytes).decode("ascii")
    return f"Basic {b64}"

def get_access_token(api_key: str, api_secret: str, base_url: str, auth_path: str) -> str:
    """
    Get OAuth2 access token
    """

    # prepare request
    url = base_url + auth_path
    auth_header = build_auth_header(api_key, api_secret)
    headers = {
        "Authorization": auth_header,
        "Accept": "application/json",
    }
    params = {
        "grant_type": "client_credentials",
    }

    # make request for toke
    response = requests.get(url, headers=headers, params=params, timeout=30)

    # evaluate reponse
    response.raise_for_status()

    try:
        data = response.json()
        access_token = data.get("access_token") or data.get("accessToken") or ""
    except ValueError:
        access_token = response.text
    
    access_token = access_token.strip().strip('"')
    if not access_token:
        raise RuntimeError("Could not extract access token from response.")
    
    return access_token


def call_new_fuel_prices(access_token: str, api_key: str, base_url: str, new_prices_path: str) -> dict:
    """
    Call the New Fuel Prices endpoint and return the JSON response.
    """

    # prepare request
    url = base_url + new_prices_path
    ts = datetime.now(timezone.utc).strftime("%d/%m/%Y %I:%M:%S %p")
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json; charset=utf-8",
        "Accept": "application/json",
        "apikey": api_key,
        "transactionid": str(uuid.uuid4()),
        "requesttimestamp": ts,
    }
    params = {
        "states": "NSW|TAS",  # v2 supports NSW & TAS; we stick to NSW for now
    }

    # make request
    response = requests.get(url, headers=headers, params=params, timeout=60)

    # evaluate response
    response.raise_for_status()
    data = response.json()

    return data
