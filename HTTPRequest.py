#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple

import requests
import time


class HTTPRequest(object):
    def __init__(self, delay=0, tries=3, user_agent=None):
        self._delay = delay
        self._tries = tries
        self._user_agent = user_agent

    # returns http status code 600 for a connection failure
    def get(self, url, additional_headers=None):
        remaining_tries = self._tries

        if additional_headers is None:
            additional_headers = {}

        additional_headers["User-Agent"] = self._user_agent

        while remaining_tries > 0:
            try:
                response = requests.get(url, headers=additional_headers)
                return response
            except:
                remaining_tries -= 1

                if self._delay > 0:
                    time.sleep(self._delay/1000)

        RequestError = namedtuple("response", ["status_code", "text"])
        return RequestError(status_code=600, text="Connection failure")
