#!/usr/bin/env python
# -*- coding: utf-8 -*-
from collections import namedtuple

import requests
import time


class HTTPRequest(object):
    def __init__(self, delay=0, tries=3):
        self._delay = delay
        self._tries = tries

    # TODO: differentiate connection failure from gzip error
    def get(self, url, additional_headers=None):
        remaining_tries = self._tries

        while remaining_tries > 0:
            try:
                response = requests.get(url, headers=additional_headers)
                return response
            except:
                remaining_tries -= 1

                if self._delay > 0:
                    time.sleep(self._delay/1000)

        RequestError = namedtuple("response", ["status_code", "text"])
        return RequestError(status_code=503, text="Connection failure")
