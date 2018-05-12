#!/usr/bin/env python
# -*- coding: utf-8 -*-
from HTTPRequest import HTTPRequest


# parse HTML pages with the Mercury Web Parser
class ArticleParser(object):
    def __init__(self, args):
        self._api_key = args.mercury_api_key

    def parse(self, url):
        api_url = f"https://mercury.postlight.com/parser?url={url}"
        req_headers = {"x-api-key": self._api_key}
        return HTTPRequest(tries=3, delay=2500).get(api_url, req_headers)
