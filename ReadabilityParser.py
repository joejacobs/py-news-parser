#!/usr/bin/env python
# -*- coding: utf-8 -*-
from HTTPRequest import HTTPRequest

import os
import signal
import subprocess
import time
import urllib


# parse HTML pages with the Readability.js server
class ArticleParser(object):
    _git_url = "https://github.com/joejacobs/readability.js-server"
    _readability_dir = "./readability.js-server"

    def __init__(self, args):
        self._log_file = args.readability_log_file
        self._port = args.readability_port
        self._user_agent = args.user_agent

        self._install_server()
        self._launch_server()

    def __del__(self):
        self._process.send_signal(signal.SIGINT)
        self._process.wait()

    # install the Readability.js node.js server
    def _install_server(self):
        if not os.path.exists(self._readability_dir):
            clone_cmd = ["git", "clone", self._git_url, self._readability_dir]
            clone_cwd = "./"

            npm_cmd = ["npm", "i"]
            npm_cwd = self._readability_dir

            subprocess.run(clone_cmd, cwd=clone_cwd, check=True)
            subprocess.run(npm_cmd, cwd=npm_cwd, check=True)

    # launch the Readablity.js server
    def _launch_server(self):
        cmd = ["node", f"{self._readability_dir}/main.js"]
        cwd = "./"
        env = os.environ.copy()
        env["LOGFILE"] = self._log_file
        env["PORT"] = str(self._port)
        self._process = subprocess.Popen(cmd, cwd=cwd, env=env)
        self._wait_for_launch()

    # parse a URL
    def parse(self, url):
        encoded_url = urllib.parse.quote_plus(url)
        api_url = f"http://localhost:{self._port}/article/{encoded_url}"
        return HTTPRequest(2500, 3, self._user_agent).get(api_url)

    # keep reading the log file until the server has finished launching
    def _wait_for_launch(self):
        keep_reading_log_file = True

        while(keep_reading_log_file):
            time.sleep(0.2)

            try:
                with open(self._log_file, "r") as fp:
                    log = fp.read()

                if f"Server launched on port {self._port}" in log:
                    keep_reading_log_file = False
            except FileNotFoundError:
                continue
