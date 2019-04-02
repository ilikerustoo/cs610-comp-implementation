from datetime import datetime
from http.server import BaseHTTPRequestHandler
from http.server import HTTPServer
from threading import Event
from threading import Thread
from time import sleep

from analysis import Checker
from logs import Logs
from trading import Trading
from twitter import Twitter

LOGS_TO_CLOUD = False
BACKOFF_STEP_S = 0.1
MAX_TRIES = 12
BACKOFF_RESET_S = 30 * 60
Webserver_HOST = "0.0.0.0"
Webserver_PORT = 1025
Webserver_MESSAGE = "OK"


class Webserver:

    def __init__(self):
        """Creates a Web server on a background thread."""

        self.server = HTTPServer((Webserver_HOST, Webserver_PORT),
                                 self.WebserverHandler)
        self.thread = Thread(target=self.server.serve_forever)
        self.thread.daemon = True

    def start(self):

        self.thread.start()

    def stop(self):

        self.server.shutdown()
        self.server.server_close()

    class WebserverHandler(BaseHTTPRequestHandler):

        def _set_headers(self):
            self.send_response(200)
            self.send_header("Content-type", "text/plain")
            self.end_headers()

        def do_GET(self):
            self._set_headers()
            self.wfile.write(Webserver_MESSAGE.encode("utf-8"))

        def do_HEAD(self):
            self._set_headers()


class Main:

    def __init__(self):
        self.logs = Logs(name="main", to_cloud=LOGS_TO_CLOUD)
        self.twitter = Twitter(logs_to_cloud=LOGS_TO_CLOUD)

    def twitter_callback(self, tweet):

        checker = Checker(logs_to_cloud=LOGS_TO_CLOUD)
        logs = Logs(name="main-callback", to_cloud=LOGS_TO_CLOUD)

        companies = checker.search_company_intweet(tweet)
        logs.info("Using companies: %s" % companies)
        if not companies:
            return

        trading = Trading(logs_to_cloud=LOGS_TO_CLOUD)

        twitter = Twitter(logs_to_cloud=LOGS_TO_CLOUD)
        twitter.tweet(companies, tweet)

    def run_session(self):

        self.logs.info("Starting new session.")
        try:
            self.twitter.start_streaming(self.twitter_callback)
        except:
            self.logs.catch()
        finally:
            self.twitter.stop_streaming()
            self.logs.info("Ending session.")

    def backoff(self, tries):

        delay = BACKOFF_STEP_S * pow(2, tries)
        self.logs.warn("Waiting for %.1f seconds." % delay)
        sleep(delay)

    def run(self):

        tries = 0
        while True:

            self.run_session()

            now = datetime.now()
            if tries == 0:
                self.logs.debug("Starting first backoff sequence.")
                backoff_start = now

            if (now - backoff_start).total_seconds() > BACKOFF_RESET_S:
                self.logs.debug("Starting new backoff sequence.")
                tries = 0
                backoff_start = now

            if tries >= MAX_TRIES:
                self.logs.warn("Exceeded maximum retry count.")
                break

            self.backoff(tries)

            tries += 1


if __name__ == "__main__":
    Webserver = Webserver()
    Webserver.start()
    try:
        Main().run()
    finally:
        Webserver.stop()
