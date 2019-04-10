from json import loads
from os import getenv
from queue import Empty
from queue import Queue
from threading import Event
from threading import Thread
from time import time
from tweepy import API
from tweepy import Cursor
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


TWITTER_ACCESS_TOKEN = getenv("TWITTER_ACCESS_TOKEN")
TWITTER_ACCESS_TOKEN_SECRET = getenv("TWITTER_ACCESS_TOKEN_SECRET")
TWITTER_CONSUMER_KEY = getenv("TWITTER_CONSUMER_KEY")
TWITTER_CONSUMER_SECRET = getenv("TWITTER_CONSUMER_SECRET")
TRUMP_USER_ID = "1112802541018992640"
TWEET_URL = "https://twitter.com/%s/status/%s"
EMOJI_THUMBS_UP = "\U0001F4C8"
EMOJI_THUMBS_DOWN = "\U0001F4C9"
EMOJI_SHRUG = "\U0001F340"
MAX_TWEET_SIZE = 140
NUM_THREADS = 100
QUEUE_TIMEOUT_S = 5 * 60
API_RETRY_COUNT = 60
API_RETRY_DELAY_S = 1
API_RETRY_ERRORS = [400, 401, 500, 502, 503, 504]


class Twitter:
    """A helper for talking to Twitter APIs."""

    def __init__(self):


        self.twitter_auth = OAuthHandler(TWITTER_CONSUMER_KEY,
                                         TWITTER_CONSUMER_SECRET)
        self.twitter_auth.set_access_token(TWITTER_ACCESS_TOKEN,
                                           TWITTER_ACCESS_TOKEN_SECRET)
        self.twitter_api = API(auth_handler=self.twitter_auth,
                               retry_count=API_RETRY_COUNT,
                               retry_delay=API_RETRY_DELAY_S,
                               retry_errors=API_RETRY_ERRORS,
                               wait_on_rate_limit=True,
                               wait_on_rate_limit_notify=True)
        self.twitter_listener = None

    def start_streaming(self, callback):
        """Starts streaming tweets and returning data to the callback."""

        self.twitter_listener = TwitterListener(
            callback=callback)
        twitter_stream = Stream(self.twitter_auth, self.twitter_listener)


        twitter_stream.filter(follow=[TRUMP_USER_ID])


        if self.twitter_listener and self.twitter_listener.get_error_status():
            raise Exception("Twitter API error: %s" %
                            self.twitter_listener.get_error_status())

    def stop_streaming(self):
        """Stops the current stream."""

        if not self.twitter_listener:

            return


        self.twitter_listener.stop_queue()
        self.twitter_listener = None

    def tweet(self, companies, tweet):
        """Posts a tweet listing the companies, their ticker symbols, and a
        quote of the original tweet.
        """

        link = self.get_tweet_link(tweet)
        text = self.make_tweet_text(companies, link)


        self.twitter_api.update_status(text)

    def make_tweet_text(self, companies, link):
        """Generates the text for a tweet."""


        names = []
        for company in companies:
            name = company["name"]
            if name not in names:
                names.append(name)


        tickers = {}
        sentiments = {}
        for name in names:
            tickers[name] = []
            for company in companies:
                if company["name"] == name:
                    ticker = company["ticker"]
                    tickers[name].append(ticker)
                    sentiment = company["sentiment"]

                    sentiments[name] = sentiment


        lines = []
        for name in names:
            sentiment_str = self.gnlp_sentiment_emoji(sentiments[name])
            tickers_str = " ".join(["$%s" % t for t in tickers[name]])
            line = "%s %s %s" % (name, sentiment_str, tickers_str)
            lines.append(line)


        lines_str = "\n".join(lines)
        size = len(lines_str) + 1 + len(link)
        if size > MAX_TWEET_SIZE:

            lines_size = MAX_TWEET_SIZE - len(link) - 2
            lines_str = "%s\u2026" % lines_str[:lines_size]


        text = "%s\n%s" % (lines_str, link)

        return text

    def gnlp_sentiment_emoji(self, sentiment):
        """Returns the emoji matching the sentiment."""

        if not sentiment:
            return EMOJI_SHRUG

        if sentiment > 0:
            return EMOJI_THUMBS_UP

        if sentiment < 0:
            return EMOJI_THUMBS_DOWN


        return EMOJI_SHRUG

    def get_tweet(self, tweet_id):
        """Looks up metadata for a single tweet."""


        status = self.twitter_api.get_status(tweet_id, tweet_mode="extended")
        if not status:

            return None


        return status._json

    def get_tweets(self, since_id):
        """Looks up metadata for all Trump tweets since the specified ID."""

        tweets = []


        since_id = str(int(since_id) - 1)


        for status in Cursor(self.twitter_api.user_timeline,
                             user_id=TRUMP_USER_ID, since_id=since_id,
                             tweet_mode="extended").items():


            tweets.append(status._json)



        return tweets

    def get_tweet_text(self, tweet):
        """Returns the full text of a tweet."""




        try:
            if "extended_tweet" in tweet:

                return tweet["extended_tweet"]["full_text"]
            elif "full_text" in tweet:

                return tweet["full_text"]
            else:

                return tweet["text"]
        except KeyError:

            return None

    def get_tweet_link(self, tweet):
        """Creates the link URL to a tweet."""

        if not tweet:

            return None

        try:
            screen_name = tweet["user"]["screen_name"]
            id_str = tweet["id_str"]
        except KeyError:

            return None

        link = TWEET_URL % (screen_name, id_str)
        return link


class TwitterListener(StreamListener):
    """A listener class for handling streaming Twitter data."""

    def __init__(self, callback):


        self.callback = callback
        self.error_status = None
        self.start_queue()

    def start_queue(self):
        """Creates a queue and starts the worker threads."""

        self.queue = Queue()
        self.stop_event = Event()

        self.workers = []
        for worker_id in range(NUM_THREADS):
            worker = Thread(target=self.process_queue, args=[worker_id])
            worker.daemon = True
            worker.start()
            self.workers.append(worker)

    def stop_queue(self):
        """Shuts down the queue and worker threads."""


        if self.queue:

            self.queue.join()




        if self.workers:

            self.stop_event.set()
            for worker in self.workers:

                worker.join()



    def process_queue(self, worker_id):
        """Continuously processes tasks on the queue."""







        while not self.stop_event.is_set():
            try:
                data = self.queue.get(block=True, timeout=QUEUE_TIMEOUT_S)
                start_time = time()
                self.handle_data(data)
                self.queue.task_done()
                end_time = time()
                qsize = self.queue.qsize()


            except Empty:


                continue






    def on_error(self, status):
        """Handles any API errors."""


        self.error_status = status
        self.stop_queue()
        return False

    def get_error_status(self):
        """Returns the API error status, if there was one."""
        return self.error_status

    def on_data(self, data):
        """Puts a task to process the new data on the queue."""


        if self.stop_event.is_set():
            return False


        self.queue.put(data)
        return True

    def handle_data(self, data):
        """Sanity-checks and extracts the data before sending it to the
        callback.
        """

        try:
            tweet = loads(data)
        except ValueError:

            return

        try:
            user_id_str = tweet["user"]["id_str"]
            screen_name = tweet["user"]["screen_name"]
        except KeyError:

            return



        if user_id_str != TRUMP_USER_ID:


            return




        self.callback(tweet)
