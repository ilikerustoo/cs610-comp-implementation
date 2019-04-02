from google.cloud import language
from re import compile
from re import IGNORECASE
from requests import get
from urllib.parse import quote_plus

from logs import Logs
from twitter import Twitter

# GET request to the Wikidata API.
WIKIDATA_QUERY_URL = "https://query.wikidata.org/sparql?query=%s&format=JSON"

# A Wikidata SPARQL query to find stock ticker symbols and other information
# for a company. The string parameter is the Freebase ID of the company.
MID_TO_TICKER_QUERY = (
    'SELECT ?companyLabel ?rootLabel ?tickerLabel ?exchangeNameLabel'
    ' WHERE {'
    '  ?entity wdt:P646 "%s" .'
    '  ?entity wdt:P176* ?manufacturer .'
    '  ?manufacturer wdt:P1366* ?company .'
    '  { ?company p:P414 ?exchange } UNION'
    '  { ?company wdt:P127+ / wdt:P1366* ?root .'
    '    ?root p:P414 ?exchange } UNION'
    '  { ?company wdt:P749+ / wdt:P1366* ?root .'
    '    ?root p:P414 ?exchange } .'
    '  VALUES ?exchanges { wd:Q13677 wd:Q82059 } .'
    '  ?exchange ps:P414 ?exchanges .'
    '  ?exchange pq:P249 ?ticker .'
    '  ?exchange ps:P414 ?exchangeName .'
    '  FILTER NOT EXISTS { ?company wdt:P31 /'
    '                               wdt:P279* wd:Q11032 } .'
    '  FILTER NOT EXISTS { ?company wdt:P31 /'
    '                               wdt:P279* wd:Q192283 } .'
    '  FILTER NOT EXISTS { ?company wdt:P31 /'
    '                               wdt:P279* wd:Q1684600 } .'
    '  FILTER NOT EXISTS { ?company wdt:P31 /'
    '                               wdt:P279* wd:Q14350 } .'
    '  FILTER NOT EXISTS { ?company wdt:P31 /'
    '                               wdt:P279* wd:Q1616075 } .'
    '  FILTER NOT EXISTS { ?company wdt:P31 /'
    '                               wdt:P279* wd:Q2001305 } .'
    '  SERVICE wikibase:label {'
    '   bd:serviceParam wikibase:language "en" .'
    '  }'
    ' } GROUP BY ?companyLabel ?rootLabel ?tickerLabel ?exchangeNameLabel'
    ' ORDER BY ?companyLabel ?rootLabel ?tickerLabel ?exchangeNameLabel')


class Checker:
    """A helper for analyzing company data in text."""

    def __init__(self, logs_to_cloud):
        self.logs = Logs(name="checker", to_cloud=logs_to_cloud)
        self.language_client = language.LanguageServiceClient()
        self.twitter = Twitter(logs_to_cloud=logs_to_cloud)

    def scrape_cmpy_info(self, mid):

        query = MID_TO_TICKER_QUERY % mid
        bindings = self.retrieve_wikidata_data(query)

        if not bindings:
            self.logs.debug("No company data found for MID: %s" % mid)
            return None

        # Collect the data from the response.
        datas = []
        for binding in bindings:
            try:
                name = binding["companyLabel"]["value"]
            except KeyError:
                name = None

            try:
                root = binding["rootLabel"]["value"]
            except KeyError:
                root = None

            try:
                ticker = binding["tickerLabel"]["value"]
            except KeyError:
                ticker = None

            try:
                exchange = binding["exchangeNameLabel"]["value"]
            except KeyError:
                exchange = None

            data = {"name": name,
                    "ticker": ticker,
                    "exchange": exchange}

            if root and root != name:
                data["root"] = root

            if data not in datas:
                self.logs.debug("Adding company data: %s" % data)
                datas.append(data)
            else:
                self.logs.warn("Skipping duplicate company data: %s" % data)

        return datas

    def search_company_intweet(self, tweet):


        if not tweet:
            self.logs.warn("No tweet to find companies.")
            return None

        text = self.get_longtext(tweet)
        if not text:
            self.logs.error("Failed to get text from tweet: %s" % tweet)
            return None

        document = language.types.Document(
            content=text,
            type=language.enums.Document.Type.PLAIN_TEXT,
            language="en")
        entities = self.language_client.analyze_entities(document).entities
        self.logs.debug("Found entities: %s" %
                        self.convert_entity_string(entities))

        companies = []
        for entity in entities:

            name = entity.name
            metadata = entity.metadata
            try:
                mid = metadata["mid"]
            except KeyError:
                self.logs.debug("No MID found for entity: %s" % name)
                continue

            company_data = self.scrape_cmpy_info(mid)

            if not company_data:
                self.logs.debug("No company data found for entity: %s (%s)" %
                                (name, mid))
                continue
            self.logs.debug("Found company data: %s" % company_data)

            for company in company_data:

                sentiment = self.gnlp_sentiment(text)
                self.logs.debug("Using sentiment for company: %s %s" %
                                (sentiment, company))
                company["sentiment"] = sentiment

                tickers = [existing["ticker"] for existing in companies]
                if not company["ticker"] in tickers:
                    companies.append(company)
                else:
                    self.logs.warn(
                        "Skipping company with duplicate ticker: %s" % company)

        return companies

    def get_longtext(self, tweet):
        """Retrieves the text from a tweet with any @mentions expanded to
        their full names.
        """

        if not tweet:
            self.logs.warn("No tweet to expand text.")
            return None

        try:
            text = self.twitter.get_tweet_text(tweet)
            mentions = tweet["entities"]["user_mentions"]
        except KeyError:
            self.logs.error("Malformed tweet: %s" % tweet)
            return None

        if not text:
            self.logs.warn("Empty text.")
            return None

        if not mentions:
            self.logs.debug("No mentions.")
            return text

        self.logs.debug("Using mentions: %s" % mentions)
        for mention in mentions:
            try:
                screen_name = "@%s" % mention["screen_name"]
                name = mention["name"]
            except KeyError:
                self.logs.warn("Malformed mention: %s" % mention)
                continue

            self.logs.debug("Expanding mention: %s %s" % (screen_name, name))
            pattern = compile(screen_name, IGNORECASE)
            text = pattern.sub(name, text)

        return text


    def retrieve_wikidata_data(self, query):

        query_url = WIKIDATA_QUERY_URL % quote_plus(query)
        self.logs.debug("Wikidata query: %s" % query_url)

        response = get(query_url)
        try:
            response_json = response.json()
        except ValueError:
            self.logs.error("Failed to decode JSON response: %s" % response)
            return None
        self.logs.debug("Wikidata response: %s" % response_json)

        try:
            results = response_json["results"]
            bindings = results["bindings"]
        except KeyError:
            self.logs.error("Malformed Wikidata response: %s" % response_json)
            return None

        return bindings


    def convert_entity_string(self, entities):

        tostrings = [self.convert_oneentity(entity) for entity in entities]
        return "[%s]" % ", ".join(tostrings)


    def convert_oneentity(self, entity):

        metadata = ", ".join(['"%s": "%s"' % (key, value) for
                              key, value in entity.metadata.items()])

        mentions = ", ".join(['"%s"' % mention for mention in entity.mentions])

        return ('{name: "%s",'
                ' type: "%s",'
                ' metadata: {%s},'
                ' salience: %s,'
                ' mentions: [%s]}') % (
            entity.name,
            entity.type,
            metadata,
            entity.salience,
            mentions)


    def gnlp_sentiment(self, text):

        if not text:
            self.logs.warn("No sentiment for empty text.")
            return 0

        document = language.types.Document(
            content=text,
            type=language.enums.Document.Type.PLAIN_TEXT,
            language="en")
        sentiment = self.language_client.analyze_sentiment(
            document).document_sentiment

        self.logs.debug(
            "Sentiment score and magnitude for text: %s %s \"%s\"" %
            (sentiment.score, sentiment.magnitude, text))

        return sentiment.score
