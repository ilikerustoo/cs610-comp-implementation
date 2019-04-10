from google.cloud import language
from re import compile
from re import IGNORECASE
from requests import get
from urllib.parse import quote_plus

from twitter import Twitter

WIKIDATA_QUERY_URL = "https://query.wikidata.org/sparql?query=%s&format=JSON"

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

    def __init__(self):
        self.language_client = language.LanguageServiceClient()
        self.twitter = Twitter()

    def scrape_cmpy_info(self, mid):

        query = MID_TO_TICKER_QUERY % mid
        bindings = self.retrieve_wikidata_data(query)

        if not bindings:
            return None

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
                datas.append(data)

        return datas

    def search_company_intweet(self, tweet):


        if not tweet:

            return None

        text = self.get_longtext(tweet)
        if not text:

            return None

        document = language.types.Document(
            content=text,
            type=language.enums.Document.Type.PLAIN_TEXT,
            language="en")
        entities = self.language_client.analyze_entities(document).entities



        companies = []
        for entity in entities:

            name = entity.name
            metadata = entity.metadata
            try:
                mid = metadata["mid"]
            except KeyError:

                continue

            company_data = self.scrape_cmpy_info(mid)

            if not company_data:


                continue


            for company in company_data:

                sentiment = self.gnlp_sentiment(text)


                company["sentiment"] = sentiment

                tickers = [existing["ticker"] for existing in companies]
                if not company["ticker"] in tickers:
                    companies.append(company)




        return companies

    def get_longtext(self, tweet):
        """Retrieves the text from a tweet with any @mentions expanded to
        their full names.
        """

        if not tweet:

            return None

        try:
            text = self.twitter.get_tweet_text(tweet)
            mentions = tweet["entities"]["user_mentions"]
        except KeyError:

            return None

        if not text:

            return None

        if not mentions:

            return text


        for mention in mentions:
            try:
                screen_name = "@%s" % mention["screen_name"]
                name = mention["name"]
            except KeyError:

                continue


            pattern = compile(screen_name, IGNORECASE)
            text = pattern.sub(name, text)

        return text


    def retrieve_wikidata_data(self, query):

        query_url = WIKIDATA_QUERY_URL % quote_plus(query)


        response = get(query_url)
        try:
            response_json = response.json()
        except ValueError:

            return None


        try:
            results = response_json["results"]
            bindings = results["bindings"]
        except KeyError:

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

            return 0

        document = language.types.Document(
            content=text,
            type=language.enums.Document.Type.PLAIN_TEXT,
            language="en")
        sentiment = self.language_client.analyze_sentiment(
            document).document_sentiment



             

        return sentiment.score
