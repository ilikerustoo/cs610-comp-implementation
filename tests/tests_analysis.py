from ast import literal_eval
from google.cloud import language
from os import getenv
from pytest import fixture

from sentiment import Checker
from sentiment import MID_TO_TICKER_QUERY
from twitter import Twitter


@fixture
def checker():
    return Checker()


def get_tweet(tweet_id):
    """Looks up data for a single tweet."""

    twitter = Twitter()
    return twitter.get_tweet(tweet_id)


def get_tweet_text(tweet_id):
    """Looks up the text for a single tweet."""

    tweet = get_tweet(tweet_id)
    checker = Checker()
    return checker.get_longtext(tweet)


def make_entity(name, type, metadata, salience, mentions):
    """Creates a language.enums.Entity object."""

    entity = language.enums.Entity()
    entity.name = name
    entity.type = type
    entity.metadata = metadata
    entity.salience = salience
    entity.mentions = mentions
    return entity


def test_scrape_cmpy_info_1(checker):
    assert checker.scrape_cmpy_info("/m/035nm") == [{
        "exchange": "New York Stock Exchange",
        "name": "General Motors",
        "ticker": "GM"}]


def test_scrape_cmpy_info_2(checker):
    assert checker.scrape_cmpy_info("/m/04n3_w4") == [{
        "exchange": "New York Stock Exchange",
        "name": "Fiat",
        "root": "Fiat Chrysler Automobiles",
        "ticker": "FCAU"}]


def test_scrape_cmpy_info_3(checker):
    assert checker.scrape_cmpy_info("/m/0d8c4") == [{
        "exchange": "New York Stock Exchange",
        "name": "Lockheed Martin",
        "ticker": "LMT"}]


def test_scrape_cmpy_info_4(checker):
    assert checker.scrape_cmpy_info("/m/0hkqn") == [{
        "exchange": "New York Stock Exchange",
        "name": "Lockheed Martin",
        "ticker": "LMT"}]


def test_scrape_cmpy_info_5(checker):
    assert checker.scrape_cmpy_info("/m/09jcvs") == [{
        "exchange": "NASDAQ",
        "name": "YouTube",
        "root": "Alphabet Inc.",
        "ticker": "GOOG"}, {
        "exchange": "NASDAQ",
        "name": "YouTube",
        "root": "Alphabet Inc.",
        "ticker": "GOOGL"}, {
        "exchange": "New York Stock Exchange",
        "name": "YouTube",
        "root": "BlackRock",
        "ticker": "BLK"}, {
        "exchange": "NASDAQ",
        "name": "YouTube",
        "root": "Google",
        "ticker": "GOOG"}, {
        "exchange": "NASDAQ",
        "name": "YouTube",
        "root": "Google",
        "ticker": "GOOGL"}, {
        "exchange": "New York Stock Exchange",
        "name": "YouTube",
        "root": "PNC Financial Services",
        "ticker": "PNC"}]


def test_scrape_cmpy_info_6(checker):
    assert checker.scrape_cmpy_info("/m/045c7b") == [{
        "exchange": "NASDAQ",
        "name": "Google",
        "ticker": "GOOG"}, {
        "exchange": "NASDAQ",
        "name": "Google",
        "ticker": "GOOGL"}, {
        "exchange": "NASDAQ",
        "name": "Google",
        "root": "Alphabet Inc.",
        "ticker": "GOOG"}, {
        "exchange": "NASDAQ",
        "name": "Google",
        "root": "Alphabet Inc.",
        "ticker": "GOOGL"}, {
        "exchange": "New York Stock Exchange",
        "name": "Google",
        "root": "BlackRock",
        "ticker": "BLK"}, {
        "exchange": "New York Stock Exchange",
        "name": "Google",
        "root": "PNC Financial Services",
        "ticker": "PNC"}]


def test_scrape_cmpy_info_7(checker):
    assert checker.scrape_cmpy_info("/m/01snr1") == [{
        "exchange": "New York Stock Exchange",
        "name": "Bayer",
        "root": "BlackRock",
        "ticker": "BLK"}, {
        "exchange": "New York Stock Exchange",
        "name": "Bayer",
        "root": "PNC Financial Services",
        "ticker": "PNC"}]


def test_scrape_cmpy_info_8(checker):
    assert checker.scrape_cmpy_info("/m/02zs4") == [{
        "exchange": "New York Stock Exchange",
        "name": "Ford Motor Company",
        "ticker": "F"}]

def test_scrape_cmpy_info_20(checker):
    assert checker.scrape_cmpy_info("/m/0d6lp") is None


def test_scrape_cmpy_info_21(checker):
    assert checker.scrape_cmpy_info("/m/04mzd6n") is None


def test_scrape_cmpy_info_invalid(checker):
    assert checker.scrape_cmpy_info("xyz") is None


def test_scrape_cmpy_info_empty(checker):
    assert checker.scrape_cmpy_info("") is None


def test_convert_oneentity_1(checker):
    assert checker.convert_oneentity(make_entity(
        name="General Motors",
        type=language.enums.Entity.Type.ORGANIZATION,
        metadata={
            "mid": "/m/035nm",
            "wikipedia_url": "http://en.wikipedia.org/wiki/General_Motors"},
        salience=0.33838183,
        mentions=["General Motors"])) == (
            '{name: "General Motors",'
            ' type: "Type.ORGANIZATION",'
            ' metadata: {"mid": "/m/035nm",'
            ' "wikipedia_url": "http://en.wikipedia.org/wiki/General_Motors"},'
            ' salience: 0.33838183,'
            ' mentions: ["General Motors"]}')


def test_convert_oneentity_2(checker):
    assert checker.convert_oneentity(make_entity(
        name="jobs",
        type=language.enums.Entity.Type.OTHER,
        metadata={},
        salience=0.31634554,
        mentions=["jobs"])) == (
        '{name: "jobs",'
        ' type: "Type.OTHER",'
        ' metadata: {},'
        ' salience: 0.31634554,'
        ' mentions: ["jobs"]}')


def test_convert_entity_string(checker):
    assert checker.convert_entity_string([make_entity(
        name="General Motors",
        type=language.enums.Entity.Type.ORGANIZATION,
        metadata={
            "mid": "/m/035nm",
            "wikipedia_url": "http://en.wikipedia.org/wiki/General_Motors"},
        salience=0.33838183,
        mentions=["General Motors"]), make_entity(
        name="jobs",
        type=language.enums.Entity.Type.OTHER,
        metadata={},
        salience=0.31634554,
        mentions=["jobs"])]) == (
        '[{name: "General Motors",'
        ' type: "Type.ORGANIZATION",'
        ' metadata: {"mid": "/m/035nm",'
        ' "wikipedia_url": "http://en.wikipedia.org/wiki/General_Motors"},'
        ' salience: 0.33838183,'
        ' mentions: ["General Motors"]}, '
        '{name: "jobs",'
        ' type: "Type.OTHER",'
        ' metadata: {},'
        ' salience: 0.31634554,'
        ' mentions: ["jobs"]}]')

def test_gnlp_sentiment_1(checker):
    assert checker.gnlp_sentiment(get_tweet_text("806134244384899072")) < 0

def test_gnlp_sentiment_1(checker):
    assert checker.gnlp_sentiment(get_tweet_text("806134244384899072")) < 0


def test_gnlp_sentiment_2(checker):
    assert checker.gnlp_sentiment(get_tweet_text("812061677160202240")) > 0


def test_gnlp_sentiment_5(checker):
    assert checker.gnlp_sentiment(get_tweet_text("816635078067490816")) > 0


def test_gnlp_sentiment_6(checker):
    assert checker.gnlp_sentiment(get_tweet_text("817071792711942145")) < 0


def test_gnlp_sentiment_9(checker):
    assert checker.gnlp_sentiment(get_tweet_text("821415698278875137")) > 0


def test_gnlp_sentiment_12(checker):
    assert checker.gnlp_sentiment(get_tweet_text("803808454620094465")) > 0


def test_gnlp_sentiment_13(checker):
    assert checker.gnlp_sentiment(get_tweet_text("621669173534584833")) < 0


def test_gnlp_sentiment_14(checker):
    assert checker.gnlp_sentiment(get_tweet_text("664911913831301123")) < 0


# def test_gnlp_sentiment_15(checker):
#     assert checker.gnlp_sentiment(get_tweet_text("823950814163140609")) > 0


def test_gnlp_sentiment_16(checker):
    assert checker.gnlp_sentiment(get_tweet_text("824055927200423936")) > 0


def test_gnlp_sentiment_none(checker):
    assert checker.gnlp_sentiment(None) == 0


def test_search_company_intweet_1(checker):
    assert checker.search_company_intweet(get_tweet("806134244384899072")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Boeing",
        "sentiment": -0.10000000149011612,
        "ticker": "BA"}]


def test_search_company_intweet_2(checker):
    assert checker.search_company_intweet(get_tweet("812061677160202240")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Lockheed Martin",
        "sentiment": 0.20000000298023224,  # -0.1
        "ticker": "LMT"}, {
        "exchange": "New York Stock Exchange",
        "name": "Boeing",
        "sentiment": 0.20000000298023224,
        "ticker": "BA"}]


def test_search_company_intweet_3(checker):
    assert checker.search_company_intweet(get_tweet("816260343391514624")) == [{
        "exchange": "New York Stock Exchange",
        "name": "General Motors",
        "sentiment": 0.0,  # -0.1
        "ticker": "GM"}]


def test_search_company_intweet_5(checker):
    assert checker.search_company_intweet(get_tweet("816635078067490816")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Ford Motor Company",
        "sentiment": 0.20000000298023224,
        "ticker": "F"}]


def test_search_company_intweet_6(checker):
    assert checker.search_company_intweet(get_tweet("817071792711942145")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Toyota",
        "sentiment": -0.30000001192092896,
        "ticker": "TM"}]

def test_search_company_intweet_8(checker):
    assert checker.search_company_intweet(get_tweet("818461467766824961")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Ford",
        "root": "Ford Motor Company",
        "sentiment": 0.0,  # 0.1
        "ticker": "F"}, {
        "exchange": "New York Stock Exchange",
        "name": "Fiat",
        "root": "Fiat Chrysler Automobiles",
        "sentiment": 0.0,  # 0.1
        "ticker": "FCAU"}]


def test_search_company_intweet_9(checker):
    assert checker.search_company_intweet(get_tweet("821415698278875137")) == [{
        "exchange": "New York Stock Exchange",
        "name": "General Motors",
        "sentiment": 0.20000000298023224,
        "ticker": "GM"}, {
        "exchange": "New York Stock Exchange",
        "name": "Walmart",
        "sentiment": 0.20000000298023224,
        "ticker": "WMT"}, {
        "exchange": "New York Stock Exchange",
        "name": "Walmart",
        "root": "State Street Corporation",
        "sentiment": 0.20000000298023224,
        "ticker": "STT"}]


def test_search_company_intweet_10(checker):
    assert checker.search_company_intweet(get_tweet("821697182235496450")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Ford Motor Company",
        "sentiment": -0.800000011920929,  # 0
        "ticker": "F"}, {
        "exchange": "New York Stock Exchange",
        "name": "General Motors",
        "sentiment": -0.800000011920929,  # 0,
        "ticker": "GM"}, {
        "exchange": "New York Stock Exchange",
        "name": "Lockheed Martin",
        "sentiment": -0.800000011920929,  # 0,
        "ticker": "LMT"}]


def test_search_company_intweet_11(checker):
    assert checker.search_company_intweet(get_tweet("821703902940827648")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Bayer",
        "sentiment": 0.0,  # 0.1
        "root": "BlackRock",
        "ticker": "BLK"}, {
        "exchange": "New York Stock Exchange",
        "name": "Bayer",
        "sentiment": 0.0,  # 0.1
        "root": "PNC Financial Services",
        "exchange": "New York Stock Exchange",
        "ticker": "PNC"}]

def test_search_company_intweet_13(checker):
    assert checker.search_company_intweet(get_tweet("621669173534584833")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Macy's",
        "root": "Macy's, Inc.",
        "sentiment": -0.5,
        "ticker": "M"}]


def test_search_company_intweet_16(checker):
    assert checker.search_company_intweet(get_tweet("824055927200423936")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Ford Motor Company",
        "sentiment": 0.4000000059604645,
        "ticker": "F"}, {
        "exchange": "New York Stock Exchange",
        "name": "General Motors",
        "sentiment": 0.4000000059604645,
        "ticker": "GM"}]


def test_search_company_intweet_17(checker):
    assert checker.search_company_intweet(get_tweet("826041397232943104")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Delta Air Lines",
        "sentiment": -0.5,
        "ticker": "DAL"}]


def test_search_company_intweet_18(checker):
    assert checker.search_company_intweet(get_tweet("824765229527605248")) == []


def test_search_company_intweet_19(checker):
    assert checker.search_company_intweet(get_tweet("827874208021639168")) == []


def test_search_company_intweet_20(checker):
    assert checker.search_company_intweet(get_tweet("828642511698669569")) == []


def test_search_company_intweet_21(checker):
    assert checker.search_company_intweet(get_tweet("828793887275761665")) == []


def test_search_company_intweet_22(checker):
    assert checker.search_company_intweet(get_tweet("829410107406614534")) == [{
        "exchange": "NASDAQ",
        "name": "Intel",
        "sentiment": 0.30000001192092896,
        "ticker": "INTC"}]


def test_search_company_intweet_23(checker):
    assert checker.search_company_intweet(get_tweet("829356871848951809")) == [{
        "exchange": "New York Stock Exchange",
        "name": "Nordstrom",
        "sentiment": -0.30000001192092896,
        "ticker": "JWN"}]


def test_search_company_intweet_25(checker):
    assert checker.search_company_intweet(get_tweet("1067494680416407552")) == [{
        "exchange": "New York Stock Exchange",
        "name": "General Motors",
        "sentiment": -0.10000000149011612,
        "ticker": "GM"}]


def test_search_company_intweet_none(checker):
    assert checker.search_company_intweet(None) is None


def test_get_longtext_1(checker):
    assert checker.get_longtext(get_tweet("829410107406614534")) == (
        "Thank you Brian Krzanich, CEO of Intel. A great investment ($7 BILLIO"
        "N) in American INNOVATION and JOBS! #AmericaFirst\U0001f1fa\U0001f1f8"
        " https://t.co/76lAiSSQ1l")


def test_get_longtext_2(checker):
    assert checker.get_longtext(get_tweet("828574430800539648")) == (
        "Any negative polls are fake news, just like the CNN, ABC, NBC polls i"
        "n the election. Sorry, people want border security and extreme vettin"
        "g.")


def test_get_longtext_3(checker):
    assert checker.get_longtext(get_tweet("828642511698669569")) == (
        "The failing The New York Times writes total fiction concerning me. Th"
        "ey have gotten it wrong for two years, and now are making up stories "
        "&amp; sources!")


def test_get_longtext_4(checker):
    assert checker.get_longtext(get_tweet("845334323045765121")) == (
        "Today, I was thrilled to announce a commitment of $25 BILLION &amp; 2"
        "0K AMERICAN JOBS over the next 4 years. THANK YOU Charter Communicati"
        "ons! https://t.co/PLxUmXVl0h")


def test_get_longtext_5(checker):
    assert checker.get_longtext(get_tweet("845645916732358656")) == (
        "ObamaCare will explode and we will all get together and piece togethe"
        "r a great healthcare plan for THE PEOPLE. Do not worry!")


STREAMING_TWEET_LONG = literal_eval(
    '{"contributors": None, "truncated": True, "text": "Today, I was thrilled '
    'to announce a commitment of $25 BILLION &amp; 20K AMERICAN JOBS over the '
    'next 4 years. THANK YOU\u2026 https://t.co/nWJ1hNmzoR", "is_quote_status"'
    ': False, "in_reply_to_status_id": None, "id": 845334323045765121, "favori'
    'te_count": 0, "source": "<a href=\\"http://twitter.com/download/iphone\\"'
    ' rel=\\"nofollow\\">Twitter for iPhone</a>", "retweeted": False, "coordin'
    'ates": None, "timestamp_ms": "1490378382823", "entities": {"user_mentions'
    '": [], "symbols": [], "hashtags": [], "urls": [{"url": "https://t.co/nWJ1'
    'hNmzoR", "indices": [120, 143], "expanded_url": "https://twitter.com/i/we'
    'b/status/845334323045765121", "display_url": "twitter.com/i/web/status/8'
    '\u2026"}]}, "in_reply_to_screen_name": None, "id_str": "84533432304576512'
    '1", "display_text_range": [0, 140], "retweet_count": 0, "in_reply_to_user'
    '_id": None, "favorited": False, "user": {"follow_request_sent": None, "pr'
    'ofile_use_background_image": True, "default_profile_image": False, "id": '
    '25073877, "verified": True, "profile_image_url_https": "https://pbs.twimg'
    '.com/profile_images/1980294624/DJT_Headshot_V2_normal.jpg", "profile_side'
    'bar_fill_color": "C5CEC0", "profile_text_color": "333333", "followers_cou'
    'nt": 26964041, "profile_sidebar_border_color": "BDDCAD", "id_str": "25073'
    '877", "profile_background_color": "6D5C18", "listed_count": 67185, "profi'
    'le_background_image_url_https": "https://pbs.twimg.com/profile_background'
    '_images/530021613/trump_scotland__43_of_70_cc.jpg", "utc_offset": -14400,'
    ' "statuses_count": 34648, "description": "45th President of the United St'
    'ates of America", "friends_count": 43, "location": "Washington, DC", "pro'
    'file_link_color": "0D5B73", "profile_image_url": "http://pbs.twimg.com/pr'
    'ofile_images/1980294624/DJT_Headshot_V2_normal.jpg", "following": None, "'
    'geo_enabled": True, "profile_banner_url": "https://pbs.twimg.com/profile_'
    'banners/25073877/1489657715", "profile_background_image_url": "http://pbs'
    '.twimg.com/profile_background_images/530021613/trump_scotland__43_of_70_c'
    'c.jpg", "name": "Donald J. Trump", "lang": "en", "profile_background_tile'
    '": True, "favourites_count": 46, "screen_name": "realDonaldTrump", "notif'
    'cations": None, "url": None, "created_at": "Wed Mar 18 13:46:38 +0000 200'
    '9", "contributors_enabled": False, "time_zone": "Eastern Time (US & Canad'
    'a)", "protected": False, "default_profile": False, "is_translator": False'
    '}, "geo": None, "in_reply_to_user_id_str": None, "possibly_sensitive": Fa'
    'lse, "lang": "en", "extended_tweet": {"display_text_range": [0, 142], "en'
    'tities": {"user_mentions": [], "symbols": [], "hashtags": [], "urls": [],'
    ' "media": [{"expanded_url": "https://twitter.com/realDonaldTrump/status/8'
    '45334323045765121/video/1", "display_url": "pic.twitter.com/PLxUmXVl0h", '
    '"url": "https://t.co/PLxUmXVl0h", "media_url_https": "https://pbs.twimg.c'
    'om/ext_tw_video_thumb/845330366156210176/pu/img/NdRWqCDX-r734Vpd.jpg", "v'
    'ideo_info": {"aspect_ratio": [16, 9], "duration_millis": 121367, "variant'
    's": [{"url": "https://video.twimg.com/ext_tw_video/845330366156210176/pu/'
    'vid/640x360/eu6vsoGqW43Cc64C.mp4", "bitrate": 832000, "content_type": "vi'
    'deo/mp4"}, {"url": "https://video.twimg.com/ext_tw_video/8453303661562101'
    '76/pu/vid/320x180/QPwu682wzK5C4e0V.mp4", "bitrate": 320000, "content_type'
    '": "video/mp4"}, {"url": "https://video.twimg.com/ext_tw_video/8453303661'
    '56210176/pu/pl/flkqzlPIkchbfIGZ.m3u8", "content_type": "application/x-mpe'
    'gURL"}, {"url": "https://video.twimg.com/ext_tw_video/845330366156210176/'
    'pu/vid/1280x720/6l5bIhFXEih9is4L.mp4", "bitrate": 2176000, "content_type"'
    ': "video/mp4"}]}, "id_str": "845330366156210176", "sizes": {"small": {"h"'
    ': 191, "resize": "fit", "w": 340}, "large": {"h": 576, "resize": "fit", "'
    'w": 1024}, "medium": {"h": 338, "resize": "fit", "w": 600}, "thumb": {"h"'
    ': 150, "resize": "crop", "w": 150}}, "indices": [143, 166], "type": "vide'
    'o", "id": 845330366156210176, "media_url": "http://pbs.twimg.com/ext_tw_v'
    'ideo_thumb/845330366156210176/pu/img/NdRWqCDX-r734Vpd.jpg"}]}, "extended_'
    'entities": {"media": [{"expanded_url": "https://twitter.com/realDonaldTru'
    'mp/status/845334323045765121/video/1", "display_url": "pic.twitter.com/PL'
    'xUmXVl0h", "url": "https://t.co/PLxUmXVl0h", "media_url_https": "https://'
    'pbs.twimg.com/ext_tw_video_thumb/845330366156210176/pu/img/NdRWqCDX-r734V'
    'pd.jpg", "video_info": {"aspect_ratio": [16, 9], "duration_millis": 12136'
    '7, "variants": [{"url": "https://video.twimg.com/ext_tw_video/84533036615'
    '6210176/pu/vid/640x360/eu6vsoGqW43Cc64C.mp4", "bitrate": 832000, "content'
    '_type": "video/mp4"}, {"url": "https://video.twimg.com/ext_tw_video/84533'
    '0366156210176/pu/vid/320x180/QPwu682wzK5C4e0V.mp4", "bitrate": 320000, "c'
    'ontent_type": "video/mp4"}, {"url": "https://video.twimg.com/ext_tw_video'
    '/845330366156210176/pu/pl/flkqzlPIkchbfIGZ.m3u8", "content_type": "applic'
    'ation/x-mpegURL"}, {"url": "https://video.twimg.com/ext_tw_video/84533036'
    '6156210176/pu/vid/1280x720/6l5bIhFXEih9is4L.mp4", "bitrate": 2176000, "co'
    'ntent_type": "video/mp4"}]}, "id_str": "845330366156210176", "sizes": {"s'
    'mall": {"h": 191, "resize": "fit", "w": 340}, "large": {"h": 576, "resize'
    '": "fit", "w": 1024}, "medium": {"h": 338, "resize": "fit", "w": 600}, "t'
    'humb": {"h": 150, "resize": "crop", "w": 150}}, "indices": [143, 166], "t'
    'ype": "video", "id": 845330366156210176, "media_url": "http://pbs.twimg.c'
    'om/ext_tw_video_thumb/845330366156210176/pu/img/NdRWqCDX-r734Vpd.jpg"}]},'
    ' "full_text": "Today, I was thrilled to announce a commitment of $25 BILL'
    'ION &amp; 20K AMERICAN JOBS over the next 4 years. THANK YOU Charter Comm'
    'unications! https://t.co/PLxUmXVl0h"}, "created_at": "Fri Mar 24 17:59:42'
    ' +0000 2017", "filter_level": "low", "in_reply_to_status_id_str": None, "'
    'place": None}')


def test_get_longtext_streaming_long(checker):
    assert checker.get_longtext(STREAMING_TWEET_LONG) == (
        "Today, I was thrilled to announce a commitment of $25 BILLION &amp; 2"
        "0K AMERICAN JOBS over the next 4 years. THANK YOU Charter Communicati"
        "ons! https://t.co/PLxUmXVl0h")


STREAMING_TWEET_SHORT = literal_eval(
    '{"contributors": None, "truncated": False, "text": "ObamaCare will explod'
    'e and we will all get together and piece together a great healthcare plan'
    ' for THE PEOPLE. Do not worry!", "is_quote_status": False, "in_reply_to_s'
    'tatus_id": None, "id": 845645916732358656, "favorite_count": 0, "source":'
    ' "<a href=\\"http://twitter.com/download/android\\" rel=\\"nofollow\\">Tw'
    'itter for Android</a>", "retweeted": False, "coordinates": None, "timesta'
    'mp_ms": "1490452672547", "entities": {"user_mentions": [], "symbols": [],'
    ' "hashtags": [], "urls": []}, "in_reply_to_screen_name": None, "id_str": '
    '"845645916732358656", "retweet_count": 0, "in_reply_to_user_id": None, "f'
    'avorited": False, "user": {"follow_request_sent": None, "profile_use_back'
    'ground_image": True, "default_profile_image": False, "id": 25073877, "ver'
    'ified": True, "profile_image_url_https": "https://pbs.twimg.com/profile_i'
    'mages/1980294624/DJT_Headshot_V2_normal.jpg", "profile_sidebar_fill_color'
    '": "C5CEC0", "profile_text_color": "333333", "followers_count": 27003111,'
    ' "profile_sidebar_border_color": "BDDCAD", "id_str": "25073877", "profile'
    '_background_color": "6D5C18", "listed_count": 67477, "profile_background_'
    'image_url_https": "https://pbs.twimg.com/profile_background_images/530021'
    '613/trump_scotland__43_of_70_cc.jpg", "utc_offset": -14400, "statuses_cou'
    'nt": 34649, "description": "45th President of the United States of Americ'
    'a", "friends_count": 43, "location": "Washington, DC", "profile_link_colo'
    'r": "0D5B73", "profile_image_url": "http://pbs.twimg.com/profile_images/1'
    '980294624/DJT_Headshot_V2_normal.jpg", "following": None, "geo_enabled": '
    'True, "profile_banner_url": "https://pbs.twimg.com/profile_banners/250738'
    '77/1489657715", "profile_background_image_url": "http://pbs.twimg.com/pro'
    'file_background_images/530021613/trump_scotland__43_of_70_cc.jpg", "name"'
    ': "Donald J. Trump", "lang": "en", "profile_background_tile": True, "favo'
    'urites_count": 46, "screen_name": "realDonaldTrump", "notifications": Non'
    'e, "url": None, "created_at": "Wed Mar 18 13:46:38 +0000 2009", "contribu'
    'tors_enabled": False, "time_zone": "Eastern Time (US & Canada)", "protect'
    'ed": False, "default_profile": False, "is_translator": False}, "geo": Non'
    'e, "in_reply_to_user_id_str": None, "lang": "en", "created_at": "Sat Mar '
    '25 14:37:52 +0000 2017", "filter_level": "low", "in_reply_to_status_id_st'
    'r": None, "place": None}')


def test_get_longtext_streaming_short(checker):
    assert checker.get_longtext(STREAMING_TWEET_SHORT) == (
        "ObamaCare will explode and we will all get together and piece togethe"
        "r a great healthcare plan for THE PEOPLE. Do not worry!")


def test_get_longtext_none(checker):
    assert checker.get_longtext(None) is None


def test_retrieve_wikidata_data(checker):
    assert checker.retrieve_wikidata_data(
        MID_TO_TICKER_QUERY % "/m/02y1vz") == [{
            "companyLabel": {
                "type": "literal",
                "value": "Facebook",
                "xml:lang": "en"},
            "rootLabel": {
                "type": "literal",
                "value": "Facebook Inc.",
                "xml:lang": "en"},
            "exchangeNameLabel": {
                "type": "literal",
                "value": "NASDAQ",
                "xml:lang": "en"},
            "tickerLabel": {
                "type": "literal",
                "value": "FB"}}]


def test_retrieve_wikidata_data_empty(checker):
    assert checker.retrieve_wikidata_data("") is None
