import nltk
import os
import psycopg2
import functools
import operator
import re
import logging
import sys

from typing import List, Optional
from psycopg2.extras import execute_values
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import Word
from dataclasses import dataclass
from datetime import datetime
from dotenv import load_dotenv

load_dotenv()

nltk.download('wordnet')
nltk.download('punkt')
nltk.download('averaged_perceptron_tagger')
nltk.download('stopwords')

STOPWORDS = stopwords.words('english')


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
fmt = logging.Formatter("%(asctime)s %(levelname)-8s %(name)-30s %(message)s")
sh = logging.StreamHandler(sys.stderr)
sh.setFormatter(fmt)
logger.addHandler(sh)


class Settings:
    BASE_DIR = os.path.dirname(os.path.realpath(__file__))

    @staticmethod
    def get_db_kwargs() -> dict:
        return {
            "user": os.getenv("DB_USER"),
            "password": os.getenv("DB_PASSWORD"),
            "host": os.getenv("DB_HOST"),
            "port": int(os.getenv("DB_PORT")),
            "database": os.getenv("DB_NAME")
        }


@dataclass
class Tweet:
    id: int
    status_id: int
    text: str
    url: str
    favorite_count: int
    retweet_count: int
    trend: str
    normalized_trend: str
    author: str
    language_code: str
    hashtags: List[str]
    tagged_persons: List[str]
    time_collected: datetime
    date_label: datetime


@dataclass
class NGram:
    dimension: int
    sequence: List[str]
    frequency: int
    hashtag: str
    hashtag_is_in_ngram: bool


class DB:
    DB: Optional["DB"] = None

    @classmethod
    def get_instance(cls) -> "DB":
        if not cls.DB:
            cls.DB = cls()
        return cls.DB

    def __init__(self):
        self._connect()

    def _connect(self):
        self.conn = psycopg2.connect(**Settings.get_db_kwargs())
        self.cur = self.conn.cursor()
        self.conn.autocommit = True


class ORM:
    db: DB = DB.get_instance().DB

    @classmethod
    def fetch_tweets_bodies_by_hashtag(cls, hashtag: str) -> List[Tweet]:
        logger.info("Start Fetching Tweets from Postgres")
        tweets = []
        for i in range(1, 7):
            sql = f"SELECT {','.join(Tweet.__annotations__.keys())} FROM tweet WHERE hashtags[%s]=%s AND language_code=%s"
            cls.db.cur.execute(sql, (i, hashtag, "en", ))
            tweets += [Tweet(*row) for row in cls.db.cur.fetchall()]
        taken_ids = []
        unique_tweets = []
        for tweet in tweets:
            if tweet.id in taken_ids:
                continue
            unique_tweets.append(tweet)
            taken_ids.append(tweet.id)
        logger.info(f"Number of fetched tweets: {len(unique_tweets)}")
        return unique_tweets

    @classmethod
    def insert_ngrams(cls, ngrams: List[NGram]):
        logger.info(f"Insert  {len(ngrams)} ngrams")
        sql = f"INSERT INTO ngram ({','.join(NGram.__annotations__.keys())}) VALUES %s"
        execute_values(cls.db.cur, sql, [tuple(ngram.__dict__.values()) for ngram in ngrams])

    @classmethod
    def remove_ngrams(cls, dimension: int, hashtag: str):
        logger.info(f"Remove ngrams for hashtag {hashtag} with dimension {dimension}")
        sql = "DELETE FROM ngram WHERE dimension=%s and hashtag=%s"
        cls.db.cur.execute(sql, (dimension, hashtag, ))


def _clean_text(text: str):
    # remove and replace all urls
    text = re.sub(r'http\S+', ' ', text)

    # remove and replace none alphanumerical letters
    text = re.sub(r'\W+', ' ', text.lower())

    words = []
    for word in text.split():
        if word in STOPWORDS:
            continue
        words.append(Word(word).lemmatize())
    return " ".join(words)


def get_2_gram_df(corpus: List[str], hashtag: str) -> List[NGram]:
    logger.info("Start creating bigrams")

    def right_types_2_gram(ngram):
        if '-pron-' in ngram or 't' in ngram:
            return False
        for word in ngram:
            if word in STOPWORDS or word.isspace():
                return False
        acceptable_types = ('JJ', 'JJR', 'JJS', 'NN', 'NNS', 'NNP', 'NNPS')
        second_type = ('NN', 'NNS', 'NNP', 'NNPS')
        tags = nltk.pos_tag(ngram)
        if tags[0][1] in acceptable_types and tags[1][1] in second_type:
            return True
        else:
            return False

    logger.debug("Cleaning corpus")
    cleaned_tweets = [_clean_text(tweet) for tweet in corpus]

    logger.debug("Tokenize corpus")
    tokens = [word_tokenize(tweet) for tweet in cleaned_tweets]

    # flatten_list
    tokens = list(functools.reduce(operator.concat, tokens))

    logger.debug("Find Bigrams")
    bigram_finder = nltk.collocations.BigramCollocationFinder.from_words(tokens)
    bigram_freq = list(bigram_finder.ngram_fd.items())

    logger.info("Start Filtering Bigrams TODO: Change threshold to env Variable.")
    lemmatized_hashtag = Word(hashtag.lower()).lemmatize()
    return [
        NGram(
            dimension=2,
            hashtag=hashtag,
            frequency=bigram[1],
            sequence=list(bigram[0]),
            hashtag_is_in_ngram=lemmatized_hashtag in bigram[0],
        ) for bigram in bigram_freq
        if right_types_2_gram(bigram[0]) and bigram[1] > 3      # TODO change to env Variable
    ]


def get_hashtags_from_tweets(tweets: List[Tweet]) -> List[str]:
    logger.info("Getting Hashtags from Corpus")
    hashtag_list = [entity.hashtags for entity in tweets if entity.hashtags != []]

    # flatten list
    hashtag_list = list(functools.reduce(operator.concat, hashtag_list))

    # create unique lists of users
    hashtag_list = list(set(hashtag_list))

    logger.info(f"{len(hashtag_list)} Found")
    return hashtag_list


def ngram_runner(tweets: List[Tweet], hashtag: str):
    tweet_bodies = [tweet.text for tweet in tweets]
    bigrams = get_2_gram_df(tweet_bodies, hashtag)
    ORM.remove_ngrams(dimension=2, hashtag=hashtag)
    ORM.insert_ngrams(bigrams)


def runner():
    hashtag = 'globalwarming'
    logger.info(f"Start creating ngrams for hashtag {hashtag} and all references")
    tweets = ORM.fetch_tweets_bodies_by_hashtag(hashtag)
    hashtags_in_tweets = get_hashtags_from_tweets(tweets)
    logger.info(f"{len(hashtag) - 1} references found")
    if len(tweets) >= 50:
        ngram_runner(tweets, hashtag)
    else:
        logger.info(f"Not enough text bodies for creating valuable ngrams ({len(tweets)})")

    # Referenced Hashtags
    for hashtag_ in hashtags_in_tweets:
        if hashtag_ == hashtag:
            continue
        tweets = ORM.fetch_tweets_bodies_by_hashtag(hashtag_)
        if len(tweets) >= 50:
            ngram_runner(tweets, hashtag_)
        else:
            logger.info(f"Not enough text bodies for creating valuable ngrams ({len(tweets)})")


if __name__ == "__main__":
    runner()
