import nltk
import os
import io
import psycopg2
import functools
import operator
import re
import logging
import sys
import uuid

import avro.schema
from avro.io import DatumReader, BinaryDecoder
from time import sleep
from typing import List, Optional, Callable
from neo4j import GraphDatabase
from neo4j.work.transaction import Transaction
from psycopg2.extras import execute_values
from nltk.corpus import stopwords
from nltk.tokenize import word_tokenize
from textblob import Word
from dataclasses import dataclass
from datetime import datetime
from dotenv import load_dotenv
from confluent_kafka import Consumer


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

    @staticmethod
    def get_kafka_consumer_kwargs() -> dict:
        return {
            "bootstrap.servers": os.getenv("KAFKA_CLUSTER_SERVER"),
            "group.id": f"ngrams-{uuid.uuid4().hex[:6]}",
            'auto.offset.reset': 'latest',
            'security.protocol': 'SASL_SSL',
            'sasl.mechanisms': 'PLAIN',
            'sasl.username': os.getenv("KAFKA_CLUSTER_API_KEY"),
            'sasl.password': os.getenv("KAFKA_CLUSTER_API_SECRET")
        }

    @staticmethod
    def get_neo4j_kwargs() -> dict:
        return {
            "user": os.getenv("NEO4J_USER"),
            "password": os.getenv("NEO4J_PASSWORD"),
            "uri": os.getenv("NEO4J_URI")
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
    q: str
    type: str


@dataclass
class SyncJob:
    id: int
    timestamp: datetime
    q: str
    type: str


class Neo4J:
    Neo4J: Optional["Neo4J"] = None

    @classmethod
    def get_instance(cls) -> "Neo4J":
        if not cls.Neo4J:
            cls.Neo4J = cls()
        return cls.Neo4J

    def __init__(self):
        self._connect()

    def _connect(self):
        neo4j_kwargs = Settings.get_neo4j_kwargs()
        self.driver = GraphDatabase.driver(
            neo4j_kwargs["uri"],
            auth=(neo4j_kwargs["user"], neo4j_kwargs["password"])
        )

    @classmethod
    def exec(cls, executable: Callable, **kwargs):
        neo4j = cls.get_instance().Neo4J
        with neo4j.driver.session() as session:
            return session.write_transaction(executable, **kwargs)


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


class KafkaConnector:
    kafka_connector: Optional["KafkaConnector"] = None

    parser = avro.schema.parse(open(os.path.join(Settings.BASE_DIR, "avro", "schema-pg-sync-value-v1.avsc")).read())

    sync_reader = DatumReader(
        avro.schema.parse(
            open(os.path.join(Settings.BASE_DIR, "avro", "schema-pg-sync-value-v1.avsc")).read()
        )
    )

    @classmethod
    def get_instance(cls) -> "KafkaConnector":
        if not cls.kafka_connector:
            cls.kafka_connector = cls()
        return cls.kafka_connector

    def __init__(self):
        self.consumer = Consumer(
            Settings.get_kafka_consumer_kwargs()
        )

    @staticmethod
    def decode(reader: DatumReader, message: bytes) -> SyncJob:
        message_bytes = io.BytesIO(message)

        # LMAA!!! Ref: https://newbedev.com/how-to-decode-deserialize-avro-with-python-from-kafka
        # It's a crap!
        message_bytes.seek(5)

        decoder = BinaryDecoder(message_bytes)
        value = reader.read(decoder)
        return SyncJob(**value)


class ORM:
    db: DB = DB.get_instance().DB

    @classmethod
    def _clean_tweets(cls, tweets: List[Tweet]) -> List[Tweet]:
        logger.info("Start cleaning Tweets")
        taken_ids = []
        processed_texts = []
        unique_tweets = []
        for tweet in tweets:
            if tweet.id in taken_ids or tweet.text in processed_texts:
                continue
            if len(tweet.hashtags) > 7:
                # TODO - make threshold to env
                logger.info("To Many Hashtag References. TODO: Make this as an env var")
                continue
            processed_texts.append(tweet.text)
            unique_tweets.append(tweet)
            taken_ids.append(tweet.id)
        return unique_tweets

    @classmethod
    def fetch_tweets_bodies_by_hashtag(cls, hashtag: str) -> List[Tweet]:
        logger.info("Start Fetching Tweets from Postgres by Hashtag")
        tweets = []
        # TODO improve Range
        for i in range(1, 9):
            sql = f"SELECT {','.join(Tweet.__annotations__.keys())} FROM tweet WHERE hashtags[%s]=%s AND language_code=%s"
            cls.db.cur.execute(sql, (i, hashtag, "en",))
            try:
                tweets += [Tweet(*row) for row in cls.db.cur.fetchall()]
            except psycopg2.ProgrammingError as e:
                logger.warning(str(e))
        unique_tweets = cls._clean_tweets(tweets)
        logger.info(f"Number of fetched tweets: {len(unique_tweets)}")
        return unique_tweets

    @classmethod
    def fetch_tweets_bodies_by_author(cls, author: str) -> List[Tweet]:
        logger.info("Start Fetching Tweets from Postgres By Author")
        sql = f"SELECT {','.join(Tweet.__annotations__.keys())} FROM tweet WHERE author=%s AND language_code=%s"
        cls.db.cur.execute(sql, (author, "en",))
        tweets = [Tweet(*row) for row in cls.db.cur.fetchall()]
        unique_tweets = cls._clean_tweets(tweets)
        logger.info(f"Number of fetched tweets: {len(unique_tweets)}")
        return unique_tweets

    @classmethod
    def get_tweet_authors(cls):
        sql = "SELECT DISTINCT(author) FROM tweet"
        cls.db.cur.execute(sql)
        return [e[0] for e in cls.db.cur.fetchall()]

    @classmethod
    def insert_ngrams(cls, ngrams: List[NGram]):
        logger.info(f"Insert  {len(ngrams)} ngrams")
        sql = f"INSERT INTO ngram ({','.join(NGram.__annotations__.keys())}) VALUES %s"
        execute_values(cls.db.cur, sql, [tuple(ngram.__dict__.values()) for ngram in ngrams])

    @classmethod
    def remove_ngrams(cls, dimension: int, q: str, type: str):
        logger.info(f"Remove ngrams for Type {type} q {q} with dimension {dimension}")
        sql = "DELETE FROM ngram WHERE dimension=%s AND q=%s AND type=%s"
        cls.db.cur.execute(sql, (dimension, q, type, ))

    @classmethod
    def get_all_hashtags(cls) -> List[str]:
        sql = "SELECT hashtag FROM hashtag"
        cls.db.cur.execute(sql)
        return [e[0] for e in cls.db.cur.fetchall()]

    @classmethod
    def get_direct_related_tweets_by_hashtag_from_neo4j(cls, tx: Transaction, hashtags: List[str]) -> List[str]:
        logger.info(f"Fetch tweets from neo for hashtag list with length {len(hashtags)}")
        where_params_lol = [f"h.hashtag='{hashtag}'" for hashtag in hashtags]
        query = f"""
            MATCH (h:Hashtag)-->(t:Tweet)
            WHERE {" OR ".join(where_params_lol)}
            WITH DISTINCT t as t_
            RETURN t_.text
        """
        result = tx.run(query)
        results = [e["t_.text"] for e in result]
        logger.info(f"No of fetched tweets: {len(results)}")
        return results

    @classmethod
    def get_related_hashtag_list_by_hashtag_from_neo4j(cls, tx: Transaction, hashtags: List[str]) -> List[str]:
        logger.info(f"Fetch related hashtags from neo for hashtag list with length {len(hashtags)}")
        where_params_lol = [f"h0.hashtag='{hashtag}'" for hashtag in hashtags]
        query = f"""
            MATCH (h0:Hashtag)-->(t:Tweet)<--(h:Hashtag)
            WHERE {" OR ".join(where_params_lol)}
            WITH DISTINCT h AS unique_h, COUNT(t) AS tweet_count
            WHERE tweet_count > 5
            RETURN unique_h.hashtag
        """
        result = tx.run(query)
        results = list(set([e["unique_h.hashtag"] for e in result] + hashtags))
        logger.info(f"No of fetched hashtags: {len(results)}")
        return results

    @classmethod
    def ngram_is_pure_hashtag(cls, hashtags: List[str]):
        sql = "SELECT COUNT(*) FROM hashtag WHERE hashtag IN %s"
        cls.db.cur.execute(sql, (hashtags,))
        matches = cls.db.cur.fetchone()[0]
        return matches == hashtags

    @classmethod
    def get_relevant_hashtags_from_hashtag_list_neo4j(cls, tx: Transaction, root_hashtag: str):
        logger.info(f"Start fetching relevant hashtags from neo4j for {root_hashtag}")
        query = """
            MATCH (h0:Hashtag)-->(t:Tweet)<--(h:Hashtag)
            WITH  h, h0, count((h0)-->(t)) as match_count
            WHERE h0.hashtag=$hashtag
            RETURN h.hashtag
            ORDER BY match_count DESC
            LIMIT 25
        """
        result = tx.run(query, hashtag=root_hashtag)
        results = [e["h.hashtag"] for e in result]
        logger.info(f"Fetching relevant hashtag done. Found {len(results)} hashtags")
        return results


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


def get_2_gram_df(corpus: List[str], q: str, type: str) -> List[NGram]:
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
        return tags[0][1] in acceptable_types and tags[1][1] in second_type

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
    return [
        NGram(
            dimension=2,
            q=q,
            frequency=bigram[1],
            sequence=list(bigram[0]),
            type=type
        ) for bigram in bigram_freq
        if right_types_2_gram(bigram[0]) and bigram[1] > 3      # TODO change to env Variable
    ]


def get_3_gram_df(corpus: List[str], q: str, type: str) -> List[NGram]:
    # TODO - this method is redundant and could be put together with 2 gram
    #        the only difference here is the filter method and dimension
    logger.info("Start creating trigrams")

    def right_types_3_gram(ngram):
        if '-pron-' in ngram or 't' in ngram:
            return False
        for word in ngram:
            if word in STOPWORDS or word.isspace():
                return False
        first_type = ('JJ', 'JJR', 'JJS', 'NN', 'NNS', 'NNP', 'NNPS')
        third_type = ('JJ', 'JJR', 'JJS', 'NN', 'NNS', 'NNP', 'NNPS')
        tags = nltk.pos_tag(ngram)
        return tags[0][1] in first_type and tags[2][1] in third_type

    logger.debug("Cleaning corpus")
    cleaned_tweets = [_clean_text(tweet) for tweet in corpus]

    logger.debug("Tokenize corpus")
    tokens = [word_tokenize(tweet) for tweet in cleaned_tweets]

    # flatten_list
    tokens = list(functools.reduce(operator.concat, tokens))

    logger.debug("Find Trigrams")
    trigram_finder = nltk.collocations.TrigramCollocationFinder.from_words(tokens)
    trigram_freq = list(trigram_finder.ngram_fd.items())

    logger.info("Start Filtering Bigrams TODO: Change threshold to env Variable.")
    return [
        NGram(
            dimension=3,
            q=q,
            frequency=trigram[1],
            sequence=list(trigram[0]),
            type=type
        ) for trigram in trigram_freq
        if right_types_3_gram(trigram[0]) and trigram[1] > 3 and not ORM.ngram_is_pure_hashtag(trigram[0])      # TODO change to env Variable
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


def ngram_runner(tweets: List[Tweet], q: str, type: str):
    tweet_bodies = [tweet.text for tweet in tweets]
    bigrams = get_2_gram_df(tweet_bodies, q=q, type=type)
    trigrams = get_3_gram_df(tweet_bodies, q=q, type=type)
    ORM.remove_ngrams(dimension=2, q=q, type=type)
    ORM.remove_ngrams(dimension=3, q=q, type=type)
    ORM.insert_ngrams(bigrams + trigrams)


def hashtag_job_runner(sync_job: SyncJob):
    hashtag = sync_job.q
    logger.info(f"Start creating ngrams for hashtag {hashtag} and all references")
    tweets = ORM.fetch_tweets_bodies_by_hashtag(hashtag)

    if len(tweets) == 0:
        logger.warning(f"No Tweets for hashtag {hashtag}")
        return

    if len(tweets) >= 50:
        ngram_runner(tweets, q=hashtag, type="hashtag")
    else:
        logger.info(f"Not enough text bodies for creating valuable ngrams ({len(tweets)})")

    relevant_hashtags = Neo4J.exec(ORM.get_relevant_hashtags_from_hashtag_list_neo4j, root_hashtag=hashtag)

    # Referenced Hashtags
    for i, hashtag_ in enumerate(relevant_hashtags):
        logger.info(f"Related hashtag {hashtag_} - {i + 1} / {len(relevant_hashtags)}")
        if hashtag_ == hashtag:
            continue
        tweets = ORM.fetch_tweets_bodies_by_hashtag(hashtag_)
        if len(tweets) >= 50:
            ngram_runner(tweets, q=hashtag_, type="hashtag")
        else:
            logger.info(f"Not enough text bodies for creating valuable ngrams ({len(tweets)})")


def bubble_job_runner(sync_job: SyncJob):
    # TODO Optimize code - too many duplicates!

    hashtags = sync_job.q.split(",")
    logger.info(f"Start creating ngrams for bubble with hashtags {sync_job.q} and all references")

    q = f"DIRECT {sync_job.q}"
    logger.info("Start Process for direct related tweets for Hashtag list")
    tweets = Neo4J.exec(ORM.get_direct_related_tweets_by_hashtag_from_neo4j, hashtags=hashtags)
    trigrams = get_3_gram_df(tweets, q=q, type=sync_job.type)
    ORM.remove_ngrams(dimension=3, q=q, type=sync_job.type)
    ORM.insert_ngrams(trigrams)

    logger.info("Start Process for indirect related tweets for Hashtag list")
    related_hashtags = Neo4J.exec(ORM.get_related_hashtag_list_by_hashtag_from_neo4j, hashtags=hashtags)
    q = f"RELATED {sync_job.q}"
    tweets = Neo4J.exec(ORM.get_direct_related_tweets_by_hashtag_from_neo4j, hashtags=related_hashtags)
    trigrams = get_3_gram_df(tweets, q=q, type=sync_job.type)
    ORM.remove_ngrams(dimension=3, q=q, type=sync_job.type)
    ORM.insert_ngrams(trigrams)


def runner(sync_job: SyncJob):
    if sync_job.type == "hashtag":
        return hashtag_job_runner(sync_job)
    elif sync_job.type == "bubble":
        return bubble_job_runner(sync_job)
    else:
        return


def _initial_runner():
    hashtags = ORM.get_all_hashtags()
    for hashtag in hashtags:
        tweets = ORM.fetch_tweets_bodies_by_hashtag(hashtag)
        if len(tweets) >= 15:
            ngram_runner(tweets, q=hashtag, type="hashtag")
        else:
            logger.info(f"Not enough text bodies for creating valuable ngrams ({len(tweets)})")

    authors = ORM.get_tweet_authors()
    for author in authors:
        tweets = ORM.fetch_tweets_bodies_by_author(author)
        if len(tweets) >= 10:
            ngram_runner(tweets, q=author, type="author")
        else:
            logger.info(f"Not enough text bodies for creating valuable ngrams ({len(tweets)})")


def event_listener():
    consumer = KafkaConnector.get_instance().kafka_connector.consumer
    consumer.subscribe(["pg-sync"])
    while True:
        msg = consumer.poll(timeout=1)
        if msg is None:
            logger.info("No Message Received. Wait for polling.")
            continue
        elif msg.error():
            logger.error(msg.error())
        else:
            sync_job = KafkaConnector.decode(
                reader=KafkaConnector.sync_reader,
                message=msg.value()
            )
            logger.info(f"Job w. id {sync_job.id} received")
            runner(sync_job)


if __name__ == "__main__":
    while True:
        try:
            event_listener()
        except Exception as e:
            logger.exception(str(e))
        logger.info("Wait for reconnection")
        sleep(60)
