from functools import cached_property
import time

from confluent_kafka import avro
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroDeserializer

from src.crawler.models import Track
from src.crawler.schemas import SCHEMA_FOLDER
from src.configs.kafka import SCHEMA_REGISTRY
from src.configs.logger import get_logger


class BaseCrawler:
    CRAWLER_MAX_PAGES = 10_000

    # Base topics
    T_ALBUM = 'spotify.crawl.albums'
    T_ALBUM_TRACKS = 'spotify.crawl.album.tracks'
    T_ARTIST_ALBUMS = 'spotify.crawl.artist.albums'
    T_ARTIST_OFFICIAL = 'spotify.crawl.artist.official'
    T_ARTIST_WEB = 'spotify.crawl.artist.web'
    T_USER_PLAYLISTS = 'spotify.crawl.user.playlists'
    T_PLAYLIST = 'spotify.crawl.playlist'
    T_PLAYLIST_TRACKS = 'spotify.crawl.playlist.tracks'
    T_TRACK_OFFICIAL = 'spotify.crawl.track.official'
    T_TRACK_WEB = 'spotify.crawl.track.web'
    ALBUM_TOPICS = [T_ALBUM_TRACKS]
    ARTIST_TOPICS = [T_ARTIST_OFFICIAL, T_ARTIST_WEB, T_ARTIST_ALBUMS]
    PLAYLIST_TOPICS = [T_PLAYLIST, T_PLAYLIST_TRACKS]
    TRACK_TOPICS = [T_TRACK_WEB, T_TRACK_OFFICIAL]
    T_MONITOR_OBJECT = 'spotify.monitor.object'

    TOPICS = (
        # Base topics
        T_ARTIST_OFFICIAL, T_ARTIST_WEB, T_TRACK_OFFICIAL, T_TRACK_WEB, T_ALBUM_TRACKS, T_ARTIST_ALBUMS,
        T_USER_PLAYLISTS, T_PLAYLIST_TRACKS,

        # Monitor topics
        T_MONITOR_OBJECT
    )

    crawler_key_schema = avro.load(f'{SCHEMA_FOLDER}/crawler_key.avsc')
    artist_value_schema = avro.load(f'{SCHEMA_FOLDER}/artist_value.avsc')
    album_value_schema = avro.load(f'{SCHEMA_FOLDER}/album_value.avsc')
    monitor_value_schema = avro.load(f'{SCHEMA_FOLDER}/monitor_value.avsc')
    user_value_schema = avro.load(f'{SCHEMA_FOLDER}/user_value.avsc')
    playlist_value_schema = avro.load(f'{SCHEMA_FOLDER}/playlist_value.avsc')
    track_value_schema = avro.load(f'{SCHEMA_FOLDER}/track_value.avsc')

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self.schema_registry_client = SchemaRegistryClient(
            conf={'url': SCHEMA_REGISTRY})

    @cached_property
    def spotify_producer(self):
        from src.infratructure.kafka.producer import SpotifyProducer
        return SpotifyProducer()

    @cached_property
    def spotify_service(self):
        from src.crawler.services.spotify import SpotifyService
        return SpotifyService()

    def get_deserialized(self, schema_name: str = None) -> AvroDeserializer:
        schema_str = open(f'{SCHEMA_FOLDER}/{schema_name}.avsc').read()
        return AvroDeserializer(
            schema_registry_client=self.schema_registry_client,
            schema_str=schema_str
        )

    @staticmethod
    def time_millis():
        '''Use this function to get the key for Kafka Events'''
        return int(round(time.time() * 1000))

    def produce_track(self, track: Track):
        self.spotify_producer.produce(
            topic=self.T_TRACK_WEB,
            key={'timestamp': self.time_millis()},
            value={'track_id': track.track_id},
            key_schema=self.crawler_key_schema,
            value_schema=self.track_value_schema
        )
        if track.is_exists_column('album'):
            self.spotify_producer.produce(
                topic=self.T_ALBUM_TRACKS,
                key={'timestamp': self.time_millis()},
                value={'album_id': track.album.album_id},
                key_schema=self.crawler_key_schema,
                value_schema=self.album_value_schema
            )
        if track.is_exists_column('artist_track'):
            for artist_track in track.artist_track:
                self.spotify_producer.produce(
                    topic=self.T_ARTIST_WEB,
                    key={'timestamp': self.time_millis()},
                    value={'artist_id': artist_track.artist_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.artist_value_schema
                )
