from concurrent.futures import ThreadPoolExecutor
from functools import cached_property

from confluent_kafka.serialization import SerializationContext, MessageField
from spotipy.exceptions import SpotifyException

from src.configs.kafka import CRAWL_STRATEGY
from src.crawler.base import BaseCrawler


class TrackConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def track_deserializer(self):
        return self.get_deserialized(schema_name='track_value')

    def messages_handler(self, messages):
        track_ids = set()
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue
            message_value = self.track_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            track_ids.add(message_value['track_id'])
        if track_ids:
            if CRAWL_STRATEGY == 'official_api':
                try:
                    self.spotify_service.crawl_tracks(track_ids)
                except SpotifyException as e:
                    self.logger.error(e)
            elif CRAWL_STRATEGY == 'web_api':
                with ThreadPoolExecutor(max_workers=10) as executor:
                    executor.map(self.ingest_track, track_ids)
            else:
                raise ValueError(f'Invalid crawl strategy: {CRAWL_STRATEGY}')

    def ingest_track(self, track_id: str):
        track = self.spotify_service.crawl_track(track_id)
        album_ids = []
        if track:
            self.spotify_producer.produce(
                topic=self.T_TRACK_OFFICIAL,
                key={'timestamp': self.time_millis()},
                value={'track_id': track.track_id},
                key_schema=self.crawler_key_schema,
                value_schema=self.track_value_schema
            )
            if track.is_exists_column('album'):
                album_ids.append(track.album.album_id)
            if track.is_exists_column('more_albums'):
                album_ids.extend([
                    album.album_id for album in track.more_albums])

        if album_ids:
            for album_id in set(album_ids):
                self.spotify_producer.produce(
                    topic=self.T_ALBUM,
                    key={'timestamp': self.time_millis()},
                    value={'album_id': album_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.album_value_schema
                )
