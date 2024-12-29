from functools import cached_property
from typing import List

from confluent_kafka.serialization import SerializationContext, MessageField
from spotipy.exceptions import SpotifyException

from src.crawler.base import BaseCrawler


class TrackConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def track_deserializer(self):
        return self.get_deserialized(schema_name='track_value')

    def messages_handler(self, messages):
        track_ids = []
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue
            message_value = self.track_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            track_id = message_value['track_id']
            track_ids.append(track_id)
        if track_ids:
            try:
                self.ingest_tracks(track_ids)
            except SpotifyException as e:
                self.logger.error(e)

    def ingest_tracks(self, track_ids: List[str]):
        tracks = self.spotify_service.crawl_tracks(track_ids)
        for track in tracks or []:
            if not track:
                continue
            self.spotify_producer.produce(
                topic=self.T_TRACK_PLAYS_COUNT,
                key={'timestamp': self.time_millis()},
                value={'track_id': track.track_id},
                key_schema=self.crawler_key_schema,
                value_schema=self.track_value_schema
            )
