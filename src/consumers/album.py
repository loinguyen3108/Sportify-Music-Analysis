from concurrent.futures import ThreadPoolExecutor
from functools import cached_property

from confluent_kafka.serialization import SerializationContext, MessageField
from spotipy.exceptions import SpotifyException

from src.configs.crawler import CRAWLER_SHOULD_REFRESH, MAX_WORKERS
from src.configs.kafka import CRAWL_STRATEGY
from src.crawler.base import BaseCrawler


class AlbumConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def album_deserializer(self):
        return self.get_deserialized(schema_name='album_value')

    def messages_handler(self, messages):
        album_ids = set()
        tracks_by_album_ids = set()
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue
            topic = message.topic()
            message_value = self.album_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            try:
                if topic == self.T_ALBUM:
                    album_ids.add(message_value['album_id'])
                if topic == self.T_ALBUM_TRACKS:
                    tracks_by_album_ids.add(message_value['album_id'])
            except SpotifyException as e:
                self.logger.error(e)
        if album_ids:
            self.spotify_service.crawl_albums(album_ids)
        if tracks_by_album_ids:
            with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
                executor.map(self.ingest_album_tracks, tracks_by_album_ids)

    def ingest_album_tracks(self, album_id: str):
        for tracks in self.spotify_service.crawl_tracks_by_album_id(
                album_id=album_id, max_pages=self.CRAWLER_MAX_PAGES,
                strategy=CRAWL_STRATEGY, refresh=CRAWLER_SHOULD_REFRESH
        ):
            for track in tracks or []:
                if not track:
                    continue
                self.produce_track(track)
