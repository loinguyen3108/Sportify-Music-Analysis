from functools import cached_property

from confluent_kafka.serialization import SerializationContext, MessageField
from spotipy.exceptions import SpotifyException

from src.crawler.base import BaseCrawler


class AlbumConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def album_deserializer(self):
        return self.get_deserialized(schema_name='album_value')

    def messages_handler(self, messages):
        artist_ids = []
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue
            topic = message.topic()
            message_value = self.album_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            album_id = message_value['album_id']
            try:
                if topic == self.T_ALBUM_TRACKS:
                    self.ingest_album_tracks(album_id)
            except SpotifyException as e:
                self.logger.error(e)
        if artist_ids:
            self.spotify_service.crawl_artists(artist_ids)

    def ingest_album_tracks(self, album_id: str):
        for tracks in self.spotify_service.crawl_tracks_by_album_id(
                album_id=album_id, max_items=self.CRAWLER_MAX_ITEMS
        ):
            for track in tracks or []:
                if not track:
                    continue
                self.produce_track(track)
