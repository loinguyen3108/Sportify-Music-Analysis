from functools import cached_property

from confluent_kafka.serialization import SerializationContext, MessageField
from spotipy.exceptions import SpotifyException

from src.crawler.models import Track
from src.crawler.base import BaseCrawler


class playlistConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def playlist_deserializer(self):
        return self.get_deserialized(schema_name='playlist_value')

    def messages_handler(self, messages):
        artist_ids = []
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue
            topic = message.topic()
            message_value = self.playlist_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            playlist_id = message_value['playlist_id']
            try:
                if topic == self.T_PLAYLIST:
                    self.ingest_playlist(playlist_id)
                if topic == self.T_PLAYLIST_TRACKS:
                    self.ingest_playlist_tracks(playlist_id)
            except SpotifyException as e:
                self.logger.error(e)
        if artist_ids:
            self.spotify_service.crawl_artists(artist_ids)

    def ingest_playlist(self, playlist_id: str):
        playlist = self.spotify_service.crawl_playlist(playlist_id)
        if not playlist:
            return
        if playlist.is_exists_column('tracks'):
            for track in playlist.tracks:
                if not track:
                    continue
                self.produce_track(track)

        self.logger.info(f'Ingested playlist: {playlist_id}')

    def ingest_playlist_tracks(self, playlist_id: str, offset: int = 0):
        for tracks in self.spotify_service.crawl_playlist_tracks(playlist_id, offset=offset):
            for track in tracks:
                if not track:
                    continue
                self.produce_track(track)
        self.logger.info(
            f'Ingested playlist tracks for playlist: {playlist_id}')
