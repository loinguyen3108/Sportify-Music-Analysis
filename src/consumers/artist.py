from functools import cached_property

from confluent_kafka.serialization import SerializationContext, MessageField
from spotipy.exceptions import SpotifyException

from src.crawler.base import BaseCrawler


class ArtistConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def artist_deserializer(self):
        return self.get_deserialized(schema_name='artist_value')

    def messages_handler(self, messages):
        artist_ids = []
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue
            topic = message.topic()
            message_value = self.artist_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            artist_id = message_value['artist_id']
            try:
                if topic == self.T_ARTIST_ALBUMS:
                    self.ingest_artist_albums(artist_id)
                if topic == self.T_ARTISTS:
                    artist_ids.append(artist_id)
            except SpotifyException as e:
                self.logger.error(e)
        if artist_ids:
            self.spotify_service.crawl_artists(artist_ids)

    def ingest_artist_albums(self, artist_id: str, offset: int = 0):
        if not artist_id:
            raise ValueError('artist_id is required')

        self.logger.info(f'Ingesting albums by artist: {artist_id}')

        artist_ids = []
        for albums in self.spotify_service.crawl_albums_by_artist_id(
                artist_id=artist_id, max_items=self.CRAWLER_MAX_ITEMS, offset=offset
        ):
            for album in albums or []:
                self.spotify_producer.produce(
                    topic=self.T_ALBUM_TRACKS,
                    key={'timestamp': self.time_millis()},
                    value={'album_id': album.album_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.album_value_schema
                )
                artist_ids.extend([
                    _id for _id in album.artist_ids if _id != artist_id])

        for artist_id in set(artist_ids):
            self.spotify_producer.produce(
                topic=self.T_ARTISTS,
                key={'timestamp': self.time_millis()},
                value={'artist_id': artist_id},
                key_schema=self.crawler_key_schema,
                value_schema=self.artist_value_schema
            )

        self.logger.info(f'Ingested albums by artist: {artist_id}')
