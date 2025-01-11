from concurrent.futures import ThreadPoolExecutor
from functools import cached_property

from confluent_kafka.serialization import SerializationContext, MessageField
from spotipy.exceptions import SpotifyException

from src.configs.kafka import CRAWL_STRATEGY
from src.crawler.base import BaseCrawler


class ArtistConsumer(BaseCrawler):
    def __init__(self):
        super().__init__()

    @cached_property
    def artist_deserializer(self):
        return self.get_deserialized(schema_name='artist_value')

    def messages_artist_handler(self, messages):
        artist_ids = set()
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue

            message_value = self.artist_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            artist_id = message_value['artist_id']
            artist_ids.add(artist_id)
        if CRAWL_STRATEGY == 'official_api':
            self.spotify_service.crawl_artists(artist_ids)
        elif CRAWL_STRATEGY == 'web_api':
            with ThreadPoolExecutor(max_workers=100) as executor:
                executor.map(self.ingest_artist_web, artist_ids)
        else:
            raise ValueError(f'Invalid crawl strategy: {CRAWL_STRATEGY}')

    def messages_artist_album_handler(self, messages):
        artist_ids = set()
        for message in messages:
            if message is None:
                continue
            if message.error():
                self.logger.error(message.error())
                continue

            message_value = self.artist_deserializer(
                message.value(), SerializationContext(message.topic(), MessageField.VALUE))
            artist_id = message_value['artist_id']
            artist_ids.add(artist_id)
        with ThreadPoolExecutor(max_workers=100) as executor:
            executor.map(self.ingest_artist_albums, artist_ids)

    def ingest_artist_web(self, artist_id: str):
        artist = self.spotify_service.crawl_artist(artist_id)
        if artist:
            self.spotify_producer.produce(
                topic=self.T_ARTIST_OFFICIAL,
                key={'timestamp': self.time_millis()},
                value={'artist_id': artist.artist_id},
                key_schema=self.crawler_key_schema,
                value_schema=self.artist_value_schema
            )
            if artist.is_exists_column('albums'):
                for album in artist.albums:
                    self.spotify_producer.produce(
                        topic=self.T_ALBUM_TRACKS,
                        key={'timestamp': self.time_millis()},
                        value={'album_id': album.album_id},
                        key_schema=self.crawler_key_schema,
                        value_schema=self.album_value_schema
                    )
            if artist.is_exists_column('playlist_ids'):
                for playlist_id in artist.playlist_ids:
                    self.spotify_producer.produce(
                        topic=self.T_PLAYLIST_TRACKS,
                        key={'timestamp': self.time_millis()},
                        value={'playlist_id': playlist_id},
                        key_schema=self.crawler_key_schema,
                        value_schema=self.playlist_value_schema
                    )
            if artist.is_exists_column('track_ids'):
                for track_id in artist.track_ids:
                    self.spotify_producer.produce(
                        topic=self.T_TRACK_WEB,
                        key={'timestamp': self.time_millis()},
                        value={'track_id': track_id},
                        key_schema=self.crawler_key_schema,
                        value_schema=self.track_value_schema
                    )

    def ingest_artist_albums(self, artist_id: str, offset: int = 0):
        if not artist_id:
            raise ValueError('artist_id is required')

        self.logger.info(f'Ingesting albums by artist: {artist_id}')

        artist_ids = []
        for albums in self.spotify_service.crawl_albums_by_artist_id(
                artist_id=artist_id, max_pages=self.CRAWLER_MAX_PAGES, offset=offset,
                strategy=CRAWL_STRATEGY
        ):
            for album in albums or []:
                self.spotify_producer.produce(
                    topic=self.T_ALBUM_TRACKS,
                    key={'timestamp': self.time_millis()},
                    value={'album_id': album.album_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.album_value_schema
                )
                if album.is_exists_column('artist_albums'):
                    artist_ids.extend([
                        artist.artist_id for artist in album.artist_albums])

        for artist_id in set(artist_ids):
            self.spotify_producer.produce(
                topic=self.T_ARTIST_WEB,
                key={'timestamp': self.time_millis()},
                value={'artist_id': artist_id},
                key_schema=self.crawler_key_schema,
                value_schema=self.artist_value_schema
            )

        self.logger.info(f'Ingested albums by artist: {artist_id}')
