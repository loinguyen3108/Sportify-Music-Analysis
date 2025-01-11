from functools import cached_property

from src.configs.crawler import T_ARTIST, T_PLAYLIST, T_TRACK
from src.crawler.base import BaseCrawler
from src.configs.logger import get_logger


class CrawlerFlow(BaseCrawler):

    def __init__(self):
        self.logger = get_logger(self.__class__.__name__)
        self._prepare_spotify_topics()

    @cached_property
    def album_consumer(self):
        from src.consumers.album import AlbumConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=[self.T_ALBUM, self.T_ALBUM_TRACKS],
            group_id='album-consumer',
            messages_handler=AlbumConsumer().messages_handler,
            num_messages=200
        )

    @cached_property
    def artist_consumer(self):
        from src.consumers.artist import ArtistConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=[self.T_ARTIST_OFFICIAL, self.T_ARTIST_WEB],
            group_id='artist-consumer',
            messages_handler=ArtistConsumer().messages_artist_handler,
            num_messages=50
        )

    @cached_property
    def artist_albums_consumer(self):
        from src.consumers.artist import ArtistConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=[self.T_ARTIST_ALBUMS],
            group_id='artist-albums-consumer',
            messages_handler=ArtistConsumer().messages_artist_album_handler,
            num_messages=100
        )

    @ cached_property
    def monitor_consumer(self):
        from src.consumers.monitor import MonitorConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=[self.T_MONITOR_OBJECT],
            group_id='monitor-consumer',
            messages_handler=MonitorConsumer().messages_handler,
            num_messages=1000

        )

    @ cached_property
    def playlist_consumer(self):
        from src.consumers.playlist import PlaylistConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=self.PLAYLIST_TOPICS,
            group_id='playlist-consumer',
            messages_handler=PlaylistConsumer().messages_handler,
            num_messages=20
        )

    @ cached_property
    def track_consumer(self):
        from src.consumers.track import TrackConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=self.TRACK_TOPICS,
            group_id='track-consumer',
            messages_handler=TrackConsumer().messages_handler,
            num_messages=50
        )

    def _prepare_spotify_topics(self):
        for topic in self.TOPICS:
            self.spotify_producer.create_topic(topic)

        self.logger.info('Spotify topics prepared')

    def ingest_albums(self):
        self.album_consumer.consume_messages()

    def ingest_artists(self):
        self.artist_consumer.consume_messages()

    def ingest_artist_albums(self):
        self.artist_albums_consumer.consume_messages()

    def ingest_playlists(self):
        self.playlist_consumer.consume_messages()

    def ingest_tracks(self):
        self.track_consumer.consume_messages()

    def ingest_albums_by_query(self, query: str, offset: int = 0):
        if not query:
            raise ValueError('query is required')

        self.logger.info(f'Ingesting albums by query: {query}')
        for albums in self.spotify_service.search_albums(
                query=f'{query}', max_pages=20, offset=offset
        ):
            for album in albums:
                self.spotify_producer.produce(
                    topic=self.T_ALBUM_TRACKS,
                    key={'timestamp': self.time_millis()},
                    value={'album_id': album.album_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.album_value_schema
                )
        self.spotify_producer.close()
        self.logger.info(f'Ingested albums by query: {query}')

    def ingest_artists_by_query(self, query_name: str, offset: int = 0):
        if not query_name:
            raise ValueError('query_name is required')

        self.logger.info(f'Ingesting artists by query: {query_name}')
        for artists in self.spotify_service.search_artists(
                query=f'{query_name}', max_pages=20, offset=offset
        ):
            for artist in artists:
                self.spotify_producer.produce(
                    topic=self.T_ARTIST_WEB,
                    key={'timestamp': self.time_millis()},
                    value={'artist_id': artist.artist_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.artist_value_schema
                )
                self.spotify_producer.produce(
                    topic=self.T_ARTIST_ALBUMS,
                    key={'timestamp': self.time_millis()},
                    value={'artist_id': artist.artist_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.artist_value_schema
                )
        self.spotify_producer.close()
        self.logger.info(f'Ingested artists by query: {query_name}')

    def ingest_playlists_by_query(self, query: str, offset: int = 0):
        if not query:
            raise ValueError('query is required')

        self.logger.info(f'Ingesting playlists by query: {query}')

        unique_users = set()
        for playlists in self.spotify_service.search_playlists(
                query=f'{query}', max_pages=20, offset=offset
        ):
            for playlist in playlists:
                self.spotify_producer.produce(
                    topic=self.T_PLAYLIST,
                    key={'timestamp': self.time_millis()},
                    value={'playlist_id': playlist.playlist_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.playlist_value_schema
                )
                self.spotify_producer.produce(
                    topic=self.T_PLAYLIST_TRACKS,
                    key={'timestamp': self.time_millis()},
                    value={'playlist_id': playlist.playlist_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.playlist_value_schema
                )
                unique_users.add(playlist.user_id)

        for user_id in unique_users:
            self.spotify_producer.produce(
                topic=self.T_USER_PLAYLISTS,
                key={'timestamp': self.time_millis()},
                value={'user_id': user_id},
                key_schema=self.crawler_key_schema,
                value_schema=self.user_value_schema
            )

        self.spotify_producer.close()
        self.logger.info(f'Ingested playlists by query: {query}')

    def ingest_tracks_by_query(self, query: str, offset: int = 0):
        if not query:
            raise ValueError('query is required')

        self.logger.info(f'Ingesting tracks by query: {query}')
        for tracks in self.spotify_service.search_tracks(
                query=f'{query}', max_pages=20, offset=offset
        ):
            for track in tracks:
                self.produce_track(track)
        self.spotify_producer.close()
        self.logger.info(f'Ingested tracks by query: {query}')

    def monitor_object_consumer(self):
        self.monitor_consumer.consume_messages()

    def produce_monitor_messages(self):
        self.logger.info('Producing monitor messages...')
        for obj_name in [T_ARTIST, T_PLAYLIST, T_TRACK]:
            object_ids = self.spotify_service.get_monitor_messages(
                object_name=obj_name)
            if not object_ids:
                continue

            for obj_id in object_ids:
                self.spotify_producer.produce(
                    topic=self.T_MONITOR_OBJECT,
                    key={'timestamp': self.time_millis()},
                    value={'object_name': obj_name,
                           'object_value': obj_id},
                    key_schema=self.crawler_key_schema,
                    value_schema=self.monitor_value_schema
                )
