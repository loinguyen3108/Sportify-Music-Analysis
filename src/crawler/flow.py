from functools import cached_property

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
            topics=self.ALBUM_TOPICS,
            group_id='album-consumer',
            messages_handler=AlbumConsumer().messages_handler,
            num_messages=1
        )

    @ cached_property
    def artist_consumer(self):
        from src.consumers.artist import ArtistConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=self.ARTIST_TOPICS,
            group_id='artist-consumer',
            messages_handler=ArtistConsumer().messages_handler,
            num_messages=50
        )

    @cached_property
    def playlist_consumer(self):
        from src.consumers.playlist import playlistConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=self.PLAYLIST_TOPICS,
            group_id='playlist-consumer',
            messages_handler=playlistConsumer().messages_handler,
            num_messages=1
        )

    @cached_property
    def track_consumer(self):
        from src.consumers.track import TrackConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=self.TRACK_TOPICS,
            group_id='track-consumer',
            messages_handler=TrackConsumer().messages_handler,
            num_messages=50
        )

    @cached_property
    def track_plays_count_consumer(self):
        from src.consumers.track_plays_count import TrackPlaysCountConsumer
        from src.infratructure.kafka.consumer import SpotifyConsumer
        return SpotifyConsumer(
            topics=[self.T_TRACK_PLAYS_COUNT],
            group_id='track_plays_count-consumer',
            messages_handler=TrackPlaysCountConsumer().messages_handler,
            num_messages=1
        )

    def _prepare_spotify_topics(self):
        for topic in self.TOPICS:
            self.spotify_producer.create_topic(topic)

        self.logger.info('Spotify topics prepared')

    def ingest_albums(self):
        self.album_consumer.consume_messages()

    def ingest_artists(self):
        self.artist_consumer.consume_messages()

    def ingest_playlists(self):
        self.playlist_consumer.consume_messages()

    def ingest_tracks(self):
        self.track_consumer.consume_messages()

    def ingest_track_plays_count(self):
        self.track_plays_count_consumer.consume_messages()

    def ingest_artists_by_tag(self, tag_name: str, offset: int = 0):
        if not tag_name:
            raise ValueError('tag_name is required')

        self.logger.info(f'Ingesting artists by tag: {tag_name}')
        for artists in self.spotify_service.search_artists(
                query=f'tag:{tag_name}', max_items=self.CRAWLER_MAX_ITEMS, offset=offset
        ):
            for artist in artists:
                self.spotify_producer.produce(
                    topic=self.T_ARTISTS,
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
        self.logger.info(f'Ingested artists by tag: {tag_name}')

    def ingest_playlists_by_tag(self, tag_name: str, offset: int = 0):
        if not tag_name:
            raise ValueError('tag_name is required')

        self.logger.info(f'Ingesting playlists by tag: {tag_name}')

        unique_users = set()
        for playlists in self.spotify_service.search_playlists(
                query=f'tag:{tag_name}', max_items=self.CRAWLER_MAX_ITEMS, offset=offset
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
        self.logger.info(f'Ingested playlists by tag: {tag_name}')
