from bs4 import BeautifulSoup
from datetime import date
import random
import time
from typing import Any, List, Tuple, Union

import requests
from requests.exceptions import Timeout, RequestException
from spotipy import Spotify
from spotipy.oauth2 import SpotifyClientCredentials

from src.configs.crawler import SPOTIFY_CLIENT_ID, SPOTIFY_CLIENT_SECRET, \
    SPOTIFY_WEB_API, T_ARTIST, T_PLAYLIST
from src.crawler.models import Album, Artist, CrawlerTracking, Playlist, Track, User
from src.infratructure.postgres.repository import Repository
from src.crawler.services.base import BaseService
from src.crawler.services.parser import parse_albums_response, parse_artist, \
    parse_playlist_detail, parse_playlist_tracks_response, parse_search_artists_response, \
    parse_search_playlists_response, parse_track_detail, parse_tracks_from_album, \
    parse_user_playlists_response


class SpotifyService(BaseService):
    MIN_DELAY = 1
    MAX_DELAY = 5

    DEFAULT_SEARCH_LIMIT = 50
    DEFAULT_MAX_ITEMS = 10_000_000

    def __init__(self):
        super().__init__()
        self.client = self._get_client()
        self.repo = Repository()

    def _get_client(self):
        creds = SpotifyClientCredentials(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET
        )
        return Spotify(auth_manager=creds)

    def crawl_albums_by_artist_id(self, artist_id: str, max_items: int = DEFAULT_MAX_ITEMS,
                                  offset: int = 0) -> Union[None, List[Album]]:
        if not artist_id:
            raise ValueError('artist_id is required')

        func_name = 'crawl_albums_by_artist_id'
        main_arg_name = 'artist_id'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=artist_id,
                                 tracked_at=date.today()):
            self.logger.info(
                f'Already crawled albums for artist with id: {artist_id}')
            return

        self.logger.info(f'Crawling albums for artist with id: {artist_id}')
        api = self.client.artist_albums
        parser = parse_albums_response
        pages = self._get_results_page(
            api=api, parser=parser, artist_id=artist_id, max_items=max_items, limit=self.DEFAULT_SEARCH_LIMIT,
            offset=offset
        )

        crawled_albums = []
        for albums in pages:
            crawled_albums.extend([
                self.store_album(album) for album in albums])
        self.logger.info(f'Found {len(crawled_albums)} albums')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=artist_id)
        return crawled_albums

    def crawl_artists(self, artist_ids: List[str]) -> Union[None, List[Artist]]:
        if not artist_ids:
            raise ValueError('artist_ids is required')

        func_name = 'crawl_artists'
        main_arg_name = 'artist_ids'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=artist_ids,
                                 tracked_at=date.today()):
            self.logger.info('Already crawled artists with '
                             f'{len(artist_ids)} ids')
            return

        self.logger.info(f'Crawling artists with {len(artist_ids)} ids')
        crawled_artists = []
        for batch_ids in self.chunks(artist_ids):
            response = self.client.artists(list(batch_ids))
            artists = [parse_artist(artist)
                       for artist in response['artists'] or []]
            crawled_artists.extend(self._store_entities(artists))
        self.logger.info(f'Found {len(crawled_artists)} artists')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=artist_ids)
        return crawled_artists

    def crawl_playlist(self, playlist_id: str) -> Union[None, Playlist]:
        if not playlist_id:
            raise ValueError('playlist_id is required')

        func_name = 'crawl_playlist'
        main_arg_name = 'playlist_id'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=playlist_id,
                                 tracked_at=date.today()):
            self.logger.info(
                f'Already crawled playlist with id: {playlist_id}')
            return

        self.logger.info(f'Crawling playlist with id: {playlist_id}')
        response = self.client.playlist(playlist_id=playlist_id)
        playlist = parse_playlist_detail(response)
        self.store_playlist(playlist)
        self.logger.info(f'Found playlist: {playlist}')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=playlist_id)
        return playlist

    def crawl_playlist_tracks(self, playlist_id: str, max_items: int = DEFAULT_MAX_ITEMS,
                              offset: int = 0) -> Union[None, List[Track]]:
        if not playlist_id:
            raise ValueError('playlist_id is required')

        func_name = 'crawl_playlist_tracks'
        main_arg_name = 'playlist_id'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=playlist_id,
                                 tracked_at=date.today()):
            self.logger.info(
                f'Already crawled playlist tracks for playlist with id: {playlist_id}')
            return

        self.logger.info(
            f'Crawling playlist tracks for playlist with id: {playlist_id}')
        api = self.client.playlist_tracks
        parser = parse_playlist_tracks_response
        pages = self._get_results_page(
            api=api, parser=parser, playlist_id=playlist_id, max_items=max_items, offset=offset, limit=self.DEFAULT_SEARCH_LIMIT
        )

        crawled_tracks = []
        for tracks in pages:
            crawled_tracks.extend(self.store_tracks(tracks))
        self.logger.info(f'Found {len(crawled_tracks)} tracks')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=playlist_id)
        return crawled_tracks

    def crawl_playlists_by_user_id(self, user_id: str, max_items: int = DEFAULT_MAX_ITEMS,
                                   offset: int = 0) -> Union[None, List[Playlist]]:
        if not user_id:
            raise ValueError('user_id is required')

        func_name = 'crawl_playlists_by_user_id'
        main_arg_name = 'user_id'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=user_id,
                                 tracked_at=date.today()):
            self.logger.info(
                f'Already crawled playlists for user with id: {user_id}')
            return

        self.logger.info(f'Crawling playlists for user with id: {user_id}')
        api = self.client.user_playlists
        parser = parse_user_playlists_response
        pages = self._get_results_page(
            api=api, parser=parser, user=user_id, max_items=max_items, offset=offset, limit=self.DEFAULT_SEARCH_LIMIT)

        crawled_playlists = []
        crawled_users = []
        for playlists in pages:
            for playlist in playlists:
                if playlist.is_exists_column('owner'):
                    crawled_users.append(self.repo.upsert(playlist.owner))
            crawled_playlists.extend(self._store_entities(playlists))
        self.logger.info(f'Found {len(crawled_playlists)} playlists')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=user_id)
        return crawled_playlists

    def crawl_track_plays_count_by_id(self, track_id: str) -> Union[None, Track]:
        if not track_id:
            raise ValueError('track_id is required')

        func_name = 'crawl_track_plays_count_by_id'
        main_arg_name = 'track_id'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=track_id,
                                 tracked_at=date.today()):
            self.logger.info(f'Already crawled track with id: {track_id}')
            return

        self.logger.info(f'Crawling track with id: {track_id}')
        track = self.find_track_by_id(track_id=track_id)
        if not track:
            self.logger.info(f'Not found track with id: {track_id}')
            return

        plays_count = None
        # get via html
        response = requests.get(f'https://open.spotify.com/track/{track_id}')
        soup = BeautifulSoup(response.text, 'html.parser')
        link = soup.find('a', href=f'/track/{track_id}')
        if link:
            plays_count_str = link.find('span', class_='encore-text-marginal')
            if plays_count_str:
                plays_count = int(plays_count_str.text.replace(',', ''))

        if not plays_count:
            try:
                response = requests.get(
                    f'{SPOTIFY_WEB_API}/track/{track_id}/plays_count', timeout=60)
                plays_count = response.json()['plays_count']
            except Timeout:
                self.logger.error('Request to spotify web api timeout.')
            except RequestException as e:
                self.logger.error(e)

        if not plays_count:
            self.logger.warning(f'Can not get plays_count for {track_id}. '
                                f'Please try again later!')
            return track

        track.plays_count = plays_count
        self.repo.upsert(track)
        self.logger.info(f'Found plays_count: {track.plays_count} '
                         f'for track with id: {track_id}')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=track_id)
        return track

    def crawl_tracks(self, track_ids: List[str]) -> Union[None, List[Track]]:
        if not track_ids:
            raise ValueError('track_ids is required')

        func_name = 'crawl_tracks'
        main_arg_name = 'track_ids'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=track_ids,
                                 tracked_at=date.today()):
            self.logger.info(
                f'Already crawled tracks with {len(track_ids)} ids')
            return

        self.logger.info(f'Crawling tracks with {len(track_ids)} ids')
        crawled_tracks = []
        for batch_ids in self.chunks(track_ids):
            response = self.client.tracks(tracks=list(batch_ids))
            tracks = [parse_track_detail(track)
                      for track in response['tracks'] or []]
            crawled_tracks.extend(self.store_tracks(tracks))
        self.logger.info(f'Found {len(crawled_tracks)} tracks')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=track_ids)
        return crawled_tracks

    def crawl_tracks_by_album_id(self, album_id: str, max_items: int = DEFAULT_MAX_ITEMS,
                                 offset: int = 0) -> Union[None, List[Track]]:
        if not album_id:
            raise ValueError('album_id is required')

        func_name = 'crawl_tracks_by_album_id'
        main_arg_name = 'album_id'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=album_id,
                                 tracked_at=date.today()):
            self.logger.info(
                f'Already crawled tracks for album with id: {album_id}')
            return

        self.logger.info(f'Crawling tracks for album with id: {album_id}')
        api = self.client.album_tracks
        parser = parse_tracks_from_album
        pages = self._get_results_page(
            api=api, parser=parser, album_id=album_id, max_items=max_items, offset=offset, limit=self.DEFAULT_SEARCH_LIMIT
        )

        crawled_tracks = []
        for tracks in pages:
            crawled_tracks.extend([
                self.store_simplified_track(track=track, album_id=album_id)
                for track in tracks
            ])
        self.logger.info(f'Found {len(crawled_tracks)} tracks')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=album_id)
        return crawled_tracks

    def search_artists(self, query: str, max_items: int = DEFAULT_MAX_ITEMS,
                       offset: int = 0) -> Union[None, List[Artist]]:
        if not query:
            raise ValueError('query is required')

        func_name = 'search_artists'
        main_arg_name = 'query'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=query,
                                 tracked_at=date.today()):
            self.logger.info(f'Already crawled artists with query: {query}')
            return

        self.logger.info(f'Searching artists with query: {query} '
                         f'with (max_items: {max_items}, offset: {offset})')
        api = self.client.search
        parser = parse_search_artists_response
        pages = self._get_results_page(
            api=api, parser=parser, max_items=max_items, q=query,
            type=T_ARTIST, limit=self.DEFAULT_SEARCH_LIMIT, offset=offset
        )

        crawled_artists = []
        for artists in pages:
            crawled_artists.extend(self._store_entities(artists))
        self.logger.info(f'Found {len(crawled_artists)} artists')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=query)
        return crawled_artists

    def search_playlists(self, query: str, max_items: int = DEFAULT_MAX_ITEMS,
                         offset: int = 0) -> Union[None, Tuple[List[Playlist], List[User]]]:
        if not query:
            raise ValueError('query is required')

        func_name = 'search_playlists'
        main_arg_name = 'query'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=query,
                                 tracked_at=date.today()):
            self.logger.info(f'Already crawled playlists with query: {query}')
            return

        self.logger.info(f'Searching playlists with query: {query} '
                         f'with (max_items: {max_items}, offset: {offset})')
        api = self.client.search
        parser = parse_search_playlists_response
        pages = self._get_results_page(
            api=api, parser=parser, max_items=max_items, q=query,
            type=T_PLAYLIST, limit=self.DEFAULT_SEARCH_LIMIT, offset=offset
        )
        crawled_playlists = []
        crawled_users = []
        for playlists in pages:
            users = [playlist.owner for playlist in playlists]
            crawled_playlists.extend(self._store_entities(playlists))
            crawled_users.extend(self._store_entities(users))
        self.logger.info(f'Found {len(crawled_playlists)} playlists and '
                         f'{len(crawled_users)} users')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=query)
        return crawled_playlists, crawled_users

    def _delay_request(self):
        random_delay = random.uniform(self.MIN_DELAY, self.MAX_DELAY)
        self.logger.info(f'Delaying for {random_delay} seconds')
        time.sleep(random_delay)

    def _get_results_page(self, api, parser, max_items, *args, **kwargs):
        results_count = 0
        page = 1
        while results_count < max_items:
            reponse = api(*args, **kwargs)
            results = parser(reponse)
            if not results:
                break

            results_count += len(results)
            self.logger.info(
                f'Page {page}: +{len(results)} items, total: {results_count}')
            yield results
            page += 1

            kwargs['offset'] += kwargs['limit']
            self._delay_request()

    def _store_entities(self, entities: list) -> list:
        return [self.repo.upsert(entity) for entity in entities or []]

    def store_album(self, album: Album) -> Album:
        if album.is_exists_column('artists'):
            self._store_entities(album.artists)
        return self.repo.upsert(album)

    def store_playlist(self, playlist: Playlist) -> Playlist:
        if playlist.is_exists_column('owner'):
            self.repo.upsert(playlist.owner)
        if playlist.is_exists_column('tracks'):
            self.store_tracks(playlist.tracks)
        if playlist.is_exists_column('playlist_tracks'):
            self._store_entities(playlist.playlist_tracks)
        return self.repo.upsert(playlist)

    def store_track(self, track: Track) -> Track:
        if track.is_exists_column('album'):
            self.store_album(track.album)
        if track.is_exists_column('artists'):
            self._store_entities(track.artists)
        if track.is_exists_column('playlist_track'):
            self.repo.upsert(track.playlist_track)
        return self.repo.upsert(track)

    def store_tracks(self, tracks: List[Track]) -> List[Track]:
        return [self.store_track(track) for track in tracks]

    def store_simplified_track(self, track: Track, album_id: str) -> Track:
        if track.is_exists_column('artists'):
            self._store_entities(track.artists)
        track.album_id = album_id
        return self.repo.upsert(track)

    def track_func(self, func_name: str, main_arg_name: str, main_arg_value: Any):
        if isinstance(main_arg_value, list):
            main_arg_value = ','.join(main_arg_value)

        if not isinstance(main_arg_value, str):
            raise ValueError('main_arg_value must be a string')

        crawler_tracking = CrawlerTracking(
            function_name=func_name, main_arg_name=main_arg_name,
            main_arg_value=main_arg_value, tracked_at=date.today()
        )
        self.repo.upsert(crawler_tracking)

    def should_crawl(self, func_name: str, main_arg_name: str, main_arg_value: Any,
                     tracked_at: date) -> bool:
        if isinstance(main_arg_value, list):
            main_arg_value = ','.join(main_arg_value)

        if not isinstance(main_arg_value, str):
            raise ValueError('main_arg_value must be a string')

        return not self.find_crawler_tracking(func_name, main_arg_name,
                                              main_arg_value, tracked_at)

    def find_crawler_tracking(self, func_name: str, main_arg_name: str,
                              main_arg_value: Any, tracked_at: date) -> CrawlerTracking:
        return self.repo.find_one(
            CrawlerTracking,
            CrawlerTracking.function_name == func_name,
            CrawlerTracking.main_arg_name == main_arg_name,
            CrawlerTracking.main_arg_value == main_arg_value,
            CrawlerTracking.tracked_at == tracked_at
        )

    def find_track_by_id(self, track_id: str) -> Track:
        return self.repo.find_one(Track, Track.track_id == track_id)
