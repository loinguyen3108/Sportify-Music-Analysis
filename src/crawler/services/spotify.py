import random
import time
from bs4 import BeautifulSoup
from datetime import date
from functools import cached_property
from typing import Any, Iterator, List, Union

import requests
from requests.exceptions import Timeout, ConnectionError, RequestException
from sqlalchemy import text

from src.configs.crawler import SPOTIFY_CACHE_USERNAME, SPOTIFY_CLIENT_ID,  \
    SPOTIFY_CLIENT_SECRET, SPOTIFY_REDIRECT_URI, SPOTIFY_WEB_API, T_ALBUM, T_ARTIST, \
    T_PLAYLIST, T_TRACK
from src.crawler.exceptions import TemporaryError
from src.crawler.models import Album, Artist, ArtistAlbum, CrawlerTracking, Playlist, Track
from src.infratructure.postgres.repository import Repository
from src.crawler.services.base import BaseService
from src.crawler.services.parsers.offical import parse_album_detail, parse_albums_response, \
    parse_artist, parse_playlist_detail, parse_playlist_tracks_response, parse_search_albums_response, \
    parse_search_artists_response, parse_search_playlists_response, parse_track_detail, \
    parse_tracks_from_album, parse_user_playlists_response, parse_search_tracks_response
from src.crawler.services.parsers.web import parse_album_tracks, parse_albums_from_artist_data, \
    parse_artist_web, parse_playlist_tracks_web, parse_track_web_detail

OFFICIAL_API = 'official_api'
WEB_API = 'web_api'


class SpotifyService(BaseService):
    MIN_DELAY = 1
    MAX_DELAY = 5
    MAX_RETRIES = 1

    DEFAULT_SEARCH_LIMIT = 50
    DEFAULT_MAX_PAGES = 10_000_000

    QUERY_BY_GENRE = """
        WITH vietnamese_artist AS (
            SELECT
                artist_id
            FROM
                artist
            WHERE ARRAY[:genres] && genres
        )
        SELECT
            pt.playlist_id,
            a.artist_id,
            t.album_id,
            t.track_id
        FROM
            vietnamese_artist a
        JOIN 
            artist_track art ON a.artist_id = art.artist_id
        JOIN 
            track t ON art.track_id = t.track_id
        LEFT JOIN 
            playlist_track pt ON t.track_id = pt.track_id;
    """

    def __init__(self):
        super().__init__()
        self.access_token = None
        self.repo = Repository()

    @cached_property
    def client(self):
        from spotipy import Spotify
        from spotipy.cache_handler import CacheFileHandler
        from spotipy.oauth2 import SpotifyOAuth
        oauth = SpotifyOAuth(
            client_id=SPOTIFY_CLIENT_ID,
            client_secret=SPOTIFY_CLIENT_SECRET,
            redirect_uri=SPOTIFY_REDIRECT_URI,
            cache_handler=CacheFileHandler(
                username=SPOTIFY_CACHE_USERNAME),
        )
        return Spotify(auth_manager=oauth)

    @cached_property
    def web_api(self):
        from src.crawler.services.web_api import SpotifyWebApi
        return SpotifyWebApi()

    def authenticate(self):
        self.logger.info('Authenticating')
        self.client.track('7Kk581xmajJZNYit8Ssrtd')
        self.logger.info('Authenticated')

    def crawl_albums(self, album_ids: List[str], refresh: bool = False) -> Union[None, List[Album]]:
        """Crawl albums by album ids, only support 20 ids per request via official api

        Args:
            album_ids (List[str]): A list of album ids
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If album_ids is empty

        Returns:
            Union[None, List[Album]]: A list of albums or None
        """
        if not album_ids:
            raise ValueError('album_ids is required')

        func_name = 'crawl_albums'
        main_arg_name = 'album_id'
        if not refresh:
            album_ids = [
                album_id for album_id in album_ids
                if self.should_crawl(func_name=func_name, main_arg_name=main_arg_name,
                                     main_arg_value=album_id)
            ]
            if not album_ids:
                self.logger.info(f'Already crawled {len(album_ids)} albums')
                return

        self.logger.info(f'Crawling albums with {len(album_ids)} ids')
        crawled_albums = []
        for batch_ids in self.chunks(album_ids, arr_size=20):
            response = self.client.albums(list(batch_ids))
            albums = [parse_album_detail(album)
                      for album in response['albums'] or []
                      if album is not None]
            crawled_albums.extend(self.store_albums(albums))
        self.logger.info(f'Found {len(crawled_albums)} albums')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=album_ids)
        if crawled_albums:
            self._delay_request()
        return crawled_albums

    def crawl_albums_by_artist_id(
        self, artist_id: str, max_pages: int = DEFAULT_MAX_PAGES, offset: int = 0, refresh: bool = False,
        strategy: str = WEB_API
    ) -> Union[None, Iterator[List[Album]]]:
        """Crawl albums by artist id

        Args:
            artist_id (str): Artist id
            max_pages (int, optional): Maximum number of pages. Defaults to DEFAULT_MAX_PAGES.
            offset (int, optional): The offset to start. Defaults to 0.
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If artist_id is empty

        Returns:
            Union[None, Iterator[List[Album]]]: A list of albums or None

        Yields:
            Iterator[Union[None, Iterator[List[Album]]]]: A list of albums
        """

        if not artist_id:
            raise ValueError('artist_id is required')

        func_name = 'crawl_albums_by_artist_id'
        main_arg_name = 'artist_id'
        if not refresh and not self.should_crawl(
                func_name=func_name, main_arg_name=main_arg_name, main_arg_value=artist_id):
            self.logger.info(
                f'Already crawled albums for artist with id: {artist_id}')
            return

        self.logger.info(f'Crawling albums for artist with id: {artist_id}')
        if strategy == OFFICIAL_API:
            api = self.client.artist_albums
            parser = parse_albums_response
        elif strategy == WEB_API:
            api = self.web_api.get_artist_albums
            parser = parse_albums_from_artist_data
        else:
            raise ValueError(f'Unknown strategy: {strategy}')

        pages = self._get_results_page(
            api=api, parser=parser, artist_id=artist_id, max_pages=max_pages, limit=self.DEFAULT_SEARCH_LIMIT,
            offset=offset, strategy=strategy
        )

        crawled_albums = 0
        for albums in pages:
            if strategy == OFFICIAL_API:
                stored_albums = [
                    self.store_album(album) for album in albums
                    if album.release_date is not None]
            else:
                stored_albums = []
                for album in albums:
                    if not album:
                        continue

                    album.artist_album = [ArtistAlbum(
                        album_id=album.album_id, artist_id=artist_id)]
                    stored_albums.append(self.store_album(album))
            crawled_albums += len(stored_albums)
            yield stored_albums
        self.logger.info(f'Found {crawled_albums} albums')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=artist_id)

    def crawl_artist(self, artist_id: str, refresh: bool = False) -> Union[None, Artist]:
        """Crawl artist by artist id via web api

        Args:
            artist_id (str): Artist id
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If artist_id is empty

        Returns:
            Union[None, Artist]: An artist or None
        """
        if not artist_id:
            raise ValueError('artist_id is required')

        func_name = 'crawl_artist'
        main_arg_name = 'artist_id'
        if not refresh and not self.should_crawl(
                func_name=func_name, main_arg_name=main_arg_name, main_arg_value=artist_id):
            self.logger.info(f'Already crawled artist with id: {artist_id}')
            return

        self.logger.info(f'Crawling artist with id: {artist_id}')
        api = self.web_api.get_artist
        artist = self._get_result(
            api=api, parser=parse_artist_web, artist_id=artist_id
        )
        if not artist:
            self.logger.info(f'Not found artist with id: {artist_id}')
            return

        self.store_artist(artist)
        self.logger.info(f'Found artist {artist}')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=artist_id)

    def crawl_artists(self, artist_ids: List[str], refresh: bool = False) -> Union[None, List[Artist]]:
        """Crawl artists by artist ids via official api

        Args:
            artist_ids (List[str]): A list of artist ids
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If artist_ids is empty

        Returns:
            Union[None, List[Artist]]: A list of artists or None
        """
        if not artist_ids:
            raise ValueError('artist_ids is required')

        func_name = 'crawl_artists'
        main_arg_name = 'artist_id'
        if not refresh:
            artist_ids = [
                artist_id for artist_id in artist_ids
                if self.should_crawl(func_name=func_name, main_arg_name=main_arg_name,
                                     main_arg_value=artist_id)
            ]
            if not artist_ids:
                self.logger.info(
                    f'Already crawled artists with {len(artist_ids)} ids')
                return

        self.logger.info(f'Crawling artists with {len(artist_ids)} ids')
        crawled_artists = []
        for batch_ids in self.chunks(artist_ids):
            response = self.client.artists(list(batch_ids))
            artists = [parse_artist(artist)
                       for artist in response['artists'] or []
                       if artist is not None]
            crawled_artists.extend(self._store_entities(artists))
        self.logger.info(f'Found {len(crawled_artists)} artists')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=artist_ids)
        if crawled_artists:
            self._delay_request()
        return crawled_artists

    def crawl_playlist(self, playlist_id: str, refresh: bool = False) -> Union[None, Playlist]:
        """Crawl playlist by playlist id via web api

        Args:
            playlist_id (str): Playlist id
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If playlist_id is empty

        Returns:
            Union[None, Playlist]: A playlist or None
        """
        if not playlist_id:
            raise ValueError('playlist_id is required')

        func_name = 'crawl_playlist'
        main_arg_name = 'playlist_id'
        if not refresh and not self.should_crawl(
                func_name=func_name, main_arg_name=main_arg_name, main_arg_value=playlist_id):
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
        if playlist:
            self._delay_request()
        return playlist

    def crawl_playlist_tracks(
            self, playlist_id: str, max_pages: int = DEFAULT_MAX_PAGES, offset: int = 0, strategy: str = WEB_API,
            refresh: bool = False
    ) -> Union[None, Iterator[List[Track]]]:
        """Crawl playlist tracks by playlist id.

        Args:
            playlist_id (str): Playlist id
            max_pages (int, optional): Maximum number of pages. Defaults to DEFAULT_MAX_PAGES.
            offset (int, optional): The offset to start. Defaults to 0.
            strategy (str, optional): The strategy to use for fetching playlist tracks. Defaults to WEB_API.
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If playlist_id is empty

        Returns:
            Union[None, Iterator[List[Track]]]: A list of tracks or None

        Yields:
            Iterator[Union[None, Iterator[List[Track]]]]: A list of tracks or None
        """
        if not playlist_id:
            raise ValueError('playlist_id is required')

        func_name = 'crawl_playlist_tracks'
        main_arg_name = 'playlist_id'
        if not refresh and not self.should_crawl(
                func_name=func_name, main_arg_name=main_arg_name, main_arg_value=playlist_id):
            self.logger.info(
                f'Already crawled playlist tracks for playlist with id: {playlist_id}')
            return

        self.logger.info(
            f'Crawling playlist tracks for playlist with id: {playlist_id}')
        if strategy == OFFICIAL_API:
            api = self.client.playlist_tracks
            parser = parse_playlist_tracks_response
            limit = self.DEFAULT_SEARCH_LIMIT
        elif strategy == WEB_API:
            api = self.web_api.get_playlist_tracks
            parser = parse_playlist_tracks_web
            limit = 25
        else:
            raise ValueError(f'Unknown strategy: {strategy}')
        pages = self._get_results_page(
            api=api, parser=parser, playlist_id=playlist_id, max_pages=max_pages, offset=offset,
            limit=limit, strategy=strategy
        )

        crawled_tracks = 0
        for page in pages:
            if strategy == OFFICIAL_API:
                stored_tracks = self.store_tracks(page)
            elif strategy == WEB_API:
                stored_tracks = self.store_tracks(page.tracks)
                self.store_playlist(page)
            crawled_tracks += len(stored_tracks)
            yield stored_tracks
        self.logger.info(
            f'Crawled {crawled_tracks} tracks for playlist with id: {playlist_id}')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=playlist_id)

    def crawl_playlists_by_user_id(self, user_id: str, max_pages: int = DEFAULT_MAX_PAGES,
                                   offset: int = 0, refresh: bool = False) -> Union[None, List[Playlist]]:
        """Crawl playlists by user id via official api

        Args:
            user_id (str): User id
            max_pages (int, optional): Maximum number of pages. Defaults to DEFAULT_MAX_PAGES.
            offset (int, optional): The offset to start. Defaults to 0.
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If user_id is empty

        Returns:
            Union[None, List[Playlist]]: A list of playlists or None
        """
        if not user_id:
            raise ValueError('user_id is required')

        func_name = 'crawl_playlists_by_user_id'
        main_arg_name = 'user_id'
        if not refresh and not self.should_crawl(
                func_name=func_name, main_arg_name=main_arg_name, main_arg_value=user_id):
            self.logger.info(
                f'Already crawled playlists for user with id: {user_id}')
            return

        self.logger.info(f'Crawling playlists for user with id: {user_id}')
        api = self.client.user_playlists
        parser = parse_user_playlists_response
        pages = self._get_results_page(
            api=api, parser=parser, user=user_id, max_pages=max_pages, offset=offset, limit=self.DEFAULT_SEARCH_LIMIT)

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

    def crawl_track(self, track_id: str, refresh: bool = False) -> Union[None, Track]:
        """Crawl track by track id via web api

        Args:
            track_id (str): Track id
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If track_id is empty

        Returns:
            Union[None, Track]: A track or None
        """
        if not track_id:
            raise ValueError('track_id is required')

        func_name = 'crawl_track'
        main_arg_name = 'track_id'
        if not refresh and not self.should_crawl(
                func_name=func_name, main_arg_name=main_arg_name, main_arg_value=track_id):
            self.logger.info(f'Already crawled track with id: {track_id}')
            return

        self.logger.info(f'Crawling track with id: {track_id}')
        api = self.web_api.get_track
        track = self._get_result(
            api=api, parser=parse_track_web_detail,
            track_id=track_id
        )
        if not track:
            self.logger.info(f'Not found track with id: {track_id}')
            return

        self.store_track(track)
        self.logger.info(f'Found track {track}')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=track_id)
        return track

    def crawl_tracks(self, track_ids: List[str], refresh: bool = False) -> Union[None, List[Track]]:
        """Crawl tracks by track ids via official api

        Args:
            track_ids (List[str]): A list of track ids
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If track_ids is empty

        Returns:
            Union[None, List[Track]]: A list of tracks or None
        """
        if not track_ids:
            raise ValueError('track_ids is required')

        func_name = 'crawl_tracks'
        main_arg_name = 'track_id'
        if not refresh:
            track_ids = [
                track_id for track_id in track_ids
                if self.should_crawl(func_name=func_name, main_arg_name=main_arg_name,
                                     main_arg_value=track_id)
            ]
            if not track_ids:
                self.logger.info(f'Already crawled {len(track_ids)} tracks')
                return

        self.logger.info(f'Crawling tracks with {len(track_ids)} ids')
        crawled_tracks = []
        for batch_ids in self.chunks(track_ids):
            response = self.client.tracks(tracks=list(batch_ids))
            tracks = [
                parse_track_detail(track) for track in response['tracks'] or []
                if track is not None
            ]
            crawled_tracks.extend(self.store_tracks(tracks))
        self.logger.info(f'Found {len(crawled_tracks)} tracks')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=track_ids)
        if crawled_tracks:
            self._delay_request()
        return crawled_tracks

    def crawl_tracks_by_album_id(
            self, album_id: str, max_pages: int = DEFAULT_MAX_PAGES, offset: int = 0, strategy: str = WEB_API,
            refresh: bool = False
    ) -> Union[None, Iterator[List[Track]]]:
        """Crawl tracks by album id

        Args:
            album_id (str): Album id
            max_pages (int, optional): Maximum number of pages. Defaults to DEFAULT_MAX_PAGES.
            offset (int, optional): The offset to start. Defaults to 0.
            strategy (str, optional): The strategy to use for fetching playlist tracks. Defaults to WEB_API.
            refresh (bool, optional): Whether to recrawl. Defaults to False.

        Raises:
            ValueError: If album_id is empty

        Returns:
            Union[None, Iterator[List[Track]]]: A list of tracks or None

        Yields:
            Iterator[Union[None, Iterator[List[Track]]]]: A list of tracks
        """
        if not album_id:
            raise ValueError('album_id is required')

        func_name = 'crawl_tracks_by_album_id'
        main_arg_name = 'album_id'
        if not refresh and not self.should_crawl(
                func_name=func_name, main_arg_name=main_arg_name, main_arg_value=album_id):
            self.logger.info(
                f'Already crawled tracks for album with id: {album_id}')
            return

        self.logger.info(f'Crawling tracks for album with id: {album_id}')
        if strategy == OFFICIAL_API:
            api = self.client.album_tracks
            parser = parse_tracks_from_album
        elif strategy == WEB_API:
            api = self.web_api.get_album_tracks
            parser = parse_album_tracks
        else:
            raise ValueError(f'Unknown strategy: {strategy}')

        pages = self._get_results_page(
            api=api, parser=parser, album_id=album_id, max_pages=max_pages, strategy=strategy,
            offset=offset, limit=self.DEFAULT_SEARCH_LIMIT
        )

        crawled_tracks = 0
        for page in pages:
            if strategy == OFFICIAL_API:
                stored_tracks = [
                    self.store_simplified_track(track=track, album_id=album_id)
                    for track in page
                ]
            elif strategy == WEB_API:
                stored_tracks = self.store_tracks(page.tracks)
                self.store_album(page)
            yield stored_tracks
            crawled_tracks += len(stored_tracks)
        self.logger.info(
            f'Crawled {crawled_tracks} tracks for album with id: {album_id}')
        self.track_func(func_name=func_name, main_arg_name=main_arg_name,
                        main_arg_value=album_id)

    def crawl_track_plays_count_by_id(self, track_id: str) -> Union[None, Track]:
        """Crawl track plays_count via self host web api

        Args:
            track_id (str): Track id

        Raises:
            ValueError: If track_id is empty

        Returns:
            Union[None, Track]: A track or None
        """
        if not track_id:
            raise ValueError('track_id is required')

        func_name = 'crawl_track_plays_count_by_id'
        main_arg_name = 'track_id'
        if not self.should_crawl(func_name=func_name, main_arg_name=main_arg_name, main_arg_value=track_id):
            self.logger.info(f'Already crawled track with id: {track_id}')
            return

        self.logger.info(f'Crawling track plays_count with id: {track_id}')
        track = self.find_track_by_id(track_id=track_id)
        if not track:
            self.logger.info(f'Not found track with id: {track_id}')
            return

        plays_count = None
        # get via html
        self.logger.info(f'Get plays_count via html')
        try:
            response = requests.get(
                f'https://open.spotify.com/track/{track_id}')
            soup = BeautifulSoup(response.text, 'html.parser')
            link = soup.find('a', href=f'/track/{track_id}')
            if link:
                plays_count_str = link.find(
                    'span', class_='encore-text-marginal')
                if plays_count_str:
                    plays_count = int(plays_count_str.text.replace(',', ''))
        except ConnectionError as e:
            self.logger.error(e)

        if not plays_count:
            self.logger.info(f'Get plays_count via web api')
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

    def get_access_token(self, refresh: bool = False):
        if refresh or not self.access_token:
            self.logger.info('Getting access token')
            response = self.web_api.get_access_token()
            response.raise_for_status()
            data = response.json()
            if not data:
                raise Exception('Can not get access token')

            self.access_token = data['accessToken']
            self.logger.info(f'Got new access token')
            return self.access_token

        self.logger.info('Using cached access token')
        return self.access_token

    def monitor_crawler(self, object_name: str, object_value: str):
        if not object_name or not object_value:
            raise ValueError('object_name and object_value are required')

        self.logger.info(f'Crawling {object_name} with id: {object_value}')
        if object_name == 'artist':
            self.crawl_artist(artist_id=object_value, refresh=True)
        elif object_name == 'playlist':
            self.crawl_playlist_tracks(
                playlist_id=object_value, max_pages=1, refresh=True)
        elif object_name == 'track':
            self.crawl_track(track_id=object_value, refresh=True)
        else:
            raise ValueError(f'Invalid object_name: {object_name}')

        self.logger.info(
            f'Finished crawling {object_name} with id: {object_value}')

    def search_albums(self, query: str, max_pages: int = DEFAULT_MAX_PAGES,
                      offset: int = 0) -> Union[None, Iterator[List[Album]]]:
        """Search albums via official api

        Args:
            query (str): Album query
            max_pages (int, optional): Maximum number of pages. Defaults to DEFAULT_MAX_PAGES.
            offset (int, optional): The offset to start. Defaults to 0.

        Raises:
            ValueError: If query is empty

        Returns:
            Union[None, Iterator[List[Album]]]: A list of albums

        Yields:
            Iterator[Union[None, Iterator[List[Album]]]]: A list of albums
        """
        if not query:
            raise ValueError('query is required')

        api = self.client.search
        parser = parse_search_albums_response
        pages = self._get_results_page(
            api=api, parser=parser, max_pages=max_pages, q=query,
            type=T_ALBUM, limit=self.DEFAULT_SEARCH_LIMIT, offset=offset,
            strategy=OFFICIAL_API
        )

        crawled_albums = 0
        for albums in pages:
            stored_albums = self.store_albums(albums)
            crawled_albums += len(stored_albums)
            yield stored_albums

        self.logger.info(f'Found {crawled_albums} albums')

    def search_artists(self, query: str, max_pages: int = DEFAULT_MAX_PAGES,
                       offset: int = 0) -> Union[None, Iterator[List[Artist]]]:
        """Search artists via official api

        Args:
            query (str): Artist query
            max_pages (int, optional): Maximum number of pages. Defaults to DEFAULT_MAX_PAGES.
            offset (int, optional): The offset to start. Defaults to 0.

        Raises:
            ValueError: If query is empty

        Returns:
            Union[None, Iterator[List[Artist]]]: A list of artists

        Yields:
            Iterator[Union[None, Iterator[List[Artist]]]]: A list of artists
        """
        if not query:
            raise ValueError('query is required')

        self.logger.info(f'Searching artists with query: {query} '
                         f'with (max_pages: {max_pages}, offset: {offset})')
        api = self.client.search
        parser = parse_search_artists_response
        pages = self._get_results_page(
            api=api, parser=parser, max_pages=max_pages, q=query,
            type=T_ARTIST, limit=self.DEFAULT_SEARCH_LIMIT, offset=offset,
            strategy=OFFICIAL_API
        )

        crawled_artists = 0
        for artists in pages:
            stored_artists = self._store_entities(artists)
            crawled_artists += len(stored_artists)
            yield stored_artists

        self.logger.info(f'Found {crawled_artists} artists')

    def search_playlists(self, query: str, max_pages: int = DEFAULT_MAX_PAGES,
                         offset: int = 0) -> Union[None, Iterator[List[Playlist]]]:
        """Search playlists via official api

        Args:
            query (str): Playlist query
            max_pages (int, optional): Maximum number of pages. Defaults to DEFAULT_MAX_PAGES.
            offset (int, optional): The offset to start. Defaults to 0.

        Raises:
            ValueError: If query is empty

        Returns:
            Union[None, Iterator[List[Playlist]]]: A list of playlists

        Yields:
            Iterator[Union[None, Iterator[List[Playlist]]]]: A list of playlists
        """
        if not query:
            raise ValueError('query is required')

        self.logger.info(f'Searching playlists with query: {query} '
                         f'with (max_pages: {max_pages}, offset: {offset})')
        api = self.client.search
        parser = parse_search_playlists_response
        pages = self._get_results_page(
            api=api, parser=parser, max_pages=max_pages, q=query, type=T_PLAYLIST,
            limit=self.DEFAULT_SEARCH_LIMIT, offset=offset, strategy=OFFICIAL_API
        )
        crawled_playlists = 0
        for playlists in pages:
            stored_playlists = self._store_entities(playlists)
            self._store_entities([playlist.owner for playlist in playlists])
            crawled_playlists += len(stored_playlists)
            yield stored_playlists
        self.logger.info(f'Found {crawled_playlists} playlists')

    def search_tracks(self, query: str, max_pages: int = DEFAULT_MAX_PAGES,
                      offset: int = 0) -> Union[None, Iterator[List[Playlist]]]:
        """Search tracks via official api

        Args:
            query (str): Track query
            max_pages (int, optional): Maximum number of pages. Defaults to DEFAULT_MAX_PAGES.
            offset (int, optional): The offset to start. Defaults to 0.

        Raises:
            ValueError: If query is empty

        Returns:
            Union[None, Iterator[List[Playlist]]]: A list of tracks or None

        Yields:
            Iterator[Union[None, Iterator[List[Playlist]]]]: A list of tracks or None
        """
        if not query:
            raise ValueError('query is required')

        self.logger.info(f'Searching tracks with query: {query} '
                         f'with (max_pages: {max_pages}, offset: {offset})')
        api = self.client.search
        parser = parse_search_tracks_response
        pages = self._get_results_page(
            api=api, parser=parser, max_pages=max_pages, q=query, type=T_TRACK,
            limit=self.DEFAULT_SEARCH_LIMIT, offset=offset, strategy=OFFICIAL_API
        )
        crawled_tracks = 0
        for tracks in pages:
            stored_tracks = self.store_tracks(tracks)
            crawled_tracks += len(stored_tracks)
            yield stored_tracks
        self.logger.info(f'Found {crawled_tracks} tracks')

    def get_monitor_messages(self, genres: List[str]):
        results = self.repo.session.execute(
            text(self.QUERY_BY_GENRE), {'genres': genres}).mappings().all()
        if not results:
            self.logger.info('No results found')
            return

        messages = {
            T_ALBUM: set([result.album_id for result in results if result.album_id]),
            T_ARTIST: set([result.artist_id for result in results if result.artist_id]),
            T_PLAYLIST: set([result.playlist_id for result in results if result.playlist_id]),
            T_TRACK: set([
                result.track_id for result in results if result.track_id])
        }
        return messages

    def _delay_request(self):
        random_delay = random.uniform(self.MIN_DELAY, self.MAX_DELAY)
        self.logger.info(f'Delaying for {random_delay} seconds')
        time.sleep(random_delay)

    def _get_result(self, api, parser, *args, **kwargs):
        retries_count = 0
        kwargs['access_token'] = self.get_access_token()
        while True:
            try:
                self.logger.info(
                    f'Requesting to api: {api.__name__} with args: {args} and kwargs: {kwargs}')
                response = api(*args, **kwargs)
                result = parser(response)
            except (RequestException, TemporaryError) as e:
                self.logger.error(e)
                if retries_count < self.MAX_RETRIES:
                    self.logger.info(
                        f'Retrying getting access token (retries_count: {retries_count})')
                    kwargs['access_token'] = self.get_access_token(
                        refresh=True)
                    retries_count += 1
                    continue
                raise e
            return result

    def _get_results_page(self, api, parser, max_pages, strategy, *args, **kwargs):
        pages_count = 0
        retries_count = 0
        if strategy == WEB_API:
            kwargs['access_token'] = self.get_access_token()
        while pages_count < max_pages:
            try:
                self.logger.info(
                    f'Requesting to api: {api.__name__} with args: {args} and kwargs: {kwargs}')
                reponse = api(*args, **kwargs)
                result = parser(reponse)
            except (RequestException, TemporaryError) as e:
                self.logger.error(e)
                if retries_count < self.MAX_RETRIES and strategy == WEB_API:
                    self.logger.info(
                        f'Retrying getting access token (retries_count: {retries_count})')
                    kwargs['access_token'] = self.get_access_token(
                        refresh=True)
                    retries_count += 1
                    continue
                raise e

            if not result:
                break

            pages_count += 1
            self.logger.info(f'Crawled at page {pages_count}')
            yield result

            kwargs['offset'] += kwargs['limit']
            if strategy == OFFICIAL_API:
                self._delay_request()
            retries_count = 0

    def _store_entities(self, entities: list) -> list:
        return [self.repo.upsert(entity) for entity in entities or []]

    def store_album(self, album: Album) -> Album:
        if album.is_exists_column('artists'):
            self._store_entities(album.artists)
        if album.is_exists_column('artist_albums'):
            self._store_entities(album.artist_albums)
        if album.is_exists_column('more_albums'):
            self._store_entities(album.more_albums)
        return self.repo.upsert(album)

    def store_albums(self, albums: List[Album]) -> List[Album]:
        return [self.store_album(album) for album in albums]

    def store_artist(self, artist: Artist) -> Artist:
        if artist.is_exists_column('albums'):
            self.store_albums(artist.albums)
        return self.repo.upsert(artist)

    def store_playlist(self, playlist: Playlist) -> Playlist:
        if playlist.is_exists_column('owner'):
            self.repo.upsert(playlist.owner)
        if playlist.is_exists_column('users'):
            self._store_entities(playlist.users)
        if playlist.is_exists_column('tracks'):
            self.store_tracks(playlist.tracks)
        if playlist.is_exists_column('playlist_tracks'):
            self._store_entities(playlist.playlist_tracks)
        return self.repo.upsert(playlist)

    def store_track(self, track: Track) -> Track:
        if track.is_exists_column('album'):
            if track.album.release_date:
                self.store_album(track.album)
        if track.is_exists_column('artists'):
            self._store_entities(track.artists)
        if track.is_exists_column('artist_track'):
            self._store_entities(track.artist_track)
        if track.is_exists_column('playlist_track'):
            self.repo.upsert(track.playlist_track)
        if track.is_exists_column('more_albums'):
            self.store_albums(track.more_albums)
        return self.repo.upsert(track)

    def store_tracks(self, tracks: List[Track]) -> List[Track]:
        return [self.store_track(track) for track in tracks]

    def store_simplified_track(self, track: Track, album_id: str) -> Track:
        if track.is_exists_column('artists'):
            self._store_entities(track.artists)
        track.album_id = album_id
        return self.repo.upsert(track)

    def track_func(self, func_name: str, main_arg_name: str, main_arg_value: Any):
        if not isinstance(main_arg_value, (str, list)):
            raise ValueError('main_arg_value must be a string or a list')
        values = [main_arg_value] if isinstance(
            main_arg_value, str) else main_arg_value
        for value in values:
            crawler_tracking = CrawlerTracking(
                function_name=func_name, main_arg_name=main_arg_name,
                main_arg_value=value, tracked_at=date.today()
            )
            self.repo.upsert(crawler_tracking)

    def should_crawl(self, func_name: str, main_arg_name: str, main_arg_value: str) -> bool:
        if not isinstance(main_arg_value, str):
            raise ValueError('main_arg_value must be a string')

        return not self.find_crawler_tracking(func_name, main_arg_name,
                                              main_arg_value)

    def find_crawler_tracking(self, func_name: str, main_arg_name: str,
                              main_arg_value: str) -> CrawlerTracking:
        return self.repo.find_one(
            CrawlerTracking,
            CrawlerTracking.function_name == func_name,
            CrawlerTracking.main_arg_name == main_arg_name,
            CrawlerTracking.main_arg_value == main_arg_value
        )

    def find_crawler_tracking_today(self, func_name: str, main_arg_name: str,
                                    main_arg_value: str) -> CrawlerTracking:
        return self.repo.find_one(
            CrawlerTracking,
            CrawlerTracking.function_name == func_name,
            CrawlerTracking.main_arg_name == main_arg_name,
            CrawlerTracking.main_arg_value == main_arg_value,
            CrawlerTracking.tracked_at == date.today()
        )

    def find_track_by_id(self, track_id: str) -> Track:
        return self.repo.find_one(Track, Track.track_id == track_id)
