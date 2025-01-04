from requests import Session
from requests.exceptions import RequestException

from src.crawler.services.base import BaseService


class SpotifyWebApi(BaseService):
    DEFAULT_USER_AGENT = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:133.0) Gecko/20100101 Firefox/133.0'
    QUERY_URL = 'https://api-partner.spotify.com/pathfinder/v1/query'
    DEFAULT_HEADERS = {
        'User-Agent': DEFAULT_USER_AGENT,
        'Accept': 'application/json',
        'Accept-Language': 'en',
        'content-type': 'application/json;charset=UTF-8',
        'Connection': 'keep-alive',
        'app-platform': 'WebPlayer',
        'spotify-app-version': '896000000',
    }
    ALBUM_HASH_KEY = '8f4cd5650f9d80349dbe68684057476d8bf27a5c51687b2b1686099ab5631589'
    ARTIST_HASH_KEY = '4bc52527bb77a5f8bbb9afe491e9aa725698d29ab73bff58d49169ee29800167'
    TRACK_HASH_KEY = '5c5ec8c973a0ac2d5b38d7064056c45103c5a062ee12b62ce683ab397b5fbe7d'
    PLAYLIST_HASH_KEY = '19ff1327c29e99c208c86d7a9d8f1929cfdf3d3202a0ff4253c821f1901aa94d'

    def __init__(self):
        super().__init__()
        self.session = Session()

    def _make_request(self, method, url, headers=None, params=None):
        response = self.session.request(
            method=method,
            url=url,
            headers=headers,
            params=params
        )
        if response.status_code == 401:
            raise RequestException('Access token expired')
        response.raise_for_status()
        return response

    def get_access_token(self):
        self.logger.info('Getting access token')
        headers = {
            'User-Agent': self.DEFAULT_USER_AGENT,
            'Accept': 'application/json',
            'Accept-Language': 'en',
            'app-platform': 'WebPlayer',
            'spotify-app-version': '1.2.54.219.g19a93a5d',
            'Connection': 'keep-alive',
            'Priority': 'u=4',
        }
        params = {'productType': 'web-player'}
        return self._make_request(
            method='GET',
            url='https://open.spotify.com/get_access_token',
            headers=headers,
            params=params
        )

    def get_album_tracks(self, album_id: str, access_token: str, offset: int = 0, limit: int = 50):
        headers = self.DEFAULT_HEADERS.copy()
        headers.update({
            'authorization': f'Bearer {access_token}'
        })
        params = {
            'operationName': 'getAlbum',
            'variables': f'{{"uri":"spotify:album:{album_id}","locale":"","offset":{offset},"limit":{limit}}}',
            'extensions': f'{{"persistedQuery":{{"version":1,"sha256Hash":"{self.ALBUM_HASH_KEY}"}}}}',
        }
        return self._make_request(
            method='GET',
            url=self.QUERY_URL,
            headers=headers,
            params=params
        )

    def get_artist(self, artist_id: str, access_token: str):
        headers = self.DEFAULT_HEADERS.copy()
        headers.update({
            'authorization': f'Bearer {access_token}'
        })
        params = {
            'operationName': 'queryArtistOverview',
            'variables': f'{{"uri":"spotify:artist:{artist_id}","locale":""}}',
            'extensions': f'{{"persistedQuery":{{"version":1,"sha256Hash":"{self.ARTIST_HASH_KEY}"}}}}',
        }
        return self._make_request(
            method='GET',
            url=self.QUERY_URL,
            headers=headers,
            params=params
        )

    def get_track(self, track_id: str, access_token: str):
        headers = self.DEFAULT_HEADERS.copy()
        headers.update({
            'authorization': f'Bearer {access_token}'
        })
        params = {
            'operationName': 'getTrack',
            'variables': f'{{"uri":"spotify:track:{track_id}"}}',
            'extensions': f'{{"persistedQuery":{{"version":1,"sha256Hash":"{self.TRACK_HASH_KEY}"}}}}',
        }
        return self._make_request(
            method='GET',
            url=self.QUERY_URL,
            headers=headers,
            params=params
        )

    def get_playlist_tracks(self, playlist_id: str, access_token: str, offset: int = 0, limit: int = 50):
        headers = self.DEFAULT_HEADERS.copy()
        headers.update({
            'authorization': f'Bearer {access_token}'
        })
        params = {
            'operationName': 'fetchPlaylist',
            'variables': f'{{"uri":"spotify:playlist:{playlist_id}","offset":{offset},"limit":{limit}}}',
            'extensions': f'{{"persistedQuery":{{"version":1,"sha256Hash":"{self.PLAYLIST_HASH_KEY}"}}}}',
        }
        return self._make_request(
            method='GET',
            url=self.QUERY_URL,
            headers=headers,
            params=params
        )
