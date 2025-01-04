from decouple import config

PG_SPOTIFY_URI = config('PG_SPOTIFY_URI')
SPOTIFY_CLIENT_ID = config('SPOTIFY_CLIENT_ID')
SPOTIFY_CLIENT_SECRET = config('SPOTIFY_CLIENT_SECRET')
SPOTIFY_REDIRECT_URI = config(
    'SPOTIFY_REDIRECT_URI', default='http://localhost:8888/callback')
SPOTIFY_CACHE_USERNAME = config('SPOTIFY_CACHE_USERNAME')
SPOTIFY_WEB_API = config('SPOTIFY_WEB_API', default='http://localhost:8000')

SPOTIFY_URL = 'https://open.spotify.com'
T_ALBUM = 'album'
T_ARTIST = 'artist'
T_PLAYLIST = 'playlist'
T_TRACK = 'track'
T_USER = 'user'
