import os

# Album type
L_AT_ALBUM = 'album'
L_AT_SINGLE = 'single'
L_AT_COMPILATION = 'compilation'
C_AT_ALBUM = 1
C_AT_SINGLE = 2
C_AT_COMPILATION = 3

# Album date precision
L_ADP_YEAR = 'year'
L_ADP_MONTH = 'month'
L_ADP_DAY = 'day'
C_ADP_YEAR = 1
C_ADP_MONTH = 2
C_ADP_DAY = 3

# source tables
T_ARTIST = 'artist'
T_ARTIST_ALBUM = 'artist_album'
T_ARTIST_TRACK = 'artist_track'
T_ALBUM = 'album'
T_PLAYLIST = 'playlist'
T_PLAYLIST_TRACK = 'playlist_track'
T_TRACK = 'track'
T_USER = 'user'

# dim tables
D_ARTIST = 'dim_artist'
D_ALBUM = 'dim_album'
D_GENRE = 'dim_genre'
D_PLAYLIST = 'dim_playlist'
D_USER = 'dim_user'
D_TRACK = 'dim_track'

# fact tables
F_ALBUM_TRACK = 'fact_album_track'
F_PLAYLIST_TRACK = 'fact_playlist_track'

ALL_DWH_TABLES = [
    D_ARTIST, D_ALBUM, D_GENRE, D_PLAYLIST, D_USER, D_TRACK,
    F_ALBUM_TRACK, F_PLAYLIST_TRACK
]

PIPELINE_DIR = os.path.dirname(os.path.realpath(__file__))
CONFIG_DIR = os.path.join(PIPELINE_DIR, 'configs')
QUERY_DIR = os.path.join(PIPELINE_DIR, 'queries')
