from datetime import datetime
from typing import List, Union

from src.configs.crawler import SPOTIFY_URL
from src.crawler.exceptions import TemporaryError
from src.crawler.models import Album, Artist, ArtistAlbum, ArtistTrack, \
    Playlist, PlaylistTrack, Track, User


def parse_album(album_data: dict) -> Album:
    album_id = album_data['id'] if album_data.get('id') \
        else album_data['uri'].replace('spotify:album:', '')
    cover_image = album_data['coverArt']['sources'][0]['url'] if album_data['coverArt']['sources'] else None
    release_date = None
    release_date_precision = None
    if album_data.get('date'):
        release_date_precision = album_data['date']['precision'].lower() \
            if album_data['date'].get('precision') else None
        if album_data['date'].get('isoString'):
            date_ = datetime.strptime(
                album_data['date']['isoString'], '%Y-%m-%dT%H:%M:%SZ').date()
            release_date = date_.strftime('%Y-%m-%d')
        elif album_data['date'].get('year'):
            release_date = str(album_data['date']['year'])
            release_date_precision = 'year'
    url = f'https://open.spotify.com/album/{album_id}'
    return Album(
        album_id=album_id, album_type=album_data['type'].lower(), url=url,
        cover_image=cover_image, name=album_data['name'], release_date=release_date,
        release_date_precision=release_date_precision,
        label=album_data.get('label')
    )


def parse_album_tracks(response) -> Union[None, Album]:
    data = response.json()
    if not data:
        return

    if not data.get('data'):
        raise TemporaryError('Can not get album tracks')

    album_data = response.json()['data']['albumUnion']
    if album_data['__typename'] == 'NotFound':
        return

    # Parse simplified artist
    artists = [parse_simplified_artist(artist_data)
               for artist_data in album_data['artists']['items'] or [] if artist_data]

    # Parse album
    album = parse_album(album_data)
    album.artist_albums = [ArtistAlbum(album_id=album.album_id, artist_id=artist.artist_id)
                           for artist in artists]
    album.artists = artists

    # Parse tracks
    tracks = [parse_track_from_album(track_data, album.album_id)
              for track_data in album_data['tracksV2']['items'] or [] if track_data]
    if not tracks:
        return

    album.tracks = tracks

    # Parse more albums
    if album_data.get('moreAlbumsByArtist'):
        popular_albums = album_data['moreAlbumsByArtist']['items'][0]['discography']['popularReleasesAlbums']
        more_albums = [parse_album(popular_album)
                       for popular_album in popular_albums['items'] or []
                       if popular_album]
        album.more_albums = more_albums

    return album


def parse_artist_web(response) -> Artist:
    artist_data = response.json()
    if not artist_data:
        return

    artist_data = artist_data['data']['artistUnion']
    if artist_data['__typename'] == 'NotFound':
        return

    url = f'{SPOTIFY_URL}/artist/{artist_data["id"]}'
    image = artist_data['visuals']['avatarImage']['sources'][0]['url'] \
        if artist_data['visuals']['avatarImage'] else None
    followers_count = artist_data['stats']['followers']
    monthly_listeners = artist_data['stats']['monthlyListeners']
    artist = Artist(
        artist_id=artist_data['id'], url=url, followers_count=followers_count,
        image=image, name=artist_data['profile']['name'],
        popularity=artist_data.get('popularity'), monthly_listeners=monthly_listeners
    )
    if artist_data.get('relatedContent'):
        albums, playlist_ids, artist_ids = parse_related_content_from_artist(
            artist_data['relatedContent'])
        artist.albums = albums
        artist.playlist_ids = playlist_ids
        artist.artist_ids = artist_ids
    return artist


def parse_discography_from_artist(discography_data: dict, artist_id: str) -> Union[None, List[Album]]:
    if not discography_data:
        return

    # Parse albums
    album_items = []
    if discography_data.get('albums'):
        items = discography_data['albums']['items']
        album_items.extend([item['releases']['items'][0] for item in items])
    if discography_data.get('popularReleasesAlbums'):
        album_items.extend(
            discography_data['popularReleasesAlbums']['items'] or [])
    if discography_data.get('singles'):
        items = discography_data['singles']['items']
        album_items.extend([item['releases']['items'][0] for item in items])
    if discography_data.get('latest'):
        album_items.append(discography_data['latest'])

    albums = []
    for item in album_items:
        if not item:
            continue
        album = parse_album(item)
        album.artist_album = [ArtistAlbum(
            album_id=album.album_id, artist_id=artist_id)]
        albums.append(album)
    return albums


def parse_track_from_playlist(track_data: dict) -> Union[None, Track]:
    if track_data['__typename'] == 'NotFound':
        return

    # Parse album
    album_id = track_data['albumOfTrack']['uri'].replace('spotify:album:', '')

    explicit = track_data['contentRating']['label'].lower() == 'explicit'
    track_id = track_data['uri'].replace('spotify:track:', '')
    url = f'{SPOTIFY_URL}/track/{track_id}'
    track = Track(
        track_id=track_id, album_id=album_id, url=url,
        duration_ms=track_data['trackDuration']['totalMilliseconds'],
        name=track_data['name'], track_number=track_data['trackNumber'],
        explicit=explicit, plays_count=int(track_data['playcount']),
    )

    # Parse artist_ids
    artists_data = track_data.get('artists', {}).get('items', [])
    track.artist_track = [
        ArtistTrack(
            artist_id=artist['uri'].replace('spotify:artist:', ''),
            track_id=track.track_id
        )
        for artist in artists_data
    ]
    return track


def parse_track_web_detail(response) -> Union[None, Track]:
    data = response.json()
    if not data:
        return

    if not data.get('data'):
        raise TemporaryError('Can not get track detail')

    track_data = data['data']['trackUnion']
    if track_data['__typename'] == 'NotFound':
        return

    # Parse album
    album = parse_album(track_data['albumOfTrack'])

    explicit = track_data['contentRating']['label'].lower() == 'explicit'
    url = f'{SPOTIFY_URL}/track/{track_data["id"]}'
    track = Track(
        track_id=track_data['id'], album_id=album.album_id, url=url,
        duration_ms=track_data['duration']['totalMilliseconds'], name=track_data['name'],
        track_number=track_data['trackNumber'], explicit=explicit,
        plays_count=int(track_data['playcount']),
    )

    # Parse artists
    artists_data = track_data['firstArtist']['items']
    if track_data.get('otherArtists'):
        artists_data.extend(track_data['otherArtists']['items'])
    artists = [parse_simplified_artist(artist_data)
               for artist_data in artists_data or [] if artist_data]
    track.artist_track = [ArtistTrack(artist_id=artist.artist_id, track_id=track.track_id)
                          for artist in artists]
    track.artists = artists

    # Parse more albums
    more_albums = []
    for artist_data in artists_data:
        if not artist_data.get('discography'):
            continue
        more_albums.extend(
            parse_discography_from_artist(
                discography_data=artist_data['discography'],
                artist_id=artist_data['id'])
        )
    track.more_albums = more_albums
    return track


def parse_track_from_album(track_data: dict, album_id: str) -> Track:
    track_data = track_data['track']
    artist_ids = [artist['uri'].replace('spotify:artist:', '')
                  for artist in track_data['artists']['items'] if artist]
    track_id = track_data['uri'].replace('spotify:track:', '')
    plays_count = int(track_data['playcount']) \
        if track_data['playcount'] else None
    explicit = track_data['contentRating']['label'].lower() == 'explicit'
    track = Track(
        track_id=track_id, album_id=album_id, disc_number=track_data['discNumber'],
        duration_ms=track_data['duration']['totalMilliseconds'],
        url=f'{SPOTIFY_URL}/track/{track_id}', name=track_data['name'], track_number=track_data['trackNumber'],
        plays_count=plays_count, explicit=explicit
    )
    track.artist_track = [ArtistTrack(artist_id=artist_id, track_id=track_id)
                          for artist_id in artist_ids]
    return track


def parse_simplified_artist(artist_data: dict) -> Artist:
    artist_id = artist_data['id'] if artist_data.get('id') \
        else artist_data['uri'].replace('spotify:artist:', '')
    image = artist_data['visuals']['avatarImage']['sources'][0]['url'] \
        if artist_data['visuals']['avatarImage'] else None
    url = f'{SPOTIFY_URL}/artist/{artist_id}'
    return Artist(
        artist_id=artist_id, url=url, image=image, name=artist_data['profile']['name']
    )


def parse_playlist_detail(playlist_data: dict) -> Playlist:
    user_data = playlist_data['ownerV2']['data']
    user = parse_user(user_data)

    playlist_id = playlist_data['id'] if playlist_data.get('id') \
        else playlist_data['uri'].replace('spotify:playlist:', '')
    collaborative = playlist_data['basePermission'] == 'CONTRIBUTOR'
    image_items = playlist_data.get('images', {}).get('items')
    if image_items:
        cover_image = image_items[0]['sources'][0]['url'] \
            if image_items[0].get('sources') else None
    playlist = Playlist(
        playlist_id=playlist_id, collaborative=collaborative,
        description=playlist_data['description'] or None,
        url=f'{SPOTIFY_URL}/playlist/{playlist_id}',
        followers_count=playlist_data['followers'] or None,
        cover_image=cover_image, name=playlist_data['name'],
        user_id=user.user_id, snapshot_id=playlist_data.get('revisionId'),
        public=True
    )
    playlist.owner = user
    return playlist


def parse_playlist_tracks_web(response) -> Union[None, Playlist]:
    data = response.json()
    if not data:
        return

    if not data.get('data'):
        raise TemporaryError('Can not get playlist tracks')

    playlist_data = data['data']['playlistV2']
    if playlist_data['__typename'] == 'NotFound':
        return

    playlist = parse_playlist_detail(playlist_data)

    users = []
    tracks = []
    playlist_tracks = []
    # Parse tracks
    if playlist_data['content']['__typename'] == 'PlaylistItemsPage':
        track_items = playlist_data['content']['items']
        if not track_items:
            return

        for item in track_items:
            track_data = item['itemV2']['data']
            if track_data['__typename'] != 'Track':
                continue

            track = parse_track_from_playlist(track_data)
            if not track:
                continue

            added_at_str = item['addedAt']['isoString']
            try:
                added_at = datetime.strptime(
                    added_at_str, '%Y-%m-%dT%H:%M:%SZ')
            except ValueError:
                added_at = datetime.strptime(added_at_str.replace(
                    'Z', '+0000'), '%Y-%m-%dT%H:%M:%S.%f%z')

            user = parse_user(item['addedBy']['data']) \
                if item.get('addedBy') else None
            user_id = user.user_id if user else None
            playlist_track = PlaylistTrack(
                playlist_id=playlist.playlist_id, track_id=track.track_id,
                added_at=added_at, added_by=user_id
            )
            if user and user.user_id != playlist.owner.user_id:
                users.append(user)
            tracks.append(track)
            playlist_tracks.append(playlist_track)

    if users:
        playlist.users = users
    playlist.tracks = tracks
    playlist.playlist_tracks = playlist_tracks
    return playlist


def parse_user(user_data: dict) -> User:
    user_id = user_data['id'] if user_data.get('id') \
        else user_data['uri'].replace('spotify:user:', '')
    image = user_data['avatar']['sources'][0]['url'] \
        if user_data.get('avatar') else None
    return User(
        user_id=user_id, url=f'{SPOTIFY_URL}/user/{user_id}',
        name=user_data['name'], image=image
    )


def parse_related_content_from_artist(related_content_date: dict):
    if not related_content_date:
        return

    # Parse albums
    albums = []
    if related_content_date.get('appearsOn'):
        items = related_content_date['appearsOn']['items']
        for item in items:
            if not item:
                continue

            album_data = item['releases']['items'][0]
            album = parse_album(album_data=album_data)
            artist_items = album_data['artists']['items']
            album.artist_albums = [
                ArtistAlbum(
                    album_id=album.album_id,
                    artist_id=artist_item['uri'].replace('spotify:artist:', '')
                )
                for artist_item in artist_items
            ]
            albums.append(album)

    # Parse playlist ids
    playlist_ids = [item['data']['id']
                    for item in related_content_date['featuringV2']['items'] if item] \
        if related_content_date.get('featuringV2') else None

    # Parse more artists
    artist_ids = [item['id'] for item in related_content_date['relatedArtists']['items'] if item] \
        if related_content_date.get('relatedArtists') else None

    return albums, playlist_ids, artist_ids
