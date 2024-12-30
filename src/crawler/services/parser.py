from datetime import datetime
import re
from typing import List

from src.crawler.models import Album, Artist, Playlist, User, Track, PlaylistTrack


def parse_simplified_artist(data: dict) -> Artist:
    return Artist(
        artist_id=data['id'], url=data['external_urls']['spotify'],
        name=data['name']
    )


def parse_artist(data: dict) -> Artist:
    return Artist(
        artist_id=data['id'], url=data['external_urls']['spotify'],
        followers_count=data['followers']['total'], genres=data.get('genres'),
        image=data['images'][-1]['url'] if data['images'] else None,
        name=data['name'], popularity=data['popularity']
    )


def parse_album_detail(data: dict) -> Album:
    artists = [parse_simplified_artist(artist) for artist in data['artists']]
    album = Album(
        album_id=data['id'], album_type=data['album_type'],
        available_markets=data['available_markets'],
        url=data['external_urls']['spotify'],
        cover_image=data['images'][-1]['url'] if data['images'] else None,
        name=data['name'], release_date=data['release_date'],
        release_date_precision=data['release_date_precision'],
        restrictions=data.get('restrictions'),
        artist_ids=[artist.artist_id for artist in artists]
    )
    album.artists = artists
    return album


def parse_albums_response(response: dict) -> List[Album]:
    if not response:
        return

    items = response['items']
    return [parse_album_detail(item) for item in items if item]


def parse_search_artists_response(response: dict) -> List[Artist]:
    if not response:
        return

    items = response['artists']['items']
    return [parse_artist(item)for item in items]


def parse_search_playlists_response(response: dict) -> List[Playlist]:
    if not response:
        return

    items = response['playlists']['items']
    return [parse_playlist_from_search(item) for item in items if item]


def parse_user_playlists_response(response: dict) -> List[Playlist]:
    if not response:
        return

    items = response['items']
    return [parse_playlist_detail(item) for item in items if item]


def parse_playlist_detail(data: dict) -> Playlist:
    owner = User(
        user_id=data['owner']['id'], url=data['owner']['external_urls']['spotify'],
        name=data['owner']['display_name']
    )

    followers = data.get('followers')
    playlist = Playlist(
        playlist_id=data['id'], collaborative=data['collaborative'],
        description=data['description'], url=data['external_urls']['spotify'],
        followers_count=followers['total'] if followers else None,
        cover_image=data['images'][-1]['url'] if data['images'] else None,
        name=data['name'], public=data['public'], snapshot_id=data['snapshot_id'],
        user_id=owner.user_id
    )
    playlist.owner = owner

    tracks = data.get('tracks', {}).get('items')
    if tracks:
        playlist.tracks = [
            parse_track_detail(track['track']) for track in tracks
            if track['track'] is not None and not track['track']['is_local']
        ]
        playlist.playlist_tracks = [
            parse_playlist_track(track, playlist_id=playlist.playlist_id)
            for track in tracks if track['track'] is not None and not track['track']['is_local']
        ]

    return playlist


def parse_playlist_track(data: dict, playlist_id: str) -> PlaylistTrack:
    added_at = datetime.strptime(data['added_at'], '%Y-%m-%dT%H:%M:%SZ')
    return PlaylistTrack(
        playlist_id=playlist_id, track_id=data['track']['id'],
        added_at=added_at, added_by=data['added_by']['id'],
        url=data['track']['external_urls']['spotify']
    )


def parse_playlist_tracks_response(response: dict) -> List[Track]:
    if not response:
        return

    playlist_id = re.search(r'playlists/(.*)/tracks',
                            response['href']).group(1)
    items = response['items']
    tracks = []
    for item in items:
        if not item['track']:
            continue
        if item['track']['is_local']:
            continue
        track = parse_track_detail(item['track'])
        track.playlist_track = parse_playlist_track(
            item, playlist_id=playlist_id)
        tracks.append(track)
    return tracks


def parse_track_detail(data: dict) -> Track:
    album = parse_album_detail(data['album'])
    artists = [parse_simplified_artist(artist) for artist in data['artists']]
    track = Track(
        track_id=data['id'], album_id=album.album_id,
        artist_ids=[artist.artist_id for artist in artists],
        available_markets=data['available_markets'],
        disc_number=data['disc_number'], duration_ms=data['duration_ms'],
        explicit=data['explicit'], url=data['external_urls']['spotify'],
        name=data['name'], restrictions=data.get('restrictions'),
        track_number=data['track_number'], popularity=data['popularity']
    )
    track.album = album
    track.artists = artists
    return track


def parse_simplified_track(data: dict) -> Track:
    artists = [parse_simplified_artist(artist) for artist in data['artists']]
    return Track(
        track_id=data['id'], available_markets=data['available_markets'],
        artist_ids=[artist.artist_id for artist in artists],
        disc_number=data['disc_number'], duration_ms=data['duration_ms'],
        explicit=data['explicit'], url=data['external_urls']['spotify'],
        name=data['name'], restrictions=data.get('restrictions'),
        track_number=data['track_number']
    )


def parse_tracks_from_album(response: dict) -> List[Track]:
    if not response:
        return

    items = response['items']
    return [parse_simplified_track(item) for item in items if item]


def parse_playlist_from_search(data: dict) -> Playlist:
    owner = User(
        user_id=data['owner']['id'], url=data['owner']['external_urls']['spotify'],
        name=data['owner']['display_name']
    )
    playlist = Playlist(
        playlist_id=data['id'], collaborative=data['collaborative'],
        description=data['description'], url=data['external_urls']['spotify'],
        cover_image=data['images'][-1]['url'] if data['images'] else None,
        name=data['name'], public=data['public'], snapshot_id=data['snapshot_id'],
        user_id=owner.user_id
    )
    playlist.owner = owner
    return playlist
