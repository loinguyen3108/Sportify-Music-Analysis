from argparse import ArgumentParser


from src.crawler.flow import CrawlerFlow

consumers = ['album', 'artist', 'playlist', 'track', 'track_plays_count']


def main():
    arg_parser = ArgumentParser(description='Spotify Consumer')

    arg_parser.add_argument('-c', '--consumer', choices=consumers,
                            required=True, help='consume album, artist, playlist or track')
    arg_parser.add_argument('-n', '--num-consumers', type=int, default=1,
                            help='number of consumers')

    args = arg_parser.parse_args()
    flower = CrawlerFlow()
    if args.consumer == 'album':
        flower.ingest_albums()
    if args.consumer == 'artist':
        flower.ingest_artists()
    if args.consumer == 'playlist':
        flower.ingest_playlists()
    if args.consumer == 'track':
        flower.ingest_tracks()
    if args.consumer == 'track_plays_count':
        flower.ingest_track_plays_count()


if __name__ == '__main__':
    main()
