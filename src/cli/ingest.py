from argparse import ArgumentParser

from src.crawler.flow import CrawlerFlow


def main():
    arg_parser = ArgumentParser(description='Spotify Ingestor')

    arg_parser.add_argument('-i', '--ingestor', choices=['album', 'artist', 'playlist'],
                            required=True, help='consume album, artist or playlist')
    arg_parser.add_argument('-q', '--query', type=str,
                            required=True, help='The query to ingest')

    args = arg_parser.parse_args()
    flower = CrawlerFlow()
    if args.ingestor == 'album':
        flower.ingest_albums_by_query(args.query)
    if args.ingestor == 'artist':
        flower.ingest_artists_by_query(args.query)
    if args.ingestor == 'playlist':
        flower.ingest_playlists_by_query(args.query)


if __name__ == '__main__':
    main()
