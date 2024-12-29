from argparse import ArgumentParser

from src.crawler.flow import CrawlerFlow


def main():
    arg_parser = ArgumentParser(description='Spotify Ingestor')

    arg_parser.add_argument('-i', '--ingestor', choices=['artist', 'playlist'],
                            required=True, help='consume artist or playlist')
    arg_parser.add_argument('-t', '--tag', type=str,
                            required=True, help='The tag to ingest')

    args = arg_parser.parse_args()
    flower = CrawlerFlow()
    if args.ingestor == 'artist':
        flower.ingest_artists_by_tag(args.tag)
    if args.ingestor == 'playlist':
        flower.ingest_playlists_by_tag(args.tag)


if __name__ == '__main__':
    main()
