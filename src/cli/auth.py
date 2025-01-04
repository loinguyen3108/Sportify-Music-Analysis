import os
from argparse import ArgumentParser

from src.crawler.services.spotify import SpotifyService


def main():
    arg_parser = ArgumentParser(description='Spotify authentication')

    arg_parser.add_argument('-u', '--username', type=str, required=True,
                            help='Username to auth')

    args = arg_parser.parse_args()
    service = SpotifyService()
    os.environ['SPOTIFY_CACHE_USERNAME'] = args.username
    service.authenticate()


if __name__ == '__main__':
    main()
