from argparse import ArgumentParser

from src.crawler.flow import CrawlerFlow


def main():
    arg_parser = ArgumentParser(description='Spotify Monitor')

    arg_parser.add_argument('-a', '--action', choices=['produce', 'consume'],
                            required=True, help='Allowed: produce or consume')

    args = arg_parser.parse_args()
    flower = CrawlerFlow()
    if args.action == 'produce':
        flower.produce_monitor_messages()
    if args.action == 'consume':
        flower.monitor_object_consumer()


if __name__ == '__main__':
    main()
