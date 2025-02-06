from argparse import ArgumentParser

from datetime import date, datetime

from src.pipelines.ingestor import SpotifyIngestor
from src.pipelines.loader import SpotifyLoader
from src.pipelines.transformer import SpotifyTransformer


def main():
    arg_parser = ArgumentParser(description='Spotify Batch ETL')

    arg_parser.add_argument('-d', '--extracted-date', type=str,
                            default=date.today().strftime('%Y-%m-%d'),
                            help='The date to extract data')
    arg_parser.add_argument('-s', '--step', choices=['ingest', 'transform', 'load'],
                            required=True, help='Allowed: ingest, transform and load')
    arg_parser.add_argument('-p', '--pharse', choices=['dim', 'fact'],
                            help='Only apply to transform and loadstep.')

    args = arg_parser.parse_args()
    extracted_date = datetime.strptime(args.extracted_date, '%Y-%m-%d').date()
    if args.step == 'ingest':
        batch_ingestor = SpotifyIngestor()
        batch_ingestor.execute()
    if args.step == 'transform':
        transformer = SpotifyTransformer(extracted_date=extracted_date)
        if args.pharse == 'dim':
            transformer.transform_dims()
        if args.pharse == 'fact':
            transformer.transform_facts()
    if args.step == 'load':
        loader = SpotifyLoader()
        loader.make_dim_load_query()
        loader.make_fact_load_query()


if __name__ == '__main__':
    main()
