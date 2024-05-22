import argparse
import logging
from module.appstore import APPStoreScraper
from module.reason_generation import NegReasonGeneration
from datetime import datetime, timedelta


service_logger = logging.getLogger(__name__)


class JMOProcessor:
    def __init__(self):
        self.parser = self.parse_arguments()

    def parse_arguments(self) -> argparse.Namespace:
        """Parse the arguments provided to the operator job, according to the arguments set specified

        return: Arguments object with the parsed arguments
        """
        parser = argparse.ArgumentParser(description='JMO review ETL app')
        subparsers = parser.add_subparsers(title='subcommands', required=True, dest="subcommand")

        # ingest data
        ingest = subparsers.add_parser('scrap-data', help='scrap data from app store and ingest to bigquery')
        ingest.add_argument('--date', help='filter after the date', required=False)
        ingest.set_defaults(func=self.scrap_data)

        # load data
        load = subparsers.add_parser('generate-reason', help='load reason data to bigquery')
        load.set_defaults(func=self.generate_reason)

        # Parse the args
        return parser

    def scrap_data(self, args=None):
        print("Data scrapping started")
        try:
            if args.date:
                date_obj = datetime.strptime(args.date, '%Y-%m-%d').date()
            else:
                # Get today's date
                today = datetime.today()
                # Calculate a week ago
                date_obj = (today - timedelta(days=7)).date()
            
            jmo = APPStoreScraper(date_obj)
            jmo.scrape_data()
        except ValueError:
            logging.ERROR("Invalid date format. Please provide date in YYYY-MM-DD format.")
        
    def generate_reason(self, args=None):
        print("Data reason generation started")
        reason = NegReasonGeneration()
        reason.generate_reason()
        

    def run(self):
        args = self.parser.parse_args()
        # call appropriate function for subcommand
        args.func(args)

if __name__ == "__main__":
    processor = JMOProcessor()
    processor.run()


