import time
from Module.ExchangeRates.update_rates_mc import ExchangeRates
from datetime import datetime
import os, argparse
from datetime import date, timedelta

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)
"""This script is used to launch manually the exchange rate module.
it can be launched with args.

Args:
    function: function to execute [exchange_rates]
    -d,--date: when function = interpretation is date to obtain the exchange rate of the day
"""

if __name__ == "__main__":
    length = 4
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "-sd",
        "--start_date",
        help=" when function = interpretation is date to obtain the exchange rate of the day",
    )
    parser.add_argument(
        "-ed",
        "--end_date",
        help=" when function = interpretation is date to obtain the exchange rate of the day",
    )
    args = parser.parse_args()


    def daterange(start_date, end_date):
        for n in range(int((end_date - start_date).days)):
            yield start_date + timedelta(n)

    def main(start_date: str, end_date: str):
        '''Executed function of the exchange rate extraction module'''

        if start_date != None and end_date != None:
            start_date = datetime.strptime(start_date, "%Y-%m-%d")
            end_date = datetime.strptime(end_date, "%Y-%m-%d")
            n = 1
            for single_date in daterange(start_date.date(), end_date.date()):
                if n > 1:
                    time.sleep(600)
                date_info = datetime.combine(single_date, datetime.min.time())
                ExchangeRates(date_info).updater_process()
                n += 1

        elif start_date != None:
            date_info = datetime.strptime(start_date, "%Y-%m-%d")
            ExchangeRates(date_info).updater_process()
        else:
            date_info = datetime.now()
            ExchangeRates(date_info).updater_process()


    main(args.start_date, args.end_date)
