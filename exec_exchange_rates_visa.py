from Module.ExchangeRates.update_rates_visa import ExchangeRates
from datetime import datetime, timedelta
import os, argparse, time

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
    parser.add_argument("function", help="function to execute")
    parser.add_argument(
        "-sd",
        "--start_date",
        help = " when function = interpretation is the start date of the period for obtaining daily exchange rates"
    )
    parser.add_argument(
        "-ed",
        "--end_date",
        help = " when function = interpretation is the end date of the period for obtaining daily exchange rates"
    )
    args = parser.parse_args()

    def main(date_info: str = None):
        '''Executed function of the exchange rate extraction module'''
        if date_info != None:
            date_info = datetime.strptime(date_info, "%Y-%m-%d")
        return ExchangeRates(date_info).updater_process()

    if args.function == "exchange_rates":
        if args.start_date and args.end_date:
            if __name__ == "__main__":
                start = datetime.strptime(args.start_date, "%Y-%m-%d")
                end = datetime.strptime(args.end_date, "%Y-%m-%d")
                delta = timedelta(days=1)
                current_date = start
                while current_date <= end:
                    all_data = main(current_date.strftime("%Y-%m-%d"))
                    current_date += delta
                    time.sleep(600)

        else:
            if __name__ == "__main__":
                all_data = main()