from Module.ExchangeRates.update_rates import ExchangeRates
from datetime import datetime
import os, argparse

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
        "-d",
        "--date",
        help=" when function = interpretation is date to obtain the exchange rate of the day",
    )
    args = parser.parse_args()

    def main(date_info: str = None):
        '''Executed function of the exchange rate extraction module'''
        if date_info != None:
            date_info = datetime.strptime(date_info, "%Y-%m-%d")
        return ExchangeRates(date_info).updater_process()

    if args.function == "exchange_rates":
        if args.date:
            if __name__ == "__main__":
                all_data = main(args.date)

        else:
            if __name__ == "__main__":
                all_data = main()