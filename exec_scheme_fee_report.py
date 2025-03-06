import sys
import argparse
import os
from Module.SchemeFee.only_report import scheme_fee as sf

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

if __name__ == "__main__":
    """This script is used to launch manually the scheme fee module.
    it can be launched with args.
    
    Args:
        function: function to execute | [generate_table] generates a table for client in specified month | [read_table] reads tthe table in the specified route of scheme fee´s bucket
        -c,--client: Set the client
        -f,--file: only when read_table : route to file in s3
        -ym, --year_month: Year and month of data in format YYYYMM

    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument(
        "function",
        help="function to execute | generate_table generates a table for client in specified month | read_table reads tthe table in the specified route of scheme fee´s bucket",
    )
    parser.add_argument(
        "-c",
        "--client",
        help=" Set the client ",
    )
    parser.add_argument("-f", "--file", help="only when read_table : route to file in s3")
    parser.add_argument(
        "-ym", "--year_month", help="Year and month of data in format YYYYMM"
    )
    args = parser.parse_args()

    if args.function == "generate_table":
        if args.client and args.year_month:
            sf().generate_table(args.client, args.year_month)
        else:
            if not args.client:
                print("Client is missing")
            if not args.year_month:
                print("year_month is missing")
