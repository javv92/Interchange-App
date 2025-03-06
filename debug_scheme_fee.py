import sys
import argparse
import os
from Module.SchemeFee.managment import scheme_fee as sf

class CustomArgParse:
    function = "generate_table"
    client = 'BTRO'
    year_month = '202211'


abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)

if __name__ == "__main__":
    
    
    args = CustomArgParse()

    if args.function == "generate_table":
        if args.client and args.year_month:
            sf().generate_table(args.client, args.year_month)
        else:
            if not args.client:
                print("Client is missing")
            if not args.year_month:
                print("year_month is missing")

    elif args.function == "read_table":
        if args.client and args.year_month and args.file:
            sf().read_table(args.client, args.file, args.year_month)
        else:
            if not args.client:
                print("Client is missing")
            if not args.year_month:
                print("year_month is missing")
            if not args.file:
                print("file route is missing")
