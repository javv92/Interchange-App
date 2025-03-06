from datetime import datetime
import os, argparse
from Module.InterchangeRules.InterchangeRules import interchangeRules

abspath = os.path.abspath(__file__)
dname = os.path.dirname(abspath)
os.chdir(dname)


if __name__ == "__main__":
    """This script is used to launch manually the interchange rules module.
    it can be launched with args.
    
    Args:
       -c,--c: charge type New('TI') or Update('U') ['TI', 'U']
       -brd,--brd: Brand MC or VI
    """
    parser = argparse.ArgumentParser(description='Exchange Rules Upload')

    parser.add_argument("-c", '--c', 
                        choices=['TI', 'U'], 
                        type=str.upper,
                        required=True,
                        dest="type",
                        help="Charge Type: New or Update")

    parser.add_argument("-brd", "--brd",
                        type=str.upper,
                        required=True,
                        dest="brd",
                        help="Brand MC or VI")
                    
    args = parser.parse_args()


    print("Process Interchange Rules // Inicio")
    print("Process Type: " + args.type)
    print("Process Brand: " + str(args.brd))


    def main(date_info: str = None):
        interchangeRules().update_master_interchange(args.type, args.brd)
        return None

    main()