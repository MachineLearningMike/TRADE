if __name__ == "__main__":
    from SimpleTrader import *
else:   # imported from upper folder.
    from Trader.SimpleTrader import *

import os, sys
this_dir = os.path.dirname(__file__)
dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)
from config import *
from ThreadOnTimeMachine import *


class TraderFactory():

    def CreateTrader(structuralParams, timingParams):
        trader = None

        type = structuralParams['Type']

        if type == 'SimpleTrader':
            trader = SimpleTrader(structuralParams, timingParams)
        else:
            raise Exception('Invalid trader type')
        
        return trader
