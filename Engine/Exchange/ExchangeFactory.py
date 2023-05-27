if __name__ == "__main__":
    from Binance import *
else:   # imported from upper folder.
    from Exchange.Binance import *

import os, sys
this_dir = os.path.dirname(__file__)
dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)
from config import *

class ExchangeFactory():

    def CreateExchange(gstreamParams):
        exchange = None
        if gstreamParams['Type'] == 'Binance':
            exchange = Binance.instantiate(gstreamParams)
        else:
            raise Exception('Invalid exchange type')                  
        return exchange