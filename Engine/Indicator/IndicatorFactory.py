if __name__ == "__main__":
    from SimpleIndicator import *
    from GoodIndicator import *
    from ProductsPrices import *
    from ProductsPrices_Stream import *
else:   # imported from upper folder.
    from Indicator.SimpleIndicator import *
    from Indicator.GoodIndicator import *
    from Indicator.ProductsPrices import *
    from Indicator.ProductsPrices_Stream import *

import os, sys
this_dir = os.path.dirname(__file__)
dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)
from config import *
from ThreadOnTimeMachine import *
from Indicator.IndicatorFactory import *

class IndicatorFactory():

    def CreateIndicator(structuralParams, timingParams):
        indicator = None

        type = structuralParams['Type']

        if type == 'SimpleIndicator':
            indicator = SimpleIndicator(structuralParams, timingParams)
        elif type == 'GoodIndicator':
            indicator = GoodIndicator(structuralParams, timingParams)
        elif type == 'ProductsPrices':
            indicator = ProductsPrices(structuralParams, timingParams)
        elif type == 'ProductsPrices_Stream':
            indicator = ProductsPrices_Stream(structuralParams, timingParams)
        else:
            raise Exception('Invalid strategy type')
        
        return indicator