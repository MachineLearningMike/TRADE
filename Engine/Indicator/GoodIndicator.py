if __name__ == "__main__":
    from Indicator import *
    # from 
else:   # imported from upper folder.
    from Indicator.Indicator import *
    from Exchange.Utility import *

from datetime import datetime

import os, sys
this_dir = os.path.dirname(__file__)
dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)
from config import *

import copy

class GoodIndicator(Indicator):
    def __init__(self, structuralParams, timingParams):
        super().__init__(structuralParams, timingParams)

    def SingleStep(self, stepId):
        super().SingleStep(stepId)

        # if Binance.gstreams_ready:
        # for n in range( pow(10, 4) ): a = pow(n, 0.5)

        currentIntervalTime = self.GetCurrentIntervalTime()

        trials = 10
        keys = copy.copy(list(Binance.gstreams.keys()))
        for key in keys:
            [dataType, symbol, interval] = key.split('.')
            # with Binance.lock_gstream:
            if Binance.gstreams.get(key, None) is not None:
                # consume currentIntervalTime, whether it's Tense.Past or Tense.Present
                    
                trials -= 1
                if trials <= 0: break

        return
