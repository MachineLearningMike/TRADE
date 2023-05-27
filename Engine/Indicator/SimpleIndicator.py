from datetime import datetime

if __name__ == "__main__":
    from Indicator import *
else:   # imported from upper folder.
    from Indicator.Indicator import *

import os, sys
this_dir = os.path.dirname(__file__)
dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)
from config import *

from copy import copy
import numpy as np

class SimpleIndicator(Indicator):
    def __init__(self, structuralParams, timingParams):
        super().__init__(structuralParams, timingParams)

        self.trials = 10
        self.milestone = 0


    def SingleStep(self, stepId):
        super().SingleStep(stepId)

        # if Binance.gstreams_ready:
            # for n in range( pow(10, 4) ): a = pow(n, 0.5)



        trials = self.trials
        np.random.seed(None)
        rand = np.random.randint(0, high=60, size=trials, dtype=int)
        rand2 = np.random.randint(0, high=60, size=trials, dtype=int)
        rand3 = np.random.randint(0, high=150, size=1, dtype=int)
        keys = copy(list(Binance.gstreams.keys()))
        # for key in keys[rand3[0]:]:

        # success_cnt = 0; total_cnt = 0

        # if self.milestone > len(keys) - 1:
        #     Print("DOWNLOAD finished")
        #     return

        # for key in keys[self.milestone:]:

        #     trials -= 1
        #     # if trials < 0: break
        #     if success_cnt > self.trials: break
        #     [dataType, symbol, interval] = key.split('.')
        #     if Binance.gstreams.get(key, None) is not None:
        #         start = datetime(2017, 7, 1, 0, 0, 0, 0)    # Bianace began its operation July 2017.
        #         end = datetime.now() # datetime(2023, 2, 20)
        #         # symbol = "DOTUSDT"
        #         # start = get_current_day_start( datetime.now() ) - timedelta(days=5*int(rand2[-trials-1])) + timedelta(hours=int(rand2[trials]), minutes=int(rand[trials]), seconds=5)
        #         # end = from_dt_utc_to_dt_local(self.GetCurrentIntervalTime()) - timedelta(hours=int(rand2[-trials-1]), minutes=int(rand[trials]), seconds=1) # end is exclusive.
        #         # dataframe, nCreated, successful = Binance._singleton.Get_Price_Data_By_Time( dataType, symbol, interval, start, end )
        #         _, _, successful = Binance._singleton.Get_Price_Data_By_Time( dataType, symbol, interval, start, end )
                
        #         if successful: success_cnt += 1
        #         total_cnt += 1

        # self.milestone += total_cnt

            # start = datetime(2021, 3, 1)
            # end = datetime.now() # datetime(2023, 2, 20)
            # [dataType, symbol, interval] = key.split('.')
            # symbol = "ETHUSDT"; interval = "1m"
            # _, _, successful = Binance._singleton.Get_Price_Data_By_Time( dataType, symbol, interval, start, end )
            # break
        

        return