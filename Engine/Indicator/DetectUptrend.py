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

class GoodIndicator(Indicator):
    def __init__(self, structuralParams, timingParams):
        super().__init__(structuralParams, timingParams)

    def SingleStep(self, stepId):
        super().SingleStep(stepId)

        for gstream_name, gstream_key in self.gstreams.items():
            if gstream_key is not None:
                [dataType, symbol, interval] = gstream_key.split('.')
                start = datetime.now() - timedelta(days=int(datetime.now().microsecond/600000), hours=int(datetime.now().microsecond/50000), minutes=datetime.now().second, seconds=datetime.now().second) #vicious test.
                end = None #datetime.now()

        return