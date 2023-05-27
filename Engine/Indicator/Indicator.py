# Business License, Mike

if __name__ == "__main__":
    pass
else:   # imported from upper folder.
    pass

import os, sys
this_dir = os.path.dirname(__file__)
dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)

from config import *
from ThreadOnTimeMachine import *
from Exchange.ExchangeFactory import *
from Exchange.GStreamFactory import *

class Indicator(ThreadOnTimeMachine):
    def __init__(self, structuralParams, timingParams):
        ThreadOnTimeMachine.__init__(self, structuralParams, timingParams)

        self.exchanges = set()
        self.gstreams = {}

        if TimeMachine.tense == Tense.Present:
            for gstream_name, gstream_params in structuralParams['GStreams'].items():
                if self.gstreams.get(gstream_name, None) is None:
                    exchnage, gstream = GStreamFactory.CreateGStream(gstream_params, live=True)
                    self.exchanges.add(exchnage)
                    self.gstreams[gstream_name] = gstream
                else:
                    raise Exception('Duplicate GStream Name.')
        
        #self.contractors = [ exchange for exchange in self.exchanges ]
        #for contractor in self.contractors: contractor.clients.append(self)

    def __setInstance__(self, structuralParams, timingParams):
        super().__setInstance__(structuralParams, timingParams)
        self.name = structuralParams['Name']
        self.intervalsPerStep = timingParams['intervalsPerStep']

        return

    def SingleStep(self, stepId):
        super().SingleStep(stepId)
        if Config['Show_Traders']:
            Print("====== Indicator - {}: step {}, currInterval {}".format(self.name, stepId, self.currentInterval))

        return
  