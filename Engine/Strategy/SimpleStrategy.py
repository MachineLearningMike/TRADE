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
from Indicator.IndicatorFactory import *

class SimpleStrategy(ThreadOnTimeMachine):
    def __init__(self, structuralParams, timingParams):
        super().__init__(structuralParams, timingParams)
        self.contractors = [ IndicatorFactory.CreateIndicator(indicatorParams, timingParams)  for _, indicatorParams in structuralParams['Indicators'].items() ]
        for contractor in self.contractors: contractor.clients.append(self)

    def __setInstance__(self, structuralParams, timingParams):
        super().__setInstance__(structuralParams, timingParams)
        self.name = structuralParams['Name']
        self.intervalsPerStep = timingParams['intervalsPerStep']
        return

    def SingleStep(self, stepId):
        super().SingleStep(stepId)
        if Config['Show_Traders']:
            Print("====== Strategy - {}: step {}, currInterval {}".format(self.name, stepId, self.currentInterval)) 
        return
