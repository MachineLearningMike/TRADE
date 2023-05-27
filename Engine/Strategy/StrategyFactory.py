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
from Strategy.SimpleStrategy import *


class StrategyFactory():

    def CreateStrategy(structuralParams, timingParams):
        strategy = None

        type = structuralParams['Type']

        if type == 'SimpleStrategy':
            strategy = SimpleStrategy(structuralParams, timingParams)
        else:
            raise Exception('Invalid strategy type')
        
        return strategy