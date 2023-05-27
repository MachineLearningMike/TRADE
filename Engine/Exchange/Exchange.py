import os, sys
this_dir = os.path.dirname(__file__)
dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)

from config import *

from abc import ABC, abstractmethod

class Exchange(ABC):

    def __init__(self):
        return
        
    def GetExchangeTimeMili(self):
        return

    def Get_Data(self):
        return

    def Place_Order(self):
        return