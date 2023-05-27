if __name__ == "__main__":
    from Indicator import *
    from Utility import *
else:   # imported from upper folder.
    from Indicator.Indicator import *
    from Indicator.Utility import *

import os, sys
this_dir = os.path.dirname(__file__)
dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)
from config import *
from Exchange.Utility import *

class ProductsPrices_Stream(Indicator):
    def __init__(self, structuralParams, timingParams):
        super().__init__(structuralParams, timingParams)

    def __setInstance__(self, structuralParams, timingParams):
        super().__setInstance__(structuralParams, timingParams)

        self.quote = structuralParams['Quote']
        self.maxStreams = structuralParams['MaxStreams']
        self.interval = structuralParams['Interval']
        self.live = structuralParams['Live']

        self.prices = {}
        self.products = []
        self.products_prices = {}

        # self.products, self.products_prices = self.Maintain_Products(self.maxStreams)

        return

    def SingleStep(self, stepId):
        super().SingleStep(stepId)

        if TimeMachine.tense == Tense.Present:
            self.products, self.products_prices = self.Maintain_Products()
        
        return


    def Maintain_Products(self, nMaxStreams = None):

        products = collect_products(Binance.clientPool, self.quote)
        products_prices = {}

        count = 0
        for product in products:
            count += 1
            if nMaxStreams is not None and count >= nMaxStreams: break
            Binance._singleton.Maintain_Existence_Of_GStream('klines', product['s'], self.interval, live=self.live, max=self.maxStreams)  # False
            products_prices[product['s']] = []

        return products, products_prices
