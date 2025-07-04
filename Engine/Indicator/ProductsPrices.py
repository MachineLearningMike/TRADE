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

# import threading

class ProductsPrices(Indicator):
    def __init__(self, structuralParams, timingParams):
        super().__init__(structuralParams, timingParams)

    def __setInstance__(self, structuralParams, timingParams):
        super().__setInstance__(structuralParams, timingParams)

        self.quote = structuralParams['Quote']

        self.structuralParams_thread = structuralParams['StructuralParams_Price_Thread']
        self.timingParams_thread = structuralParams['TimingParams_Price_Thread']

        self.price_thread = None

        self.prices = {}
        self.products = []
        self.products_prices = {}

        return

    def SingleStep(self, stepId):
        super().SingleStep(stepId)

        self.Check_Price_Thread()

        self.prices = collect_prices(Binance.clientPool, self.quote, self.prices)
        if len(self.products) <= 0: # Avoid calling amsp.
            self.products = collect_products(Binance.clientPool, self.quote)
        self.products_prices = get_products_prices(self.products, self.prices)

        if Config['Show_Traders']:
            Print("====== ProductsPrices. len = {}\n".format(len(self.products_prices)))

        return


    def Start_Price_Thread(self):
        self.price_thread = threading.Thread(target = self.Thread_Fun_Maintain_Products_Prices, args = ())
        self.price_thread.start()


    def Check_Price_Thread(self):
        if self.price_thread is not None and self.price_thread._is_stopped is True:
            self.Start_Price_Thread()
