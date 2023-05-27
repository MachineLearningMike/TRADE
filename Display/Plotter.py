import matplotlib.pyplot as plt
import numpy as np

from threading import Thread

import Engine.Exchange.Binance


class Plotter():
    def __init__(self, exchange):

        self.exchange = exchange
        self.gstreams = exchange.gstreams
        self.badKeys = exchange.badKeys

        self.fig = None
        self.axes = None
        self.nRows = 0
        self.nCols = 0

        self.command = None

    def task(self):

        if self.command == "run":

            while True:
                if self.gstreams is not None:
                    gstreams = self.gstreams.copy()
                    
                    for key, gs in gstreams.items():


                


        # = plt.subplots( nrows=nRows, ncols=nCols, figsize = (16, 9) )


        # = plt.subplots( nrows=nRows, ncols=nCols, figsize = (16, 9) )

            
    

# start_time = perf_counter()

# # create two new threads
# t1 = Thread(target=task)
# t2 = Thread(target=task)

# # start the threads
# t1.start()
# t2.start()

# # wait for the threads to complete
# t1.join()
# t2.join()

# end_time = perf_counter()

# print(f'It took {end_time- start_time: 0.2f} second(s) to complete.')