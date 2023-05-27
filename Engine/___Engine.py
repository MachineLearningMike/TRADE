from config import *
import threading
from TimeMachine import *
from Exchange.Exchange import *
from Exchange.Binance import *
from ManualTrader.ManualTrader import *
from AutoTrader.AutoTrader import *
from AITrainer.AITrainer import *
from TraderTrainer.TraderTrainer import *
from ThreadOnTimeMachine import *

from Trader.TraderFactory import *
from Exchange.ExchangeFactory import *
 
from Exchange.Utility import *

class Engine(threading.Thread):

    def __init__(self):
        self.running = False
        self.timing = False
        self.traders = []
        self.exchanges = []
        Print("====== Bot: engine is created.")
 
    def run(self):
        if Config['timing']['tense'] == Tense.Present and Config['Stream_Price']:
            for exchange in self.exchanges:
                if exchange.stop_Maintain_GStreams is None:
                    exchange.Maintain_GStreams()    # Occupys the engine thread for long.
        # Print("Engine finished 'run' function")
        pass

    def Start(self, structuralParams, timingParams):
        self.timing = timingParams

        if not self.running:
            threading.Thread.__init__(self)
            Print("====== Bot: engine is starting...")

            self.traders = [ TraderFactory.CreateTrader(traderParams, timingParams) for _, traderParams in structuralParams['Traders'].items() ]
            Print("====== Bot: Traders, and their strategies/indicators, are created.")

            if Config['Run_Traders']:

                self.Start_Traders()
                Print("====== Bot: Traders, and their strategies/indicators, are running...")

                # I couldn't find a pythonic code for the following block.
                for trader in self.traders:
                    for strategy in trader.contractors:
                        for indicator in strategy.contractors:
                            for exchange in indicator.exchanges:
                                if exchange not in self.exchanges:
                                    self.exchanges.append(exchange)

            self.name = structuralParams['Name']

            self.running = True
            self.start() # run on thread.
          
        return True

    def Start_Traders(self, sleep_sec = 0):
        if Config['Show_Traders']:
            Print("====== Bot: Sleeping {:.1f} seconds before launching the time machine.".format(sleep_sec))
        tm.sleep(sleep_sec)
        
        for trader in self.traders: 
            trader.Start() # This will create Binance._singleton, among others.
        
        return

    def Stop(self):
        if self.running:
            stopped = None

            for trader in self.traders: trader.Stop()
            if Config['Show_Traders']:
                Print("====== Bot: signaled traders to stop.")
    
            for trader in self.traders: trader.Join()
    
            if Config['Show_Traders']:
                Print("====== Bot: traders stopped.")

            signaled = Binance._singleton.Stop_Maintain_GStreams()
            
            if Config['Show_Traders']:
                Print("====== Bot: signaled the streams to stop.")

            if signaled:
                self.join()
                stopped = True
                self.running = False

                if Config['Show_Traders']:
                    Print("====== Bot: streaming stopped.")
            else:
                stopped = False

            if Config['Show_Traders']:
                Print("====== Bot: engine stopped.")

        else:
            stopped = True

        return stopped


    def TestCall(self, message):
        if Config['Show_Traders']:
            Print("====== Bot: engine is called on TestCall: {}".format(message))


    def Get_N_Products(self, dataType = 'klines', symbols_to_include = [], interval = '1m'):
        n_products = 0

        # with Binance.lock_gstream:
        for key, (_, _, _, _, _, t_state) in Binance.gstreams.items():
            [_dataType, _symbol, _interval] = key.split('.')
            if dataType == _dataType and (symbols_to_include is None or _symbol in symbols_to_include) and interval == _interval and t_state & T_State.no_missing:
                n_products += 1
        return n_products


    def Get_Recent_Prices(self, dataType, symbols_to_include, interval, nRows = None, nProducts = None, symbols_to_exclude = None):
        products_prices = {}

        gstreams = {}
        # with Binance.lock_gstream:
        for key, (_, _, _, _, _, t_state) in Binance.gstreams.items():
            gstreams[key] = t_state

        for key, t_state in gstreams.items():
            [_dataType, _symbol, _interval] = key.split('.')
            if symbols_to_exclude is None or _symbol not in symbols_to_exclude:
                if dataType == _dataType and (symbols_to_include is None or _symbol in symbols_to_include) and interval == _interval and t_state & T_State.no_missing:
                    dataframe = Binance._singleton.Get_Recent_Prices(_dataType, _symbol, _interval, nRows)
                    products_prices[_symbol] = dataframe
                    if nProducts is not None and len(products_prices) >= nProducts:
                        break
        
        return products_prices

    def Start_Add_To_Plot(self, Add_To_Plot):
        Binance.Start_Add_To_Plot(Add_To_Plot)
        
    @classmethod
    def ChooseMode(cls, stream=True, show_stream=True, demisser=True, show_demosser=True, traders=True, show_tranders=True, present=True ):
            Config['Stream_Price'] = stream
            Config['Show_Transceiver'] = stream and show_stream
            Config['Run_Demisser'] = demisser
            Config['Show_Demisser'] = demisser and show_demosser
            Config['Run_Traders'] = traders
            Config['Show_Traders'] = traders and show_tranders
            Config['timing']['tense'] = Tense.Present if present else Tense.Past
       

# engine = Engine()
# engine.ChooseMode(
#     stream=True,
#     show_stream=True,
#     demisser=True,
#     show_demosser=True,
#     traders=True, 
#     show_tranders=True,

#     present=True
#     )
# engine.Start(Config['structure'], Config['timing'])
# Print("Finished running '___Engine.py'")