from .ExchangeFactory import *
from threading import Thread


class GStreamFactory():

    def CreateGStream( gstreamParams, live = False ):
        exchange = ExchangeFactory.CreateExchange(gstreamParams)
        gstream_key = exchange.Maintain_Existence_Of_GStream \
        (gstreamParams['dataType'], gstreamParams['symbol'], gstreamParams['interval'], live = live, max =  200)

        return exchange, gstream_key