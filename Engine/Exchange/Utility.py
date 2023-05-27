import re, shutil, time
import pandas
import json
from json import JSONEncoder
from pathlib import Path
from datetime import date, datetime, timedelta
import time as tm
import pytz
import urllib.request
import binance
from numpy.lib.npyio import save
import pandas as pd
import numpy as np
from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError
from pandas.core.frame import DataFrame
import patoolib
import requests

if __name__ == "__main__":
  from enums import *
else:   # imported from upper folder.
  from Exchange.enums import *

import os, sys
this_dir = os.path.dirname(__file__)

dir = os.path.join(this_dir, '..')
if dir not in sys.path: sys.path.append(dir)

from config import *
from Shared_Utility import *


file_not_found = False
no_line_feed = False

def from_dt_local_to_dt_utc( dt_local ):
  now_timestamp = tm.time()
  offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
  dt_utc = dt_local - offset
  dt_utc = dt_utc.replace(tzinfo=pytz.utc)
  return dt_utc

def from_dt_utc_to_dt_local( dt_utc ):
  now_timestamp = tm.time()
  offset = datetime.fromtimestamp(now_timestamp) - datetime.utcfromtimestamp(now_timestamp)
  dt_local = dt_utc + offset
  dt_local = dt_local.replace(tzinfo=None) # ?
  return dt_local


def get_current_day_start( dt_any ):
  return datetime(dt_any.year, dt_any.month, dt_any.day).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=dt_any.tzinfo)

def get_previoius_day_start( dt_any ):
  return (dt_any.replace(hour=0) + timedelta(hours=-1)).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=dt_any.tzinfo)

def get_next_day_start( dt_any ):
  return (dt_any.replace(hour=0) + timedelta(hours=25)).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=dt_any.tzinfo)

def get_current_month_start( dt_any ):
  return datetime(dt_any.year, dt_any.month, 1).replace(hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=dt_any.tzinfo)

def get_next_month_start( dt_any ):
  return (dt_any.replace(day=1) + timedelta(days=32)).replace(day=1, hour=0, minute=0, second=0, microsecond=0).replace(tzinfo=dt_any.tzinfo)

def Extend_Stream(dataType, symbols, intervals, folder, lastIds):
 
  symbolcounter = 0

  paths = {}
  for symbol in symbols:
    Print("[{}/{}] - start extend daily {} {}.".format(symbolcounter+1, len(symbols), symbol, dataType))
    for interval in intervals:
      now_utc = datetime.utcnow().replace(tzinfo = pytz.utc)
      base_path, file_name = get_base_path(now_utc, dataType, symbol, interval)
      path = extend_today_file(base_path, file_name, folder)
      paths[symbol+'.'+interval] = path
    symbolcounter += 1

  return paths


def extend_today_file(path, file_name, folder):
  pass

def get_base_path(datetime_utc, dataType, symbol, interval, dailyNotMonthly = True ):    # do not change them, as they are a part of download url.
  if dataType == 'klines':
    if dailyNotMonthly:
        base_path = "data/spot/daily/klines/{}/{}/".format(symbol.upper(), interval)
        file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, datetime_utc.strftime("%Y-%m-%d"))
    else:
        base_path = "data/spot/monthly/klines/{}/{}/".format(symbol.upper(), interval)
        file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, datetime_utc.strftime("%Y-%m"))     
  elif dataType == 'trades':
    base_path = "data/spot/daily/trades/{}/".format(symbol.upper())
    file_name = "{}-trades-{}.zip".format(symbol.upper(),  datetime_utc.strftime("%Y-%m-%d"))
  elif dataType == 'aggTrades':
    base_path = "data/spot/daily/aggTrades/{}/".format(symbol.upper())
    file_name = "{}-aggTrades-{}.zip".format(symbol.upper(),  datetime_utc.strftime("%Y-%m-%d"))
  else: raise Exception('Invalid dataType.')

  return base_path, file_name

def get_download_url(file_url):
  return "{}{}".format(BASE_URL_DOWNLOAD, file_url)

def get_all_symbols():
  response = urllib.request.urlopen("https://api.binance.com/api/v3/exchangeInfo").read()
  return list(map(lambda symbol: symbol['symbol'], json.loads(response)['symbols']))


def get_file_location(datetime_utc, dataType, symbol, interval, dailyNotMonthly = True):
  base_folder, file_name = get_base_path(datetime_utc, dataType, symbol, interval, dailyNotMonthly = dailyNotMonthly)
  save_folder = os.path.join(os.environ.get('STORE_DIRECTORY'), base_folder)
  save_path = os.path.join(save_folder, file_name)

  return base_folder, file_name, save_folder, save_path


def download_file(current_utc, dataType, symbol, interval, dailyNotMonthly = True): #base_path, file_name, folder=None):
  base_folder, file_name, save_folder, save_path = get_file_location(current_utc, dataType, symbol, interval, dailyNotMonthly = dailyNotMonthly)
  download_path = os.path.join(base_folder, file_name)

  global file_not_found
  file_not_found = False

  csv_path = str(Path(save_path).with_suffix('.csv'))
  if os.path.exists(csv_path):
    Print("\nfile already exists! {}".format(csv_path))
  else:
    # make the directory
    if not os.path.exists(save_folder):
      Path(save_folder).mkdir(parents=True, exist_ok=True)

    try:
      if not os.path.exists(save_path):
        download_url = get_download_url(download_path)
        dl_file = urllib.request.urlopen(download_url)
        length = dl_file.getheader('content-length')
        if length:
          length= int(length)
          blocksize = max(4096,length//100)

        with open(save_path, 'wb') as out_file:
          dl_progress = 0
          Print("\nFile Download: {}".format(save_path))
          while True:
            buf = dl_file.read(blocksize)   
            if not buf:
              break
            dl_progress += len(buf)
            out_file.write(buf)
            done= round(50 * dl_progress / length)
            # sys.stdout.write("\r[%s%s]" % ('#' * done, '.' * (50-done)) )    
            sys.stdout.flush()

      if not os.path.exists(csv_path):
        try:
          patoolib.extract_archive(save_path, outdir=save_folder)
        except:
          os.remove(save_path) # remove, as it may be corrupted.
          csv_path = None
          # download_file(current_utc, dataType, symbol, interval)

      #Commented out, as it would leads to sudden termination. 
      #os.remove(save_path)

    except urllib.error.HTTPError:
      Print("\nFile not found: {}".format(download_url), end="")
      file_not_found = True
      csv_path = None   

  return csv_path

def is_valid_dataframe(dataType, interval, dataframe):
  valid = False

  if dataframe.shape[0] <= 0:
    valid = True
  else:
    interval_mili = intervalToMilliseconds(interval)
    start_mili = dataframe.loc[0,0]
    end_mili = dataframe.loc[dataframe.shape[0]-1,0] + interval_mili
    valid = ( end_mili - start_mili <= (dataframe.shape[0]+1) * interval_mili ) # Prove that shape[0] is large enough and not row is missing.

  return valid


def get_lastIds(paths): # This function knows some of the file structure.
  lastIds = {}

  for key, paths in paths.items():
    if len(paths) <= 0: continue
    path = paths[-1]
    if path is not None and os.path.exists(path):
      [dataType, symbol, interval] = key.split('.')
      dataframe = read_csv_file(dataType, path) # pd.read_csv(path, header=None, index_col=None)
      if dataframe.shape[0] > 0 and dataframe.shape[1] > 0:
        lastIds[key] = dataframe.loc[dataframe.shape[0]-1, 0] # dataframe.loc[dataframe.shape[0]-1][0]
      del dataframe
    else:
      lastIds[key] = None
  
  return lastIds


def create_empty_dynarray():
  return np.empty([0, 12], dtype=float)
  # https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data

def correcdynarray_dtypes(dataType, df):
  if dataType == 'klines':
    # https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data
    df[0] = df[0].astype('int64')     # Open time
    df[1] = df[1].astype('float64')   # Open
    df[2] = df[2].astype('float64')   # High
    df[3] = df[3].astype('float64')   # Low
    df[4] = df[4].astype('float64')   # Close
    df[5] = df[5].astype('float64')   # Volume
    df[6] = df[6].astype('int64')     # Close time
    df[7] = df[7].astype('float64')   # Quote asset volume
    df[8] = df[8].astype('int64')     # Number of trades
    df[9] = df[9].astype('float64')   # Taker buy base asset volume
    df[10] = df[10].astype('float64') # Taker buy quote asset volume
    df[11] = df[11].astype('float64') # Ignore
  return df



def parse_datalines_to_datalines(dataType, datalines):

  if dataType == 'klines':
    # https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data
    length = len(datalines)
    for idx in range(length):
      dl = datalines[idx]
      dl[0]= round(dl[0])     # Open time. You can't use 'int64' here. Lucky, 'int' works as 'int64' here.
      dl[1] = float(dl[1])   # Open
      dl[2] = float(dl[2])   # High
      dl[3] = float(dl[3])   # Low
      dl[4] = float(dl[4])   # Close
      dl[5] = float(dl[5])   # Volume
      dl[6]= round(dl[6])     # Close time You can't use 'int64' here. Lucky, 'int' works as 'int64' here.
      dl[7] = float(dl[7])   # Quote asset volume
      dl[8]= round(dl[8])     # Number of trades
      dl[9] = float(dl[9])   # Taker buy base asset volume
      dl[10] = float(dl[10]) # Taker buy quote asset volume
      dl[11] = float(dl[11]) # Ignore.  You can't round it.
      datalines[idx] = dl
  return datalines


def fetch_save_csv_data(clientPool, binance_wait_sec, sample_utc, dataType, symbol, interval, filepath, dailyNotMonthly = True):
  interval_mili = intervalToMilliseconds(interval)
  start_mili = None; end_mile = None
  if dailyNotMonthly:
    start_mili= round(datetime.timestamp(get_current_day_start(sample_utc))*1000/interval_mili) * interval_mili
    end_mile= round(datetime.timestamp(get_next_day_start(sample_utc))*1000/interval_mili) * interval_mili - interval_mili
  else:
    start_mili= round(datetime.timestamp(get_current_month_start(sample_utc))*1000/interval_mili) * interval_mili
    end_mili= round(datetime.timestamp(get_next_month_start(sample_utc))*1000/interval_mili) * interval_mili - interval_mili

  nRowsToFetch = round((end_mili - start_mili) / interval_mili) + 1
  
  successful = False
  if not dailyNotMonthly:
    Print("Starting to fetch one-month worth candles...", end="")
  
  rows = exact_fetch(clientPool, binance_wait_sec, dataType, symbol, interval, start_mili, end_mili, nRowsToFetch, interval_mili)
  if rows is not None:
    rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
    dataframe = pd.DataFrame(rows)
    successful = create_or_append_csv_file(filepath, dataframe.to_csv(header = False, index = False))

  return successful

def snap_sort_deredundant_in_opening_time(dynarray, interval_mili):
    org = dynarray.shape[0]
    dynarray[:, 0] = (dynarray[:, 0]/interval_mili).astype(int)*interval_mili # opening time
    if interval_mili <= 60000: dynarray[:, 6] = (dynarray[:, 6]/interval_mili).astype(int)*interval_mili # closing time

    # "ALICEUSDT", https://data.binance.vision/data/spot/monthly/klines/ALICEUSDT/3m/ALICEUSDT-3m-2021-02.zip prohibits these lines.
    # index = np.logical_not(np.isnan(dynarray[:, 6]))
    # dynarray[index, 6] = (dynarray[index, 6]/interval_mili).astype(int)*interval_mili # closing time

    if dynarray.shape[0] >= 2: # and interval_mili <= 60000:
        # dynarray = dynarray[np.argsort(dynarray, axis=0)[:,0]] # sort in opening time.
        sort = np.argsort(dynarray[:,0], axis=0)
        dynarray = dynarray[sort]
        delta = dynarray[1:, 0] - dynarray[:-1, 0]
        dynarray = np.insert(dynarray[1:][ delta >= interval_mili ], 0, dynarray[0], axis=0)
  
        # index = np.argmax( delta < interval_mili )
        # if not (index == 0 and delta[index] >= interval_mili):  # some increment is less than interval_mili, including zero or nagatives.
        #   dynarray = dynarray[np.argsort(dynarray, axis=0)[:,0]] # sort in opening time.
    delta = org-dynarray.shape[0]
    if delta > 0:
        Print("R-{} ".format(delta), end="")
  
    return dynarray

def get_from_historic_file(clientPool, binance_wait_sec, sample_utc, dataType, symbol, interval, start_utc, end_utc, dailyNotMonthly = True):
  base_folder, file_name, _, _ = get_file_location(sample_utc, dataType, symbol, interval, dailyNotMonthly)
  folder = os.path.join(os.environ.get('STORE_DIRECTORY'), base_folder)
  filepath = str(Path(os.path.join(folder, file_name)).with_suffix('.csv'))

  dynarray = None
  nCreated = 0
  successful = True

  interval_mili = intervalToMilliseconds(interval)

  if not os.path.exists(filepath):
    path = download_file(sample_utc, dataType, symbol, interval, dailyNotMonthly = dailyNotMonthly)
    if path == None or filepath != path:
      return None, 0, False

    # if path is None:
    #   successful = fetch_save_csv_data(clientPool, binance_wait_sec, sample_utc, dataType, symbol, interval, filepath, dailyNotMonthly = dailyNotMonthly)

  if successful:
    dataframe = read_csv_file(dataType, filepath) #pd.read_csv(file_path)

    if dataframe is not None:
        dynarray = dataframe.to_numpy()
        dynarray = select_from_dynarray_by_time(dataType, dynarray, start_utc, end_utc)
        dynarray = snap_sort_deredundant_in_opening_time(dynarray, interval_mili)

        if dynarray is not None:
            # if dailyNotMonthly:
            #     start_mili = max(round(datetime.timestamp(get_current_day_start(sample_utc))*1000),
            #                     round(datetime.timestamp(start_utc)*1000))
            #     end_mili = min(round(datetime.timestamp(get_next_day_start(sample_utc))*1000) - interval_mili,
            #                 round(datetime.timestamp(end_utc)*1000))
            # else:
            #     start_mili = max(round(datetime.timestamp(get_current_month_start(sample_utc))*1000),
            #                     round(datetime.timestamp(start_utc)*1000))
            #     end_mili = min(round(datetime.timestamp(get_next_month_start(sample_utc))*1000) - interval_mili,
            #                 round(datetime.timestamp(end_utc)*1000))

            # if dynarray.shape[0] <= 0 or dynarray[0][0] != start_mili:
            #     # Print("??? The head data points missing in the file. Filling in ... ")
            #     rows = exact_fetch(clientPool, binance_wait_sec, dataType, symbol, interval, start_mili, start_mili, 1, interval_mili)
            #     if rows is not None and len(rows) == 1:
            #         rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
            #         dynarray = np.insert(dynarray, 0, rows, axis=0)
            #     else:
            #         successful = False
            # if successful and dynarray[-1][0] != end_mili:
            #     # Print("??? The tail data points missing in the file. Filling in ... ")
            #     rows = exact_fetch(clientPool, binance_wait_sec, dataType, symbol, interval, end_mili, end_mili, 1, interval_mili)
            #     if rows is not None and len(rows) == 1:
            #         rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
            #         dynarray = np.append(dynarray, rows, axis=0)
            #     else:
            #         successful = False

            # This will lead to inter-file creationg of missing candles, if a file has outward missing candles.
            start_mili = dynarray[0, 0]
            end_mili = dynarray[-1, 0]
            
            # This will require the previous file and/or the next file, if a file has outward missing candles.
            # start_mili = round(datetime.timestamp(start_utc)*1000/interval_mili) * interval_mili
            # end_mili = round(datetime.timestamp(end_utc)*1000/interval_mili) * interval_mili

            nRowsExpected = round((end_mili - start_mili) / interval_mili) + 1

            if successful and dynarray.shape[0] < nRowsExpected:
                # Print("??? Missing data points found in the file: {}/{}. Filling in ... ".format(dynarray.shape[0], nRowsExpected))
                dynarray, nCreated, successful = create_missing_prices_inland(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, start_mili, end_mili + interval_mili, interval_mili)
                # Print("Done." if successful else "Failed.")
                successful = dynarray.shape[0] == nRowsExpected
                if not successful:
                    # dynarray, nCreated, successful = create_missing_prices(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, start_mili, end_mili + interval_mili, interval_mili)
                    pass                  
                   
        else:
            successful = False
    else:
        # No file, then no fetch no create.
        successful = False
  else:
    # No file, then no fetch no create.
    successful = False

  newline = ""
  global file_not_found
  if file_not_found:
      file_not_found = False
      newline = "\n"

  global no_line_feed
  if successful:
    Print(newline + "OK ", end="")
    no_line_feed = True
  else:
    Print(newline + "NO ", end="")
    no_line_feed = True

  return dynarray, nCreated, successful

def create_missing_prices_inland(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, start_mili, end_mili, ignore_last=True):
    # end_mili is exclusive.
    assert dynarray is not None and dynarray.shape[0] > 0

    nCreated = 0
    successful = True

    interval_mili = intervalToMilliseconds(interval)
    dynarray, nCreated, successful \
    = fill_inward(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, 0, dynarray.shape[0], ignore_last=ignore_last)

    # Give up fill_outward, as it requires the previous file data and/or the next file data.
    # Instead, create missing prices inter-file.
    # nRowsExpexted = round((end_mili-start_mili)/interval_mili ) + 1
    # successful = dynarray.shape[0] == nRowsExpexted
    # if not successful:
    #     dynarray, nCreated, successful \
    #     = fill_outward(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, start_mili, end_mili, ignore_last=ignore_last)

    return dynarray, nCreated, successful

def select_from_dynarray_by_time(dataType, dynarray, start_utc, end_utc):
  timeslot = get_timeslot(dataType)
  start_timestamp= round(datetime.timestamp(start_utc)*1000)

  array = None
  if end_utc != None:
    end_timestamp= round(datetime.timestamp(end_utc)*1000) if end_utc is not None else float('inf')
    array = dynarray[ np.logical_and ( start_timestamp <= dynarray[:, timeslot], dynarray[:, timeslot] <= end_timestamp ) ] # note "start<=" and "<= end".
  else:
    array = dynarray[ ( start_timestamp <= dynarray[:, timeslot] )]

  return array


def fill_inward(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, search_start, search_end, ignore_last=True):
    nCreated = 0
    successful = True
    interval_mili = intervalToMilliseconds(interval)

    while search_start < search_end - 1:    # search_end is exclusive
        last_relative = last_non_missing_index(dynarray[search_start : search_end, 0]/interval_mili)
        # dynarray[search_start: search_start + last_relative + 1] has no missing points
        # dynarray[search_start + last_relative,  search_start + last_relative + 2] does have missing points
        # assert search_start + last_relative + 1 < search_end - 1

        if last_relative is not None:  # and last_relative > 0:
            last = search_start + last_relative
            assert last < search_end - 1, "last_non_missing_index >= search_end -1."
            start_mili = round(dynarray[last, 0] + interval_mili)   # note: + interval_mili
            end_mili = round(dynarray[last + 1, 0]) - interval_mili # note: - interval_mili
            nRowsToFill = round((end_mili - start_mili)/interval_mili) + 1
            
            if nRowsToFill > 0:

                # start_mili is the opentime of the first candle to fetch.
                # end_mili is the opentime of the last candle to tetch.
                # nRowsToFill is not used.
                
                # The missing points should not be available by exact_fetch, becasue they were missing in historic price files.
                rows = exact_fetch(clientPool, binance_wait_sec, dataType, symbol, interval, start_mili, end_mili, nRowsToFill, interval_mili)
                if rows is not None:    # wow
                    try:
                        assert len(rows) == nRowsToFill, \
                            "The number of rows fetched to fill in != requested."
                        rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
                        array = np.array(rows) # dtype should be float64.
                        array = snap_sort_deredundant_in_opening_time(array, interval_mili)
                        assert is_one_incremental( array[:, 0].astype(np.int64) / interval_mili ), \
                            "The One-Incremental rule was violated after fetching"    # time-ordered no-missing.
                        Print("F-{nRowsToFill}".format(), end="")
                    except:
                        rows = None

                if rows is None:
                    array = np.ndarray(shape=(nRowsToFill, 12), dtype=float)

                    # Try to fetch from coincodex
                    start = datetime.fromtimestamp(round(start_mili/1000))
                    end = datetime.fromtimestamp(round(end_mili/1000))
                    # history = get_coincodex_history_numpy(symbol[:-4], start, end) # 4: len("USDT")
                    # if history is not None:
                    #     history = extract_from_coincodex_history(history, start, end, interval_mili)
                    history = fetch_coincodex_history_numpy(symbol[:-4], start, end, interval_mili/1000)

                    latest_mili = dynarray[last, 0]

                    mark = None
                    pArray = None
                    if history is not None:
                        pArray = history[:, 1]
                        mark = -2
                        # vArray = history[:, 2]
                        Print("c-", end="")
                    else:
                        mark = -3
                        Print("t-", end="")
                        # Print("{}, {}, {}".format(symbol, start, end))
                        infra_start_price = dynarray[last, 4]   # choose closing price
                        ultra_end_price = dynarray[last + 1, 4] # choose closing price
                        price_inc_per_row = (ultra_end_price - infra_start_price ) / (nRowsToFill + 1)

                        pArray = np.array([infra_start_price + price_inc_per_row * (i+1) for i in range(nRowsToFill)], dtype=float)

                    array[:, 0] = np.array([latest_mili + interval_mili * (i+1) for i in range(nRowsToFill)], dtype=float)
                    array[:, 1] = pArray  # O
                    array[:, 2] = pArray  # H
                    array[:, 3] = pArray  # L
                    array[:, 4] = pArray  # C
                    array[:, 5] = 0 # No, vArray is not Binance volume   # volume
                    array[:, 6] = array[:, 0] + interval_mili - 1000  # closing timem
                    array[:, 7] = 0 # No, pArray * vArray is not Binance volume  # quote volume
                    array[:, 8] = 0       # the number of trades
                    array[:, 9] = 0       # takerbuy base volume
                    array[:, 10] = 0      # takerbuy quote volume
                    array[:, 11] = mark   
                    Print("{} ".format(nRowsToFill), end="")
                    global no_line_feed
                    no_line_feed = True

                dynarray = np.insert(dynarray, last + 1, array, axis=0)
                nCreated += nRowsToFill
                search_start = last + nRowsToFill
                nRows = dynarray.shape[0]
                search_end = nRows - 1 if ignore_last else nRows

            else:
                successful = False
                # raise Exception("last_non_missing_index found but nRowsToFill <= 0.")
        else:
            break   # last_non_missing_index doesn't exist, and all are not missing.

    return dynarray, nCreated, successful


# def fill_outward(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, start_mili, end_mili, ignore_last=True):
#     nCreated = 0
#     successful = True
#     interval_mili = intervalToMilliseconds(interval)
#     nTotalRows = round((end_mili - start_mili)/interval_mili) + 1

#     mili = dynarray[0, 0] - interval_mili
#     dynarray, nCreated, successful = \
#     fill_outward_single(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, start_mili, mili, ignore_last=ignore_last)

#     if dynarray.shape[0] < nTotalRows:
#         mili = dynarray[-1, 0] + interval_mili
#         dynarray, nCreated2, successful = \
#         fill_outward_single(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, mili, end_mili, ignore_last=ignore_last)
#         nCreated + nCreated2

#     successful = dynarray.shape[0] == nTotalRows

#     return dynarray, nCreated, successful


# def fill_outward_single(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, start_mili, end_mili, ignore_last=True)
#     interval_mili = intervalToMilliseconds(interval)
#     nRowsToFill = round((end_mili-start_mili)/interval_mili) + 1
#     if nRowsToFill > 0:
#         # start_mili is the opentime of the first candle to fetch.
#         # end_mili is the opentime of the last candle to tetch.
#         # nRowsToFill is not used.
        
#         # The missing points should not be available by exact_fetch, becasue they were missing in historic price files.
#         rows = exact_fetch(clientPool, binance_wait_sec, dataType, symbol, interval, start_mili, end_mili, nRowsToFill, interval_mili)
#         if rows is not None:    # wow
#             try:
#                 assert len(rows) == nRowsToFill, \
#                     "The number of rows fetched to fill in != requested."
#                 rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
#                 array = np.array(rows) # dtype should be float64.
#                 array = snap_sort_deredundant_in_opening_time(array, interval_mili)
#                 assert is_one_incremental( array[:, 0].astype(np.int64) / interval_mili ), \
#                     "The One-Incremental rule was violated after fetching"    # time-ordered no-missing.
#                 Print("F-{nRowsToFill}".format(), end="")
#             except:
#                 rows = None

#         if rows is None:
#             array = np.ndarray(shape=(nRowsToFill, 12), dtype=float)

#             # Try to fetch from coincodex
#             start = datetime.fromtimestamp(round(start_mili/1000))
#             end = datetime.fromtimestamp(round(end_mili/1000))
#             history = get_coincodex_history_numpy(symbol[:-4], start, end) # 4: len("USDT")
#             if history is not None:
#                 history = extract_from_coincodex_history(history, start, end, interval_mili)
#             latest_mili = dynarray[last, 0]

#             mark = None
#             pArray = None
#             if history is not None:
#                 pArray = history[:, 1]
#                 mark = -2
#                 # vArray = history[:, 2]
#                 Print("c-", end="")
#             else:
#                 mark = -3
#                 Print("t-", end="")
#                 # Print("{}, {}, {}".format(symbol, start, end))
#                 infra_start_price = dynarray[last, 4]   # choose closing price
#                 ultra_end_price = dynarray[last + 1, 4] # choose closing price
#                 price_inc_per_row = (ultra_end_price - infra_start_price ) / (nRowsToFill + 1)

#                 pArray = np.array([infra_start_price + price_inc_per_row * (i+1) for i in range(nRowsToFill)], dtype=float)

#             array[:, 0] = np.array([latest_mili + interval_mili * (i+1) for i in range(nRowsToFill)], dtype=float)
#             array[:, 1] = pArray  # O
#             array[:, 2] = pArray  # H
#             array[:, 3] = pArray  # L
#             array[:, 4] = pArray  # C
#             array[:, 5] = 0 # No, vArray is not Binance volume   # volume
#             array[:, 6] = array[:, 0] + interval_mili - 1000  # closing timem
#             array[:, 7] = 0 # No, pArray * vArray is not Binance volume  # quote volume
#             array[:, 8] = 0       # the number of trades
#             array[:, 9] = 0       # takerbuy base volume
#             array[:, 10] = 0      # takerbuy quote volume
#             array[:, 11] = mark   # Let -2 mark interpolated candles.
#             Print("{} ".format(nRowsToFill), end="")
#             global no_line_feed
#             no_line_feed = True

#         dynarray = np.insert(dynarray, last + 1, array, axis=0)
#         nCreated += nRowsToFill
#         search_start = last + nRowsToFill
#         nRows = dynarray.shape[0]
#         search_end = nRows - 1 if ignore_last else nRows

#     else:
#         successful = False
#         # raise Exception("last_non_missing_index found but nRowsToFill <= 0.")
#     return dynarray, nCreated, successful


def create_missing_prices_core_inland(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, interval_mili, start_mili, end_mili, nRowsToFill):
    # start_mili is the opentime of the first candle to fetch.
    # end_mili is the opentime of the last candle to tetch.
    # nRowsToFill is not used.
    
    # The missing points should not be available by exact_fetch, becasue they were missing in historic price files.
    rows = exact_fetch(clientPool, binance_wait_sec, dataType, symbol, interval, start_mili, end_mili, nRowsToFill, interval_mili)
    if rows is not None:    # wow
        try:
            assert len(rows) == nRowsToFill, \
                "The number of rows fetched to fill in != requested."
            rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
            array = np.array(rows) # dtype should be float64.
            array = snap_sort_deredundant_in_opening_time(array, interval_mili)
            assert is_one_incremental( array[:, 0].astype(np.int64) / interval_mili ), \
                "The One-Incremental rule was violated after fetching"    # time-ordered no-missing.
            Print("F-{nRowsToFill}".format(), end="")
        except:
            rows = None

    if rows is None:
        array = np.ndarray(shape=(nRowsToFill, 12), dtype=float)

        # Try to fetch from coincodex
        start = datetime.fromtimestamp(round(start_mili/1000))
        end = datetime.fromtimestamp(round(end_mili/1000))

        # history = get_coincodex_history_numpy(symbol[:-4], start, end) # 4: len("USDT")
        # if history is not None:
        #   history = extract_from_coincodex_history(history, start, end, interval_mili)
        history = fetch_coincodex_history_numpy(symbol[:-4], start, end, interval_mili/1000)


        mark = None
        pArray = None
        if history is not None:
          pArray = history[:, 1]
          mark = -2
          # vArray = history[:, 2]
          Print("c-", end="")
        else:
          mark = -3
          Print("t-", end="")
          # Print("{}, {}, {}".format(symbol, start, end))
          pArray = np.array([infra_start_price + price_inc_per_row * (i+1) for i in range(nRowsToFill)], dtype=float)
        array[:, 0] = np.array([latest_mili + interval_mili * (i+1) for i in range(nRowsToFill)], dtype=float)
        array[:, 1] = pArray  # O
        array[:, 2] = pArray  # H
        array[:, 3] = pArray  # L
        array[:, 4] = pArray  # C
        array[:, 5] = 0 # No, vArray is not Binance volume   # volume
        array[:, 6] = array[:, 0] + interval_mili - 1000  # closing timem
        array[:, 7] = 0 # No, pArray * vArray is not Binance volume  # quote volume
        array[:, 8] = 0       # the number of trades
        array[:, 9] = 0       # takerbuy base volume
        array[:, 10] = 0      # takerbuy quote volume
        array[:, 11] = mark   # Let -2 mark interpolated candles.
        Print("{} ".format(nRowsToFill), end="")
        global no_line_feed
        no_line_feed = True
    return


def create_missing_prices_core(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, interval_mili, search_start, search_end, ignore_last = False):
    nCreated = 0
    successful = True

    while search_start < search_end - 1:    # search_end is exclusive
        last_relative = last_non_missing_index(dynarray[search_start : search_end, 0]/interval_mili)
        # dynarray[search_start: search_start + last_relative + 1] has no missing points
        # dynarray[search_start + last_relative,  search_start + last_relative + 2] does have missing points
        # assert search_start + last_relative + 1 < search_end - 1


        if last_relative is not None:  # and last_relative > 0:
            last = search_start + last_relative
            assert last < search_end - 1, "last_non_missing_index >= search_end -1."
            start_mili = round(dynarray[last, 0] + interval_mili)   # note: + interval_mili
            end_mili = round(dynarray[last + 1, 0]) - interval_mili # note: - interval_mili
            nRowsToFill = round((end_mili - start_mili)/interval_mili) + 1
            infra_start_price = dynarray[last, 4]   # choose closing price
            ultra_end_price = dynarray[last + 1, 4] # choose closing price
            price_inc_per_row = (ultra_end_price - infra_start_price ) / (nRowsToFill + 1)

            if nRowsToFill > 0:
                # start_mili is the opentime of the first candle to fetch.
                # end_mili is the opentime of the last candle to tetch.
                # nRowsToFill is not used.
                
                # The missing points should not be available by exact_fetch, becasue they were missing in historic price files.
                rows = exact_fetch(clientPool, binance_wait_sec, dataType, symbol, interval, start_mili, end_mili, nRowsToFill, interval_mili)
                if rows != None:    # wow
                    try:
                        assert len(rows) == nRowsToFill, \
                            "The number of rows fetched to fill in != requested."
                        rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
                        array = np.array(rows) # dtype should be float64.
                        array = snap_sort_deredundant_in_opening_time(array, interval_mili)
                        assert is_one_incremental( array[:, 0].astype(np.int64) / interval_mili ), \
                            "The One-Incremental rule was violated after fetching"    # time-ordered no-missing.
                    except:
                        rows = None

                if rows == None:
                    latest = dynarray[last]

                    # rows = [ [
                    #     latest[0] + interval_mili * (i+1),   # open time

                    #     infra_start_price + price_inc_per_row * (i+1), # Open
                    #     infra_start_price + price_inc_per_row * (i+1), # High
                    #     infra_start_price + price_inc_per_row * (i+1), # Low
                    #     infra_start_price + price_inc_per_row * (i+1), # Close
                    #     0,  # Volume
                    #     latest[6] + interval_mili * (i+1),   # closing time. should be interval_mili - 1000 greater than open time.
                    #     0,  # Quote asset volume
                    #     0,  # Number of trades
                    #     0,  # Taker buy base asset volume
                    #     0,  # Taker buy quote asset volume
                    #     latest[11],     # Ignore.  You can't round it.
                    # ] for i in range(nRowsToFill) ]

                    # rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
                    # array = np.array(rows) # dtype should be float64.

                    array = np.ndarray(shape=(nRowsToFill, 12), dtype=float)
                    array[:, 0] = np.array([latest[0] + interval_mili * (i+1) for i in range(nRowsToFill)], dtype=float)
                    pArray = np.array([infra_start_price + price_inc_per_row * (i+1) for i in range(nRowsToFill)], dtype=float)
                    array[:, 1] = pArray  # O
                    array[:, 2] = pArray  # H
                    array[:, 3] = pArray  # L
                    array[:, 4] = pArray  # C
                    array[:, 5] = 0       # base volume
                    array[:, 6] = array[:, 0] + interval_mili - 1000  # closing time
                    array[:, 7] = 0       # quote volume
                    array[:, 8] = 0       # the number of trades
                    array[:, 9] = 0       # takerbuy base volume
                    array[:, 10] = 0      # takerbuy quote volume
                    array[:, 11] = -1   # Let -1 mark interpolated candles.
                    Print("{} ".format(nRowsToFill), end="")
                    global no_line_feed
                    no_line_feed = True

                dynarray = np.insert(dynarray, last + 1, array, axis=0)
                nCreated += nRowsToFill
                search_start = last + nRowsToFill
                nRows = dynarray.shape[0]
                search_end = nRows - 1 if ignore_last else nRows

            else:
                successful = False
                # raise Exception("last_non_missing_index found but nRowsToFill <= 0.")
        else:
            break   # last_non_missing_index doesn't exist, and all are not missing.

    return dynarray, nCreated, successful


def PrePad_with_Zero(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, interval_mili, start_utc):
    # assume dynarray.shape[0] > 0
    first_ts_mili = round(datetime.timestamp(start_utc)*1000/interval_mili) * interval_mili
    last_ts_mili = round(dynarray[0][0] - interval_mili)
    nRowsToFill = round((last_ts_mili - first_ts_mili)/interval_mili) + 1

    row0 = [
        first_ts_mili,   # open time
        0, 0, 0, 0, 0, # OHLC, V,
        first_ts_mili + interval_mili - 1000, # closing time
        0, 0, 0, 0, -1,   # -1 as the flag of created candle.
    ]
    row1 = [
        last_ts_mili,   # open time
        0, 0, 0, 0, 0, # OHLC, V,
        last_ts_mili + interval_mili - 1000, # closing time
        0, 0, 0, 0, -1,   # -1 as the flag of created candle.
    ]

    def add_rows(dynarray, rows):
        rows = parse_datalines_to_datalines(dataType, rows) # Convert string to float.
        array = np.array(rows) # dtype should be float64.       
        dynarray = np.insert(dynarray, 0, array, axis=0)
        return dynarray

    nCreated = nRowsToFill; successful = True
    if nRowsToFill <= 0:    # should be 0
        pass
    elif nRowsToFill <= 1:  # should be 1
        rows = [row0]
        dynarray = add_rows(dynarray, rows)
    elif nRowsToFill <= 2:  # should be 2
        rows = [row0, row1]
        dynarray = add_rows(dynarray, rows)
    else:                   # should be > 2
        rows = [row0, row1]
        dynarray = add_rows(dynarray, rows)
        dynarray, _nCreated, successful = \
        create_missing_prices_core(clientPool, binance_wait_sec, dynarray, dataType, symbol, interval, interval_mili, 0, 2)
        nCreated = _nCreated + 2
    
    return dynarray, nCreated, successful


import dateutil
# custom Decoder
def DecodeDateTime(dict):
   if "checkedFrom" in dict:
      dict["checkedFrom"] = dateutil.parser.parse(dict["checkedFrom"]).replace(tzinfo=pytz.utc)
   if "checkedTo" in dict:
      dict["checkedTo"] = dateutil.parser.parse(dict["checkedTo"]).replace(tzinfo=pytz.utc)
   if "checkedDataStart" in dict:
      dict["checkedDataStart"] = dateutil.parser.parse(dict["checkedDataStart"]).replace(tzinfo=pytz.utc)
   if "latestDataStart" in dict:
      dict["latestDataStart"] = dateutil.parser.parse(dict["latestDataStart"]).replace(tzinfo=pytz.utc)
   return dict

def readOrCreateTokenLookup():
    tokenLookup = {}
    try:
        with open(Config['pathMarketLookup'],'r') as data:
            tokenLookup = json.load(data)
            for key, value in tokenLookup.items():
                try:
                  tokenLookup[key] = DecodeDateTime(value)
                except:
                  pass

# end_utc = min(end_utc, datetime.strptime(value["end"], '%Y-%m-%d %H:%M:%S').replace(tzinfo=pytz.utc))

    except:
        with open(Config['pathMarketLookup'], 'w') as file:
            file.write(json.dumps(tokenLookup, default=str, indent=4))

    return tokenLookup


# subclass JSONEncoder
class DateTimeEncoder(JSONEncoder):
        #Override the default method
        def default(self, obj):
            if isinstance(obj, (datetime.date, datetime.datetime)):
                return obj.isoformat()

def updateTokenLookup(tokenLookup): # inclusive
    successful = True
    try:
        with open(Config['pathMarketLookup'], 'w') as file:
            file.write(json.dumps(tokenLookup, default=str, indent=4, sort_keys=True, cls=DateTimeEncoder))
    except:
        successful = False
    
    return successful


def select_from_single_historic_file_by_id(sample_utc, dataType, symbol, interval, start_id, end_id):
  base_folder, file_name, _, _ = get_file_location(sample_utc, dataType, symbol, interval)
  folder = os.path.join(os.environ.get('STORE_DIRECTORY'), base_folder)
  filepath = str(Path(os.path.join(folder, file_name)).with_suffix('.csv'))

  if not os.path.exists(filepath):
    download_file(sample_utc, dataType, symbol, interval)

  dataframe = pd.read_csv(dataType, filepath)
  return select_from_dataframe_by_id(dataType, dataframe, start_id, end_id )


def select_from_dataframe_by_id(dataType, dataframe, start_id, end_id ): # end_id INCLUSIVE.
  dataframe = dataframe.loc[ ( start_id <= dataframe[0] ) & ( dataframe[0] <= end_id ) ]
  return dataframe


def accept_stream_data(lock, dataType, symbol, interval, dynarray, t_filepath, tickbuffer, data, live = False):
  Print(data['s'], data['k']['i'], 'live' if live else '')

  report = 'success'

  if dataType == 'klines':
    pass
    """
    # https://github.com/binance/binance-spot-api-docs/blob/master/rest-api.md#klinecandlestick-data
    df[0] = df[0].astype('int64')     # Open time
    df[1] = df[1].astype('float64')   # Open
    df[2] = df[2].astype('float64')   # High
    df[3] = df[3].astype('float64')   # Low
    df[4] = df[4].astype('float64')   # Close
    df[5] = df[5].astype('float64')   # Volume --- guess it's base voluem + quote volume. This guess is wrong. It's purely base volume.
    df[6] = df[6].astype('int64')     # Close time
    df[7] = df[7].astype('float64')   # Quote asset volume
    df[8] = df[8].astype('int64')     # Number of trades
    df[9] = df[9].astype('float64')   # Taker buy base asset volume
    df[10] = df[10].astype('float64') # Taker buy quote asset volume
    df[11] = df[11].astype('float64') # Ignore
    """
    """
    # https://github.com/binance/binance-spot-api-docs/blob/master/web-socket-streams.md#klinecandlestick-streams
    {
      "e": "kline",     // Event type
      "E": 123456789,   // Event time
      "s": "BNBBTC",    // Symbol
      "k": {
        "t": 123400000, // Kline start time
        "T": 123460000, // Kline close time
        "s": "BNBBTC",  // Symbol
        "i": "1m",      // Interval
        "f": 100,       // First trade ID
        "L": 200,       // Last trade ID
        "o": "0.0010",  // Open price
        "c": "0.0020",  // Close price
        "h": "0.0025",  // High price
        "l": "0.0015",  // Low price
        "v": "1000",    // Base asset volume
        "n": 100,       // Number of trades
        "x": false,     // Is this kline closed?
        "q": "1.0000",  // Quote asset volume
        "V": "500",     // Taker buy base asset volume
        "Q": "0.500",   // Taker buy quote asset volume
        "B": "123456"   // Ignore
      }
    }
    """
    eventtime = data['E']
    kstream = data['k']
    starttime_stream = kstream['t']
    closetime_stream = kstream['T']
    closed = kstream['x']

    if live or closed:

      (open, close, high, low, ntrades, vbase, vquote, vbasetakerbuy, vquotetakerbuy) = \
        (kstream['o'], kstream['c'], kstream['h'], kstream['l'], kstream['n'], kstream['v'], kstream['q'], kstream['V'], kstream['Q'])

      #======================= Append and pop from tickbuffer.
      tickbuffer.append((eventtime, open, close, high, low, ntrades, vbase, vquote, vbasetakerbuy, vquotetakerbuy))
      # Ugly but fastest way to remove some first elements in an ordered list.
      intervalmili = intervalToMilliseconds(interval)
      fromtime = eventtime - intervalmili # ---------- Design decision: one interval of streaming data will be maintained in the tickbuffer.
      length_initial = len(tickbuffer)
      max_idx = 0
      while max_idx < length_initial:
        if tickbuffer[max_idx][0] >= fromtime: break # [0] for eventtime. Note eventtime is not exactly same with open even when closed == True. Slightly larger.
        else: max_idx += 1

      with lock: del tickbuffer[: max_idx]

      #========================= Update dataframe
      volume = float(kstream['v']) # + float(kstream['q'])  This guess is wrong.
      ignore = kstream['B']
      new_row_raw = [starttime_stream, open, high, low, close, volume, closetime_stream, vquote, ntrades, vbasetakerbuy, vquotetakerbuy, ignore]
      [new_row] = parse_datalines_to_datalines(dataType, [new_row_raw]) # Convert string to float.


      with lock:
          shape = dynarray.shape[0]
          if shape[0] <= 0:
            dynarray.loc[0] = new_row

          else:
            opentime_framelast = dynarray.loc[dynarray.shape[0]-1][0].astype(dynarray.dtypes[0])

            if opentime_framelast >= starttime_stream:
              # Stream is talking about the same candle as the frame last candle.        
              dynarray.loc[dynarray.shape[0]-1] = new_row # closed or not.

            else:
              # Stream is talking about a candle that is later than the frame last candle.
              if opentime_framelast + intervalmili < starttime_stream: # some candles are missing in the frame.
                report = 'frame_lagging'

              if not is_date_changing(starttime_stream):
                dynarray.loc[dynarray.shape[0]] = new_row # closed or not.
                # commented out for speed. dynarray = correcdynarray_dtypes(dataType, dynarray)
              else:
                df_inc_yeday = pd.DataFrame()
                df_inc_today = pd.DataFrame([new_row])
                today_utc = datetime.utcfromtimestamp(1,0 * starttime_stream/1000 + 1) # + 1 for safety.
                change_date(dataType, symbol, interval, dynarray, t_filepath, df_inc_yeday, df_inc_today, today_utc ) # closed or not.

    #======================= Check for lagging.
    last = dynarray.loc[dynarray.shape[0]-1]
    opentime_framelast = last[0].astype(dynarray.dtypes[0])
    if opentime_framelast < starttime_stream - intervalToMilliseconds(interval):
      report = 'frame_lagging'


  else:
    raise Exception('Unsupported dataType: {}'.format(dataType))

  return report


def is_date_changing(timestampmili):
  dt_utc = datetime.utcfromtimestamp(timestampmili/1000)
  return timestampmili == round(datetime.timestamp(get_current_day_start(dt_utc))*1000)


def change_date(dataType, symbol, interval, dynarray, t_filepath, df_inc_yeday, df_inc_today, today_utc ):

  if dataType == 'klines':
    # Close dynarray and t_filepath with df_yeday, and make them yesterday.
    create_or_append_csv_file(t_filepath, df_inc_yeday.to_csv(header = False, index = False))
    dynarray = read_csv_file(dataType, t_filepath) # Just try the file.
    yeday_utc = today_utc - timedelta(days=1)
    base_folder, file_name, save_folder, save_path = get_file_location(yeday_utc, dataType, symbol, interval)
    folder = os.path.join(os.environ.get('STORE_DIRECTORY'), base_folder)
    yeday_path = str(Path(os.path.join(folder, file_name)).with_suffix('.csv'))

    os.remove(yeday_path) # Just in case.
    os.rename(t_filepath, yeday_path)
    del dynarray, t_filepath

    # Open a new dynarray and t_filepath, and fill in them with df_today.
    base_folder, file_name, _, _ = get_file_location(today_utc, dataType, symbol, interval)
    file_name = '_' + file_name #----------------------
    folder = os.path.join(os.environ.get('STORE_DIRECTORY'), base_folder)
    t_filepath = str(Path(os.path.join(folder, file_name)).with_suffix('.csv'))

    os.remove(t_filepath) # In case.
    create_or_append_csv_file(t_filepath, df_inc_today.to_csv(header = False, index = False))
    dynarray = read_csv_file(dataType, t_filepath)
    
  else:
    raise Exception('Unsupported dataType: {}'.format(dataType))

  return dynarray, t_filepath


def get_utctime_by_id(client, dataType, symbol, interval, id):

  utctime = None
  
  if id is None: 
    utctime = None
  else:
    data = get_data_for_single_id(client, dataType, symbol, interval, id)
    if data is None:
      utctime = None
    else:
      if dataType == 'klines':
        ts = data[0]
        utctime = datetime.utcfromtimestamp(ts/1000)
      elif dataType == 'trades':
        ts = data['time']
        utctime = datetime.utcfromtimestamp(ts/1000)
      elif dataType == 'aggTrades':
        ts = data['T']
        utctime = datetime.utcfromtimestamp(ts/1000)
      else:
        raise Exception('Invalid dataType.')

  if utctime is not None: utctime.replace(tzinfo=pytz.utc)
  
  return utctime


def get_data_for_single_id(client, dataType, symbol, interval, id):
  dataline = None
  
  if dataType == 'klines':
    #klines = client.get_historical_klines(symbol, interval, start_str = str(id), end_str = None, limit = 1) #  Defaults to SPOT.
    klines = client.get_klines(symbol=symbol, interval=interval, limit=1, startTime=id, endTime=None)

    if len(klines) != 1:
      raise Exception('Inconsistent response from Binance - 00.')
    else:
      dataline = klines[0]
  elif dataType == 'trades':
    trades = client.get_historical_trades( symbol, 1, id )
    if len(trades) != 1: 
      raise Exception('Inconsistent response from Binance - 01.')
    else:
      dataline = trades[0]
  elif dataType == 'aggTrades':
    aggTrades = client.get_aggregate_trades(symbol, id, None, None, 1)
    if len(aggTrades) != 1:
      raise Exception('Inconsistent response from Binance - 02.')
    else:
      dataline = aggTrades[0]
  else:
    raise Exception('Invalid dataType.')
  
  return dataline


def max_fetch_data_from_last(clientPool, dataType, symbol, interval, lastId):

  #dataframe = pd.DataFrame()
  maxLimit = 500
  nReceived = maxLimit
  datalines = []

  Print("------- max_fetch_data_from_last called. {} lastId: {}".format(symbol, lastId))

  trials = 0

  while nReceived >= maxLimit:

    try:
      if dataType == 'klines':
        with clientPool.lock:
          tm.sleep(Config['binance_wait_sec'])
          lines = clientPool.Next().get_klines(symbol = symbol, interval = interval, limit = maxLimit, startTime = lastId + 1, endTime = None)
      elif dataType == 'trafes':
        lines = None
  
      elif dataType == 'aggTrades':
        with clientPool.lock:
          tm.sleep(Config['binance_wait_sec'])
          lines = clientPool.Next().get_aggregate_trades(symbol = symbol, fromId = lastId + 1, startTime = None, endTime = None, limit = maxLimit)
    except:
      trials += 1
      Print("Binance client was disconnectd.")
      if trials <= 3: continue

    nReceived = len(lines)
    if len(lines) > 0: 
      lastId = lines[-1][0]
      datalines += lines

  # Note: Most of the time, the last line is under construction if dataType == 'klines' or 'aggTrades'?
  # Do NOT remove them, though, because it sometimes has just been completed.

  return datalines


def fetch_full_day_data_from_last(clientPool, dataType, symbol, interval, lastId):
  maxLimit = 500
  nToReceive= round(24*60*60*1000 / intervalToMilliseconds(interval))
  datalines = []

  Print("------- fetch_full_day_data_from_last called. {} lastId: {}".format(symbol, lastId))

  trials = 0
  nReceived = maxLimit

  while nToReceive > 0:
    maxLimit = min(nToReceive, 500)

    try:
      if dataType == 'klines':
        with clientPool.lock:
          tm.sleep(Config['binance_wait_sec'])
          lines = clientPool.Next().get_klines(symbol = symbol, interval = interval, limit = maxLimit, startTime = lastId + 1, endTime = None)
      elif dataType == 'trafes':
        lines = None
  
      elif dataType == 'aggTrades':
        with clientPool.lock:
          tm.sleep(Config['binance_wait_sec'])
          lines = clientPool.Next().get_aggregate_trades(symbol = symbol, fromId = lastId + 1, startTime = None, endTime = None, limit = maxLimit)
    except:
      trials += 1
      if trials < 3: continue

    nReceived = len(lines)
    if len(lines) > 0: 
      lastId = lines[-1][0]
      datalines += lines

    nToReceive -= nReceived

  return datalines


def create_full_day_data(clientPool, dataType, symbol, interval, prev_day_lastId, sampletime_utc):
  lines = fetch_full_day_data_from_last(clientPool, dataType, symbol, interval, prev_day_lastId)
  if len(lines) > 0:
    dataframe = pd.DataFrame(lines)

    base_folder, file_name, save_folder, save_path = get_file_location(sampletime_utc, dataType, symbol, interval)
    #download_path = os.path.join(base_folder, file_name)
    csv_path = str(Path(save_path).with_suffix('.csv'))

    create_or_append_csv_file(csv_path, dataframe.to_csv(header = False, index = False))
    dataframe = read_csv_file(dataType, csv_path)

    lastId= round( dataframe.loc[dataframe.shape[0]-1][0] )# for klines only.

    return lastId


def get_timeslot(dataType):
  return 0 if dataType == 'klines' else 3 if dataType == 'trades' else 5 if dataType == 'aggTrades' else None


def read_csv_file(dataType, file_path):
  dataframe = None
  try:
    dataframe = pd.read_csv(file_path, header=None, index_col=None)
  except:
    datetime = None

  return dataframe

def write_csv_file(dataframe, filepath):
  dataframe.to_csv(filepath, columns=None, header=False, index=False)

def create_or_append_csv_file(file_path, csv_data):
  if os.path.exists(file_path):
    append_write = 'a'
  else:
    append_write = 'w'
  
  successful = True
  try:
    file = open(file_path, append_write)
    #last_line = file.readlines()[-1]
    #if not (last_line is '\n' or last_line is '\n\r'):
    #  file.write('\n\r')
    file.write(csv_data)
    #file.write('\n\r')
    file.close()
  except:
    successful = False

  return successful


def intervalToMilliseconds(interval):
    """Convert a Binance interval string to milliseconds

    :param interval: Binance interval string 1m, 3m, 5m, 15m, 30m, 1h, 2h, 4h, 6h, 8h, 12h, 1d, 3d, 1w
    :type interval: str

    :return:
        None if unit not one of m, h, d or w
        None if string not in correct format
        int value of interval in milliseconds
    """
    ms = None
    seconds_per_unit = {
        "m": 60,
        "h": 60 * 60,
        "d": 24 * 60 * 60,
        "w": 7 * 24 * 60 * 60
    }

    unit = interval[-1]
    if unit in seconds_per_unit:
        try:
            ms= int(interval[:-1]) * seconds_per_unit[unit] * 1000
        except ValueError:
            pass
    return ms

def convert_to_date_object(d):
  year, month, day = [round(x) for x in d.split('-')]
  date_obj = datetime.date(year, month, day)
  return date_obj

def match_date_regex(arg_value, pat=re.compile(r'\d{4}-\d{2}-\d{2}')):
  if not pat.match(arg_value):
    raise ArgumentTypeError
  return arg_value

def check_directory(arg_value):
  if os.path.exists(arg_value):
    while True:
      option = input('Folder already exists! Do you want to overwrite it? y/n  ')
      if option != 'y' and option != 'n':
        Print('Invalid Option!')
        continue
      elif option == 'y':
        shutil.rmtree(arg_value)
        break
      else:
        break
  return arg_value

"""
def get_parser(parser_type):
  parser = ArgumentParser(description=("This is a script to download historical {} data").format(parser_type), formatter_class=RawTextHelpFormatter)
  parser.add_argument(
      '-s', dest='symbols', nargs='+',
      help='Single symbol or multiple symbols separated by space')
  parser.add_argument(
      '-y', dest='years', default=YEARS, nargs='+', choices=YEARS,
      help='Single year or multiple years separated by space\n-y 2019 2021 means to download {} from 2019 and 2021'.format(parser_type))
  parser.add_argument(
      '-m', dest='months', default=MONTHS,  nargs='+', type=int, choices=MONTHS,
      help='Single month or multiple months separated by space\n-m 2 12 means to download {} from feb and dec'.format(parser_type))
  parser.add_argument(
      '-d', dest='dates', nargs='+', type=match_date_regex,
      help='Date to download in [YYYY-MM-DD] format\nsingle date or multiple dates separated by space\ndownload past 35 days if no argument is parsed')
  parser.add_argument(
      '-startDate', dest='startDate', type=match_date_regex,
      help='Starting date to download in [YYYY-MM-DD] format')
  parser.add_argument(
      '-endDate', dest='endDate', type=match_date_regex,
      help='Ending date to download in [YYYY-MM-DD] format')
  parser.add_argument(
      '-folder', dest='folder', type=check_directory,
      help='Directory to store the downloaded data')
  parser.add_argument(
      '-c', dest='checksum', default=0, type=int, choices=[0,1],
      help='1 to download checksum file, default 0')

  if parser_type == 'klines':
    parser.add_argument(
      '-i', dest='intervals', default=INTERVALS, nargs='+', choices=INTERVALS,
      help='single kline interval or multiple intervals separated by space\n-i 1m 1w means to download klines interval of 1minute and 1week')

  return parser

"""




#https://code.luasoftware.com/tutorials/cryptocurrency/python-connect-to-binance-api/

import time
import json
import hmac
import hashlib
import requests
from urllib.parse import urljoin, urlencode

API_KEY = 'UIGu...'
SECRET_KEY = 'VyX...'
BASE_URL_API = 'https://api.binance.com/'

headers = {
    'X-MBX-APIKEY': API_KEY
}

class BinanceException(Exception):
    def __init__(self, status_code, data):

        self.status_code = status_code
        if data:
            self.code = data['code']
            self.msg = data['msg']
        else:
            self.code = None
            self.msg = None
        message = f"{status_code} [{self.code}] {self.msg}"

        # Python 2.x
        # super(BinanceException, self).__init__(message)
        super().__init__(message)

def Get_Server_time():
  PATH =  '/api/v1/time'
  params = None

  timestamp= round(time.time() * 1000)

  url = urljoin(BASE_URL_API, PATH)
  r = requests.get(url, params=params)
  if r.status_code == 200:
      # Print(json.dumps(r.json(), indent=2))
      data = r.json()
      Print(f"diff={timestamp - data['serverTime']}ms")
  else:
      raise BinanceException(status_code=r.status_code, data=r.json())


def Get_Price():
  PATH = '/api/v3/ticker/price'
  params = {
      'symbol': 'BTCUSDT'
  }

  url = urljoin(BASE_URL_API, PATH)
  r = requests.get(url, headers=headers, params=params)
  if r.status_code == 200:
      Print(json.dumps(r.json(), indent=2))
  else:
      raise BinanceException(status_code=r.status_code, data=r.json())


def Get_Order_Book():
  PATH = '/api/v1/depth'
  params = {
      'symbol': 'BTCUSDT',
      'limit': 5
  }

  url = urljoin(BASE_URL_API, PATH)
  r = requests.get(url, headers=headers, params=params)
  if r.status_code == 200:
      Print(json.dumps(r.json(), indent=2))
  else:
      raise BinanceException(status_code=r.status_code, data=r.json())


def Create_Order():

  PATH = '/api/v3/order'
  timestamp= round(time.time() * 1000)
  params = {
      'symbol': 'ETHUSDT',
      'side': 'SELL',
      'type': 'LIMIT',
      'timeInForce': 'GTC',
      'quantity': 0.1,
      'price': 500.0,
      'recvWindow': 5000,
      'timestamp': timestamp
  }

  query_string = urlencode(params)
  params['signature'] = hmac.new(SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

  url = urljoin(BASE_URL_API, PATH)
  r = requests.post(url, headers=headers, params=params)
  if r.status_code == 200:
      data = r.json()
      Print(json.dumps(data, indent=2))
  else:
      raise BinanceException(status_code=r.status_code, data=r.json())

def Get_Order():
  PATH = '/api/v3/order'
  timestamp= round(time.time() * 1000)
  params = {
      'symbol': 'ETHUSDT',
      'orderId': '336683281',
      'recvWindow': 5000,
      'timestamp': timestamp
  }

  query_string = urlencode(params)
  params['signature'] = hmac.new(SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

  url = urljoin(BASE_URL_API, PATH)
  r = requests.get(url, headers=headers, params=params)
  if r.status_code == 200:
      data = r.json()
      Print(json.dumps(data, indent=2))
  else:
      raise BinanceException(status_code=r.status_code, data=r.json())


def Delete_Order():
  PATH = '/api/v3/order'
  timestamp= round(time.time() * 1000)
  params = {
      'symbol': 'ETHUSDT',
      'orderId': '336683281',
      'recvWindow': 5000,
      'timestamp': timestamp
  }

  query_string = urlencode(params)
  params['signature'] = hmac.new(SECRET_KEY.encode('utf-8'), query_string.encode('utf-8'), hashlib.sha256).hexdigest()

  url = urljoin(BASE_URL_API, PATH)
  r = requests.delete(url, headers=headers, params=params)
  if r.status_code == 200:
      data = r.json()
      Print(json.dumps(data, indent=2))
  else:
      raise BinanceException(status_code=r.status_code, data=r.json())

def _win_set_time(time_tuple):
    # import pywin32
    # http://timgolden.me.uk/pywin32-docs/win32api__SetSystemTime_meth.html
    # pywin32.SetSystemTime(year, month , dayOfWeek , day , hour , minute , second , millseconds )
    dayOfWeek = datetime.datetime(time_tuple).isocalendar()[2]
    # pywin32.SetSystemTime( time_tuple[:2] + (dayOfWeek,) + time_tuple[2:])


def _linux_set_time(time_tuple):
    import ctypes
    import ctypes.util
    import time

    # /usr/include/linux/time.h:
    #
    # define CLOCK_REALTIME                     0
    CLOCK_REALTIME = 0

    # /usr/include/time.h
    #
    # struct timespec
    #  {
    #    __time_t tv_sec;            /* Seconds.  */
    #    long int tv_nsec;           /* Nanoseconds.  */
    #  };
    class timespec(ctypes.Structure):
        _fields_ = [("tv_sec", ctypes.c_long),
                    ("tv_nsec", ctypes.c_long)]

    librt = ctypes.CDLL(ctypes.util.find_library("rt"))

    ts = timespec()
    ts.tv_sec = round( time.mktime( datetime.datetime( *time_tuple[:6]).timetuple() ) )
    ts.tv_nsec = time_tuple[6] * 1000000 # Millisecond to nanosecond

    # http://linux.die.net/man/3/clock_settime
    librt.clock_settime(CLOCK_REALTIME, ctypes.byref(ts))



def set_time(time_tuple):
  if sys.platform.startswith('linux'):
      _linux_set_time(time_tuple)
  elif  sys.platform=='win32':
      _win_set_time(time_tuple)