#!/usr/bin/env python
import os
import sys
import glob
import pandas as pd
from datetime import *
from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError
from pathlib import Path
import zipfile

from enums import *
from utility import download_file, get_all_symbols, get_parser, get_start_end_date_objects, convert_to_date_object, \
  get_path

def download_daily_klines(trading_type, symbols, num_symbols, intervals, dates, start_date, end_date, folder, checksum):
  current = 0
  date_range = None

  if start_date and end_date:
    date_range = start_date + " " + end_date

  if not start_date:
    start_date = START_DATE
  else:
    start_date = convert_to_date_object(start_date)

  if not end_date:
    end_date = END_DATE
  else:
    end_date = convert_to_date_object(end_date)

  #Get valid intervals for daily
  intervals = list(set(intervals) & set(DAILY_INTERVALS))
  print("Found {} symbols".format(num_symbols))

  for symbol in symbols:
    print("[{}/{}] - start download daily {} klines ".format(current+1, num_symbols, symbol))
    for interval in intervals:
      for date in dates:
        current_date = convert_to_date_object(date)
        if current_date >= start_date and current_date <= end_date:
          path = get_path(trading_type, "klines", "daily", symbol, interval)
          file_name = "{}-{}-{}.zip".format(symbol.upper(), interval, date)
          download_file(path, file_name, date_range, folder)

          if checksum == 1:
            checksum_path = get_path(trading_type, "klines", "daily", symbol, interval)
            checksum_file_name = "{}-{}-{}.zip.CHECKSUM".format(symbol.upper(), interval, date)
            download_file(checksum_path, checksum_file_name, date_range, folder)

    current += 1



def get_parser():
  parser = ArgumentParser(description=("This is a script to update daily to final"), formatter_class=RawTextHelpFormatter)
  parser.add_argument(
      '-f', dest='folder',
      default='D:\\open_source\\backtrader\\datas\\binance',
      help='directory to extract zip file',)
  parser.add_argument(
      '-t', dest='type',
      default='spot',
      help='data type , spot um cm', )
  parser.add_argument(
      '-s', dest='symbol',
      default='BTCUSDT',
      help='BTCUSDT ETHUSDT', )
  parser.add_argument(
      '-i', dest='interval',
      default='4h',
      help='1m 5m 1h 4h', )
  parser.add_argument(
      '-d', dest='whichdate',
      default=None,
      help='update which date', )
  return parser

def generate_daily_path(top_dir, type, symbol, interval):
    return os.path.join(top_dir, "data", type, "final", "klines", symbol,
                        interval)

def generate_final_path_to_update(top_dir, type, symbol, interval):
    return os.path.join(top_dir, "data", type, "final", "klines", symbol,
                        interval, "{}-{}.csv".format(symbol, interval))

def convert_to_milliseconds(timestamp_str):
    dt = datetime.strptime(timestamp_str, '%Y-%m-%d %H:%M:%S')
    return int(dt.timestamp() * 1000)

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])

    # 下载文件
    download_date = None
    if not args.whichdate:
        download_date = datetime.date(datetime.now()).strftime('%Y-%m-%d')
    else:
        download_date = args.whichdate

    download_daily_klines(trading_type=args.type,
                          symbols=[args.symbol],
                          num_symbols=1,
                          intervals=[args.interval],
                          dates=[download_date],
                          start_date=None,
                          end_date=None,
                          folder= args.folder,
                          checksum=False)

    #解压缩文件
    daily_path_dir = os.path.join(args.folder, "data", args.type, "daily", "klines", args.symbol, args.interval)
    file_name_zip = "{}-{}-{}.zip".format(args.symbol, args.interval, download_date)
    file_name_csv = "{}-{}-{}.csv".format(args.symbol, args.interval, download_date)
    file_name_tmp_csv = "{}-{}-{}-temp.csv".format(args.symbol, args.interval, download_date)
    zip_file_path = os.path.join(daily_path_dir, file_name_zip)
    csv_file_path = os.path.join(daily_path_dir, file_name_csv)
    csv_file_temp_path = os.path.join(daily_path_dir, file_name_tmp_csv)

    if not os.path.exists(zip_file_path):
        print("today csv file not exists, skip update".format(zip_file_path))
        exit(0)

    with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
        zip_ref.extractall(daily_path_dir)
    os.remove(zip_file_path)

    column_names = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'Close time', 'Quote asset volume',
                    'Number of trades',
                    'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
    date_parser = lambda x: pd.to_datetime(x, unit='ms')

    df_daily = pd.read_csv(csv_file_path ,
                            names=column_names,
                            header=None,
                            parse_dates=[0],
                            date_parser=date_parser)

    df_daily.sort_values(by='open_time', inplace=True)
    df_daily.reset_index(drop=True, inplace=True)
    df_daily.ffill()
    df_daily.to_csv(csv_file_temp_path, index=False)

    final_file_path = generate_final_path_to_update(args.folder, args.type, args.symbol, args.interval)
    df_final = pd.read_csv(final_file_path, dtype={'open_time': str})
    df_daily_new = pd.read_csv(csv_file_temp_path, dtype={'open_time': str})

    combined_df = pd.concat([df_final, df_daily_new], ignore_index=True)
    combined_df['open_time'] = pd.to_datetime(combined_df['open_time'])
    sorted_df = combined_df.sort_values(by='open_time')
    unique_df = sorted_df.drop_duplicates()
    unique_df.to_csv(final_file_path, index=False)

    os.remove(csv_file_temp_path)








