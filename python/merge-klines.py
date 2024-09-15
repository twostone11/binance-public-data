#!/usr/bin/env python
import os
import sys
import glob
import pandas as pd
from datetime import *
from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError
from pathlib import Path




def get_parser():
  parser = ArgumentParser(description=("This is a script to process file in directory"), formatter_class=RawTextHelpFormatter)
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
  return parser

def generate_monthly_path_pattern(top_dir, type, symbol, interval):
    return os.path.join(top_dir, "data", type, "monthly", "klines", symbol,
                        interval, "{}-{}-????-??.csv".format(symbol, interval))

def generate_final_path_to_save(top_dir, type, symbol, interval):
    return os.path.join(top_dir, "data", type, "final", "klines", symbol,
                        interval, "{}-{}.csv".format(symbol, interval))

def convert_to_date_object(d):
  year, month, day = [int(x) for x in d.split('-')]
  date_obj = date(year, month, day)
  return date_obj

def get_dates_list():
    period = convert_to_date_object(datetime.today().strftime('%Y-%m-%d')) - convert_to_date_object(
        datetime.today().strftime('%Y-%m-01'))
    dates = pd.date_range(end=datetime.today(), periods=period.days + 1).to_pydatetime().tolist()
    dates = [date.strftime("%Y-%m-%d") for date in dates]
    return dates

def generate_daily_path_list(top_dir, type, symbol, interval):
    date_list = get_dates_list()
    daily_path_list = []
    for dateitem in date_list:
        daily_path_list.append(os.path.join(top_dir, "data", type, "daily", "klines", symbol,
                        interval, "{}-{}-{}.csv".format(symbol, interval, dateitem)))
    return daily_path_list


def merge_kilines_func(directory_to_scan, daily_path_list, directory_to_save):
    csv_files = glob.glob(directory_to_scan, recursive=False)
    column_names = ['open_time', 'open', 'high', 'low', 'close', 'volume', 'Close time', 'Quote asset volume',
                    'Number of trades',
                    'Taker buy base asset volume', 'Taker buy quote asset volume', 'Ignore']
    date_parser = lambda x: pd.to_datetime(x, unit='ms')
    combined_df = pd.DataFrame()
    for file in csv_files:
        df = pd.read_csv(file,
                         names=column_names,
                         header=None,
                         parse_dates=[0],
                         date_parser=date_parser)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    for daily_path in daily_path_list:
        if not os.path.exists(daily_path):
            continue
        df = pd.read_csv(daily_path,
                         names=column_names,
                         header=None,
                         parse_dates=[0],
                         date_parser=date_parser)
        combined_df = pd.concat([combined_df, df], ignore_index=True)

    combined_df.sort_values(by='open_time', inplace=True)
    combined_df.reset_index(drop=True, inplace=True)
    combined_df.ffill()
    combined_df.to_csv(directory_to_save, index=False)

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])

    directory_to_scan = generate_monthly_path_pattern(args.folder, args.type, args.symbol, args.interval)
    directory_to_save = generate_final_path_to_save(args.folder, args.type, args.symbol, args.interval)
    daily_path_list = generate_daily_path_list(args.folder, args.type, args.symbol, args.interval)

    directory = Path(os.path.dirname(directory_to_save))
    directory.mkdir(parents=True, exist_ok=True)

    merge_kilines_func(directory_to_scan, daily_path_list, directory_to_save)

