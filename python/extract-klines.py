#!/usr/bin/env python

import os
import zipfile
import os, sys, re, shutil
import json
from pathlib import Path
from datetime import *
import urllib.request
from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError
from enums import *

from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError

def get_parser():
  parser = ArgumentParser(description=("This is a script to extrate zip file in directory"), formatter_class=RawTextHelpFormatter)
  parser.add_argument(
      '-s', dest='symbol', default='BTCUSDT',
      help='Single symbol or multiple symbols separated by space')

  parser.add_argument(
      '-folder', dest='folder', default="D:\\open_source\\backtrader\\datas\\binance",
      help='Directory to store the downloaded data')

  parser.add_argument(
      '-t', dest='type', required=True, choices=TRADING_TYPE,
      help='Valid trading types: {}'.format(TRADING_TYPE))

  parser.add_argument(
      '-i', dest='interval', default='4h', choices=INTERVALS,
      help='single kline interval or multiple intervals separated by space\n-i 1m 1w means to download klines interval of 1minute and 1week')
  return parser

def get_path(trading_type, time_period, symbol, interval):
  trading_type_path = 'data/spot'
  if trading_type != 'spot':
    trading_type_path = f'data/futures/{trading_type}'
  if interval is not None:
    path = f'{trading_type_path}/{time_period}/klines/{symbol}/{interval}/'
  else:
    path = f'{trading_type_path}/{time_period}/klines/{symbol}/'
  return path


def extract_directory(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.zip'):
            zip_file_path = os.path.join(directory, filename)
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(directory)
            os.remove(zip_file_path)

if __name__ == "__main__":
    parser = get_parser()
    args = parser.parse_args(sys.argv[1:])

    sub_path = get_path(args.type, "monthly", args.symbol, args.interval)
    dest_path = os.path.join(args.folder, sub_path)
    print("extract path {}".format(dest_path))
    extract_directory(dest_path)
    sub_path = get_path(args.type, "daily", args.symbol, args.interval)
    dest_path = os.path.join(args.folder, sub_path)
    print("extract path {}".format(dest_path))
    extract_directory(dest_path)
