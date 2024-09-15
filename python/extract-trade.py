#!/usr/bin/env python

import os
import zipfile
import sys

from argparse import ArgumentParser, RawTextHelpFormatter, ArgumentTypeError

def get_parser(parser_type):
  parser = ArgumentParser(description=("This is a script to extrate zip file in directory"), formatter_class=RawTextHelpFormatter)
  parser.add_argument(
      '-f', dest='folder',
      default='D:\\open_source\\backtrader\\datas\\binance\\data\\spot\\daily\\klines\\BTCUSDT\\4h',
      help='directory to extract zip file',)
  return parser


def extract_directory(directory):
    for filename in os.listdir(directory):
        if filename.endswith('.zip'):
            zip_file_path = os.path.join(directory, filename)
            with zipfile.ZipFile(zip_file_path, 'r') as zip_ref:
                zip_ref.extractall(directory)
            os.remove(zip_file_path)

if __name__ == "__main__":
    parser = get_parser('trades')
    args = parser.parse_args(sys.argv[1:])
    extract_directory(args.folder)
    extract_directory("D:\\open_source\\backtrader\\datas\\binance\\data\\spot\\monthly\\klines\\BTCUSDT\\4h")
