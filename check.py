#!/usr/bin/env python3
# coding: utf8

"""

"""


import os
import sys
import h5py as h5
import subprocess
import re
import argparse
from collections import defaultdict
import pandas as pd
import multiprocessing as mp

class CustomFormatter(argparse.RawDescriptionHelpFormatter,
                      argparse.ArgumentDefaultsHelpFormatter):
    pass

def parse_cmdline_args():
    parser = argparse.ArgumentParser(
        description=sys.modules[__name__].__doc__,
        formatter_class=CustomFormatter)
    
    parser.add_argument('-f', '--f', type=str, help="Input the first stream file")
    parser.add_argument('-s','--s', type=str, help="Input the second stream file")  
    return parser.parse_args()

def parsing_stream(input_stream):
    dd_1 = defaultdict(list)
    
    with open(input_stream, 'r') as stream:
        reading_chunk = False
        found_pattern = False
        for line in stream:
            if line.strip() == '----- Begin chunk -----':
                reading_chunk = True
            elif line.startswith('Image filename:'):
                name_of_file = line.strip().split()[-1]
                found_pattern = True
            elif line.startswith('Event:'):
                
                event = line.split(':')[-1].strip()  
            elif line.startswith('indexed_by'):
                indexed_by = line.split(' = ')[-1]
            elif line.strip() == '----- End chunk -----':
                reading_chunk = False
                if found_pattern and 'none' not in indexed_by:
                    dd_1['filename'].append(name_of_file)
                    dd_1['event'].append(event)
                found_pattern = False
            elif reading_chunk:
                pass
            else:
                pass
    return dd_1

if __name__ == "__main__":
    args = parse_cmdline_args()
    first_stream_file = args.f
    second_stream_file = args.s
    
    if os.path.exists(first_stream_file) and os.path.exists(second_stream_file):
        pool = mp.Pool(mp.cpu_count())
        results = pool.map(parsing_stream, [first_stream_file, second_stream_file])
        pool.close()
        
        dd_1, dd_2 = results
        
        df = pd.DataFrame(dd_1)
        df2 = pd.DataFrame(dd_2)
        
        if len(df) == 0:
            print(f'{os.path.basename(first_stream_file)} has no indexed patterns')
        elif len(df2) == 0:
            print(f'{os.path.basename(second_stream_file)} has no indexed patterns')
        else:    
            mergedStuff = pd.merge(df, df2, on=['filename', 'event'], how='inner', suffixes=('_fisrt', '_second')) #intersection
            print(f'The total intersection between {os.path.basename(first_stream_file).split(".")[0]} and {os.path.basename(second_stream_file).split(".")[0]} in % is {round(mergedStuff.shape[0] * 100 / max(df.shape[0], df2.shape[0]), 2)} over maximum and {round(mergedStuff.shape[0] * 100 / min(df.shape[0], df2.shape[0]), 2)} over minimum')
    else:
        if not os.path.exists(first_stream_file):
            print(f'Not exists: {first_stream_file}')
        else:
            print(f'Not exists: {second_stream_file}')