#! /usr/bin/env python3

import sys


for line in sys.stdin:
    try:
        key, count = line.split('\t')
        count = 10**10 - int(count)
    except ValueError as e:
        continue
    print(f'{count}\t{key}')


