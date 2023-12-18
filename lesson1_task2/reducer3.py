#! /usr/bin/env python3

import sys

for line in sys.stdin:
    try:
        count, key, cou = line.strip().split()
        count = 10**10 - int(count)
    except ValueError as e:
        continue
    print(key, count, cou)


