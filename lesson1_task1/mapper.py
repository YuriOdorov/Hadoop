#! /usr/bin/env python 3

import sys
import random

for line in sys.stdin:
    try:
        ident = str(random.randint(1, 9)) + line.strip()
    except ValueError as e:
        continue
    print(ident, 1)
