#! /usr/bin/env python

import sys
import random

counter = random.randint(1, 5)
idents = []

for line in sys.stdin:
    try:
        idents += [line.split()[0][1:]]
        counter -= 1
        if not counter:
            print(','.join(idents))
            counter = random.randint(1, 5)
            idents = []
    except ValueError as e:
        continue

if idents:
    print(','.join(idents))
