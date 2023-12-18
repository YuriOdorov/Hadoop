#! /usr/bin/env python

import sys

current_key = None
current_word = None
word_sum = 0

for line in sys.stdin:
    try:
        key, word, count = line.split('\t', 2)
        count = int(count)
    except ValueError as e:
        continue
    if current_key != key:
        if current_key:
            print(f'{current_key} {len(current_word)}\t{word_sum}')
        word_sum = 0
        current_key = key
        current_word = set()
    word_sum += count
    current_word.add(word)

if current_key:
    print(f'{current_key} {len(current_word)}\t{word_sum}')
