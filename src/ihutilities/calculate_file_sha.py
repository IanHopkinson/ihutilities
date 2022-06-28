#!/usr/bin/env python
# encoding: utf-8

import sys
from ihutilities import calculate_file_sha

if __name__ == "__main__":
    arg = sys.argv[1:]
    if len(arg) == 0:
        print("Available commandlines:")
        print("calculate_file_sha.py [filepath]")
        sys.exit()
    elif len(arg) == 1:
        filepath = arg[0]

    print(calculate_file_sha(filepath))
