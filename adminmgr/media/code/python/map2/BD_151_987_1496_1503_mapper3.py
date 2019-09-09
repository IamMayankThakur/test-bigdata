#!/usr/bin/python3


import sys
d={}
# input comes from STDIN (standard input)
for line in sys.stdin:
        # remove leading and trailing whitespace
        words  = line.strip().split(",")
        # split the line into words
        if "ball"==words[0]:
                print(int(words[7])+int(words[8]),words[4],words[6],1,sep="|")
