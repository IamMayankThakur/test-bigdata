#!/usr/bin/python3


import sys
w=0
for line in sys.stdin:
        words = line.strip().split(",")
        if "ball"==words[0]:
                if words[-2]!='""' and words[-2]!='run out' and words[-2]!='retired hurt':
                        w=1
                else:
                        w=0
                print(w,words[4],words[6],1,sep="|")
        