#!/usr/bin/python3

import string
import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    #line = line.strip()
    # split the line into words
    words = line.split(",")
    #print(words[0])
    if words[0]=="ball":                       #for every ball bowled
        words[9]=words[9].replace(" ","")
        if(words[9]=="\"\"" or words[9]=="runout") or words[9]=="retiredhurt":
            a=0				#if out,value of a is 0
        else:
            a=1
                                 
        # what we output here will be the input for the reduce step, 
        # i.e. the input for reducer.py
        mykey=words[4]+"+"+words[6]+"+"+str(a)
        print (mykey,1,sep="+")                     
