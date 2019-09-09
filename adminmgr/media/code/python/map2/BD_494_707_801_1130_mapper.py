#!/usr/bin/python3
import sys
import csv

#Word Count Example
# input comes from standard input STDIN
for line in sys.stdin:
    line = line.strip() #remove leading and trailing whitespaces
    words = line.split(',') #split the line into words and returns as a list
    if(words[0]=="ball"):
        key=words[6]+","+words[4]
        #key1=words[7]+","+words[8]
        #runs1=int(words[7])+int(words[8])
        #runs=str(runs1)
        #print(key,runs)
        #runs=words[7]+","+words[8]
        print('%s    %s    %s' % (key,words[7],words[8]))
#print('%s    %s    %s' % (key,words[7],words[8]))
