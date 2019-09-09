#!/usr/bin/python3
import sys


venue = None


for l in sys.stdin:
    l = l.strip()
    v = l.split(",")
    
    if(v[0]=="ball" and venue):
        if(int(v[8])==0):
            print('%s\t%s' % (venue+','+v[4],str(int(v[7]))+",1"))
    if(v[0]=="info"):
        if(v[1]=="venue"):
            venue=v[2]
            if(len(v)>3):
                venue = venue + "," + v[3]
                #print(venue)


#print(c)

