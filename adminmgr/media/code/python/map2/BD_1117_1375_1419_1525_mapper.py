#!/usr/bin/python3
import sys
c=0
for l in sys.stdin:
    l = l.strip()
    v = l.split(",")
    
    if(v[0]=="ball"):
        print('%s\t%s' % (v[6]+','+v[4],str(int(v[7])+int(v[8]))+",1"))


#print(c)

