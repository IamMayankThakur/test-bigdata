#!/usr/bin/python3
import sys
c=0
for l in sys.stdin:
    l = l.strip()
    v = l.split(",")
    
    if(v[0]=="ball"):
        if(v[4]+','+v[6] == 'MS Dhoni,PP Ojha'):
            c+=1
        if(len(v[9])!=2 and v[9]!="run out" and v[9]!="retired hurt"):
            print('%s\t%s' % (v[4]+','+v[6],"1,1"))
        else:
            print('%s\t%s' % (v[4]+','+v[6],"0,1"))


#print(c)

