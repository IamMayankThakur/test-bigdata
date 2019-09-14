#!/usr/bin/python3
import sys
st=""
x = sys.stdin
for data in x:
    l=data.strip()
    mylist=l.split(',')
    if(len(list)<7):
        if(list[1]=='venue'):
            if(len(list)==3 and list[1]=='venue'):
                st=list[2]
            if(len(list)==4 and list[1]=='venue'):
                st=str(str(list[2])+","+str(list[3]))
    else:
        if(list[8]=='0'):
            j=int(list[7])
            print(st,list[4],j,1,sep='-')