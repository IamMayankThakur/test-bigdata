#!/usr/bin/python3
"""reducer.py"""

from operator import itemgetter
import sys
import operator

current_word = None
current_count = 0
current_run = 0
word = None
mydict={}


for line in sys.stdin:
    
    line = line.strip()
    words=line.split("+")
    words[0]=words[0].replace("(\'","")
    words[3]=words[3].replace("(\'","")
    words[3]=words[3].replace(")\n","")   
    words[2]=words[2].replace("\',","")


    
    try:
        words[3] = int(words[3])
        words[2] = int(words[2])
	
    except ValueError:
        continue

   
    
    if current_word == (words[0])+":"+(words[1]):
        current_count += words[3]
        current_run += words[2] 
        
    else:
        if current_run and current_count>9:
            
            run_rate=(current_run*100)/current_count
            a=current_word.split(":")
            if(a[0] in mydict.keys()):
                mydict[a[0]].append( [a[1],int(run_rate),current_run] )
            else:
                mydict[a[0]]=[ [a[1],int(run_rate),current_run] ]
             
        current_count = words[3]
        current_run = words[2]     
        current_word = words[0]+":"+words[1]



list=[]

for key in mydict.keys():
    
    b = sorted(mydict[key], key = lambda x: (-x[1], -x[2]))
    
    d = b[0]
    list.append([key,d[0]])

final = sorted(list, key = lambda x: (x[0]))

for value in final:
        li=",".join(value)
        print(li)


