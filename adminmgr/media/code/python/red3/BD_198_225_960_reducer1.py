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

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    words=line.split("+")
    words[0]=words[0].replace("(\'","")
    words[3]=words[3].replace("(\'","")
    words[3]=words[3].replace(")\n","")   
    words[2]=words[2].replace("\',","")


    # convert count (currently a string) to int
    try:
        words[3] = int(words[3])
        words[2] = int(words[2])
	#print(words[2])
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    # this IF-switch only works because Hadoop sorts map output
    # by key (here: word) before it is passed to the reducer
    if current_word == (words[0])+":"+(words[1]):
        current_count += words[3]
        current_run += words[2] 
    else:
        if current_run and current_count>10:
            
            run_rate=(current_run*100)/current_count
            a=current_word.split(":")
            if(a[0] in mydict.keys()):
                mydict[a[0]].append( (a[1],run_rate,current_run) )
            else:
                mydict[a[0]]=[ (a[1],run_rate,current_run) ]
             #just want to create a dictionary for each stadium with key as batsman and run rate as value and after on stadium is done clear dict and start loop again
  
        current_count = words[3]
        current_run = words[2]     
        current_word = words[0]+":"+words[1]
list=[]
for key in mydict:
    b = sorted(mydict[key], key = lambda x: (-x[1], -x[2]))
    d = b[0]
    list.append([key,d[0]])
#final = sorted(list, key = lambda x: (x[0]))
for value in list:
        li=",".join(value)
        print(li)

# do not forget to output the last word if needed!
#if current_word == (words[0]+words[1]):
 #   print (words[0]+","+words[1]+","+count_wickets+","+current_count)
