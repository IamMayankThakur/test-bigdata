#!/usr/bin/python3
from operator import itemgetter
import sys
import operator

current_word = "a"
current_bat = None
current_count = 0
count_wickets = 0
word = None
mylist=[]


# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # parse the input we got from mapper.py
    words=line.split("+")     #words contains batsman,bowler,out/not_out and value 
    #words[0]=words[0].replace("(\'","")
    #words[3]=words[3].replace(")\n","")
    #words[2]=words[2].replace("\',","")

    # convert words[3] i.e, the value (currently a string) to int
    try:
        words[3] = int(words[3])
        words[2]= int(words[2])
    except ValueError:
        continue
    
    if current_word == (words[0]+":"+words[1]):
        current_count += words[3]
        count_wickets += words[2]
    else:
        if current_word and current_count>5:
            a=current_word.split(":")
            mylist.append( [a[0],a[1],count_wickets,current_count] )
        current_count = words[3]
        count_wickets = words[2]
        current_word = words[0]+":"+words[1]
    # do not forget to output the last word if needed!
#if current_word == prev_pair and curren_count>5:
 #   mylist.append( [words[0],words[1],count_wickets,current_count] )

#sorting the output in desending order of wickets, ascending order of deliveries and 
# then alphabetically        
final = sorted(mylist, key = lambda x: (-x[2], x[3], x[0]))
for value in final:
        print(value[0]+","+value[1]+",",value[2],",",value[3],sep="")
    

