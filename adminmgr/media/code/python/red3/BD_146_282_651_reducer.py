#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
from signal import signal, SIGPIPE, SIG_DFL
signal(SIGPIPE,SIG_DFL) 
dic={}
ven=''
pl=''
Finlist=[]
for line in sys.stdin:
    line = line.strip()
    my_list = line.split("\t")
    #print(my_list)
    ven=my_list[0]
    pl=my_list[1]
    
    if ven in dic:
        if pl in dic[ven]:#Updating my balls and runs
            dic[ven][pl][0]=dic[ven][pl][0]+int(my_list[2])#Runs
            dic[ven][pl][1]=dic[ven][pl][1]+int(my_list[3])#Balls
        else:#If the player is not present in the dictionary
            dic[ven][pl]=[0,1]
            dic[ven][pl][0]=dic[ven][pl][0]+int(my_list[2])
    else:#If we dont have an entry at all
        dic[ven]={}
        dic[ven][pl]=[0,1]
        dic[ven][pl][0]=dic[ven][pl][0]+int(my_list[2])

#Calculating Strike Rate for each player and appending
for ven in dic:
    for pl in dic[ven]:
        if(dic[ven][pl][1]>=10):#Played minimum 10 balls
            strike_rate=(dic[ven][pl][0]*100)/dic[ven][pl][1]
        else:
            strike_rate=0
        dic[ven][pl]=strike_rate

#print(dic)
#print(dic[ven][pl][2])

for ven in dic:
    dic_sort=sorted(dic[ven].items(),key=itemgetter(1),reverse=True)
    Finlist.append((ven,dic_sort[0][0]))
resultList=sorted(Finlist,key=itemgetter(0))#Sorting based on venue

for i in resultList:
    print('%s,%s'%(i[0],i[1]))

#print(res)
#print('%s,%s'%(ven,dic_sort[0][0]))
#print(dic_sort)
#print(dic1)
