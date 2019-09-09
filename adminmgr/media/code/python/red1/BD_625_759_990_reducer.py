
#!/usr/bin/python3
import csv
from operator import itemgetter
import sys
import ast
Dict_ = {}
List_ = []

for line in sys.stdin:
    res = ast.literal_eval(line)
    tup = (res[0], res[1])                       #(res[0] res[1])  (batsman bowler)
    if tup in Dict_:
        Dict_[tup][0]= Dict_[tup][0]+res[2]      #res[2] wicket 
        Dict_[tup][1]= Dict_[tup][1]+res[3]      #res[3] delivery
                                    		
    else:
        Dict_[tup] = [res[2], res[3]]



for x in Dict_.items():
     List_.append([x[0][0], x[0][1], -1 * x[1][0], x[1][1]])


List_wicket = sorted(List_, key=itemgetter(2,3,0))   #(wicket,deli,batsman)

for j in List_wicket:
     j[2] *=(-1)

for i in List_wicket:
    if(i[3] > 5):
        print('%s,%s,%s,%s'%(i[0],i[1],i[2],i[3]))



'''for i in list_:
    i[2] *= (-1)'''
