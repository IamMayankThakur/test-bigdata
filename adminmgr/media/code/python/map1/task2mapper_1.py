import sys
infile=sys.stdin
for line in infile:
    mylist=line.split(",")
    if(len(mylist)>9):
	if(mylist[9]!='run out' and mylist[9]!='""' and mylist[9]!=)
	    print('%s,%s,%d,%d'%(mylist[4],mylist[6],1,1))
	else:
	    print('%s,%s,%d,%d'%(mylist[4],mylist[6],0,1))