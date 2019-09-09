#!/usr/bin/python3
'''my_cols=['info','ball','innings','batting_team','batsman1','batsman2','bowler','batsman1_score','extras','wicket','batsman_out']
data = pd.read_csv("alldata.csv",names=my_cols, skipinitialspace=True, engine='python')'''
import sys
dic={}
li=[]
for line in sys.stdin:
	li.append(line)
	row=list(map(str,line.split(",")))
	#print(row)
	if(row[1]=="venue"):
		#if(row[2][0]=="\""):
			#venue=row[2][1:]
		print(row[2], row[2][-1])

		if(len(row)!=4):
			
			if(row[2][-1]=="\n"):
				venue=row[2][:-1]
			else:
				venue=row[2]
		else:
			venue_t=row[2]
			if(row[3][-1]=="\n"):
				venue=venue_t+","+row[3][:-1]
			else:
				venue=venue_t+","+row[3]
			
	if(row[0]=="ball"):
		if (venue,row[4]) not in dic:
			dic[(venue,row[4])]=1
		else:
			dic[(venue,row[4])]+=1

for line in li:
	row=list(map(str,line.split(",")))
	if(row[1]=="venue"):
		#if(row[2][0]=="\""):
			#venue=row[2][1:]
		if(len(row)!=4):
			if(row[2][-1]=="\n"):
				venue=row[2][:-1]
			else:
				venue=row[2]
		else:
			venue_t=row[2]
			if(row[3][-1]=="\n"):
				venue=venue_t+","+row[3][:-1]
			else:
				venue=venue_t+","+row[3]
	if(row[0]=="ball"):
		if(dic[(venue,row[4])]>=10 and row[8]=="0"):
			print(venue,row[4],row[7],sep="|")