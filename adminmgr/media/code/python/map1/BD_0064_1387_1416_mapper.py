#!/usr/bin/python3
import csv 
import sys
#sys.stdout=open('op.txt','w')
# csv file name 
#filename = "alldata.csv"
  
# initializing the titles and rows list 
fields = [] 
rows = [] 
  
# reading csv file 
#with open(filename, 'r') as csvfile: 
    # creating a csv reader object 
	#csvreader = csv.reader(csvfile) 
      
    # extracting field names through first row 
	#fields = next(csvreader) 
  
    # extracting each data row one by one 
	#for row in csvreader: 
		#rows.append(row) 
		
  
    # get total number of rows 
	#print("Total no. of rows: %d"%(csvreader.line_num)) 
  
# printing the field names 
#print('Field names are:' + ', '.join(field for field in fields)) 
  
#  printing first 5 rows 

for row in sys.stdin:
	row=row.strip()
	row1=list(row.split(","))
	#print(row1[0])
	rows.append(row1)

'''for row in rows[20:30]:
	print(row)'''
task1={}
result={}

#177391
#print(rows[220])
for row in rows:
	#print(row[0])
	if row[0]=="ball":
		a=row[4]
		b=row[6]
		if(a,b) not in task1:
			task1[(a,b)]=[0]*2
			task1[(a,b)][1]+=1
			if row[9]!='""' and row[10]!='""' and row[9]!="run out" and row[9]!="retired hurt":
				task1[(a,b)][0]=1
		else:
			task1[(a,b)][1]+=1
			if row[9]!='""' and row[10]!='""' and row[9]!="run out" and row[9]!="retired hurt":
				task1[(a,b)][0]+=1
#print(task1)


for v in task1:
	print(v[0],end=",")
	print(v[1],end=":")
	print(task1[v][0],end=",")
	print(task1[v][1])

