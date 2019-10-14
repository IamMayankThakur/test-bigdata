from __future__ import print_function

import re
import sys
from operator import add
from pyspark import SparkContext
from pyspark.sql import SparkSession

def wicks(line):
	#line = line.rstrip()
	wick_info = re.split(r",",line)
	return wick_info[0],wick_info[1]
	
def deli(line):
	#line = line.rstrip()
	del_info = re.split(r",",line)
	b_avg = int(del_info[2])/int(del_info[3])
	if (b_avg>1.0):
		return del_info[0],b_avg
	return del_info[0],1.0

def name(x):
	parts = re.split(r",",x)
	return parts[1]
	
def prowess(w,d):
	bat = len(w)
	for b in w:
		yield (b, float(d / bat))
		
if __name__=="__main__":
	if (len(sys.argv) != 4):
		print("Usage: playerrank <file> <iterations> <weight>",file=sys.stderr)
		sys.exit(-1)
		
	spark = SparkSession\
			.builder\
			.appName("BowlerRating")\
			.getOrCreate()
			
	lines = spark.read.text(sys.argv[1]).rdd.map(lambda r:r[0])
	
	wickets = lines.map(lambda wick : wicks(wick)).distinct().groupByKey().cache()
	#wickets = w.reduce(add)
	#w = wickets.reduceByKey(add)
	deliveries = lines.map(lambda x: deli(x)).reduceByKey(add)
	
	
	#for (bat,rank) in deliveries.collect():
	#	print("%s %s"%(bat,rank))
		
	#spark.stop()
							
	
	if (int(sys.argv[3])==0):
		weight = 0.80
	else:
		weight = float(sys.argv[3])/100
	
	if (int(sys.argv[2])==0):
		ite = 0
		val = False
		data = []
		bowl_name=lines.map(lambda x: name(x)).distinct()
		for i in bowl_name.collect():
			data.append([i,1.0])
		while (not val):
			temp_prowess = wickets.join(deliveries).flatMap(lambda p: prowess(p[1][0],p[1][1]))
			deliveries = temp_prowess.reduceByKey(add).mapValues(lambda x: x * weight + (1-weight))
			temp = []			
			i = 0
			val = True
			for (bowl,rank) in deliveries.collect():
				r = [i for i in data if bowl in i]
				#print(r)
				
				if ((rank-r[0][1]>=0.0001) or (r[0][1]-rank>=0.0001)):
					val = False
				temp.append([bowl,rank])
			if (val == True):
				break
			data = temp
			#print(ite)
			ite = ite+1
			
	else:
		i =0
		ite = int(sys.argv[2])
		for i in range(ite):
			temp_prowess = wickets.join(deliveries).flatMap(lambda p: prowess(p[1][0],p[1][1]))
			deliveries = temp_prowess.reduceByKey(add).mapValues(lambda x: x * weight + (1-weight))

	deliveries = deliveries.sortBy(lambda a: a[0])
	deliveries = deliveries.sortBy(lambda a: -a[1])
	#print(type(deliveries))
	for (bowl,rank) in deliveries.collect():
		print("%s,%.12f"%(bowl,float(rank)))
	#print(ite)

	spark.stop()			
				
						
	
	
	
