import csv
import sys

for line in sys.stdin:
    line = line.strip()
    line = line.split(',')

    try:

        batsman=line[4]
        bowler=line[6]
        if(line[9] == '""' or line[9] == "retired hurt" or line[9] == "run out"):
            print(str([line[4], line[6], 0, 1]))
            #print('%s\t%s\t%d\t%d'% (batsman,bowler,0,1))
        else:
            #print('%s\t%s\t%d\t%d'% (batsman,bowler,1,1))
            print(str([line[4], line[6], 1, 1]))

    except IndexError:
        continue


