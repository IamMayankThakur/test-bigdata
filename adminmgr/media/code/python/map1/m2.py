##!/usr/bin/python3
import sys

for line in sys.stdin:
    rec=line.strip()
    fie=rec.split(",")
    if(fie[0]=="ball"):
        m_o=(fie[6],fie[4])+(int(fie[7])+int(fie[8]),1)
        print(m_o)
