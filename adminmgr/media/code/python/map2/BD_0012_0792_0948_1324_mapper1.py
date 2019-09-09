#!/usr/bin/python3
import sys

for string in sys.stdin:
	string = string.strip()
	string = string.split(",")
	try:
		print([string[6],string[4], int(string[7]) + int(string[8]), 1])
	except IndexError:
		pass
