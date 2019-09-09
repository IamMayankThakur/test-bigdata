#!/usr/bin/python3

import sys
import operator

result = dict()
for elements in sys.stdin.readlines():
    values = elements.strip().split('|')
    if values[0] not in result:
        result[values[0]] = {values[1]: [int(values[2]), int(values[3])]}
    elif values[0] in result and values[1] not in result[values[0]]:
        result[values[0]][values[1]] = [int(values[2]), int(values[3])]
    else:
        result[values[0]][values[1]][0] += int(values[2])
        result[values[0]][values[1]][1] += int(values[3])

modResult = dict()
for elements in result:
    if elements not in modResult:
        modResult[elements] = {}
    for player in result[elements]:
        if result[elements][player][1] >= 10:
            modResult[elements][player] = result[elements][player]


for elements in modResult:
    for players in modResult[elements]:
        player = modResult[elements][players]
        modResult[elements][players] = [int(player[0] * 100 / player[1]), player[0]]

for elements in modResult:
    modResult[elements] = sorted(modResult[elements].items(),
                                 key=operator.itemgetter(1), reverse=True)

modResult = sorted(modResult.items())


for elements in modResult:
    for players in elements:
        if isinstance(players, list):
            print(elements[0], ',', players[0][0], sep='')
