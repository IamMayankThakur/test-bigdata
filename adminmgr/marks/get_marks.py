import pandas as pd
import numpy as np


a1 = pd.read_csv('a1.csv')
a2 = pd.read_csv('a2.csv')
a3 = pd.read_csv('a3.csv')
final = pd.read_csv('final.csv')

for index, row in final.iterrows():
    usn = row['USN']
    try:
        m_a1 = a1.loc[a1['USN'] == usn]
        m1 = float(m_a1['Task1']) + float(m_a1['Task2']) + \
            float(m_a1['Task3']) + float(m_a1['Task4']) + float(m_a1['Viva'])
    except:
        m1 = 0
    try:
        m_a2 = a2.loc[a2['USN'] == usn]
        m2 = float(m_a2['Task1']) + float(m_a2['Task2']) + float(m_a2['Viva'])
    except:
        m2 = 0
    try:
        m_a3 = a3.loc[a3['USN'] == usn]
        m3 = float(m_a3['Task1']) + float(m_a3['Task2']) + float(m_a3['Viva'])
    except:
        m3 = 0
    # print(final.head())
    final.at[index, 'A - 1 (5)'] = m1
    final.at[index, 'A - 2 (5)'] = m2
    final.at[index, 'A - 3 (5)'] = m3
    # print(final.head())

final.to_csv('final_marks.csv', index=False)
