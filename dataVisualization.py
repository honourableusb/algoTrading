# dataVisualization.py
# graphs for our data, most basic going up to (hopefully) showing different purchases with algorithm

# get ticker that we want to get info on
import matplotlib
import matplotlib.pyplot as pl
import sys
import os
import pandas
import pyodbc
import numpy as np

ticker = 'GS'

def plot(x, y, ticker='GS'):
    # print('plot')
    pl.plot(x,y, color='blue', linestyle='solid', linewidth=2)
    # pl.locator_params(axis='x', nbins=12)
    pl.xticks(np.arange(0, len(x)+1, 251))

    # ax.xaxis.set_major_locator(pl.MaxNLocator(12))
    delta = max(y) * .1
    pl.ylim(min(y) - delta, max(y) + delta)
    pl.xlim(min(x),max(x))
    pl.xlabel('Date')
    pl.ylabel('Value (USD)')
    title = ticker + ' cost history'
    # print(title)
    pl.title(title)
    pl.show()

def main():
    # print('main')
    # ticker = sys.argv[1]
    connectionString = 'Driver={SQL Server};Server=DESKTOP-MAH;Database=stockInfo;Trusted_Connection=True;'
    conn = pyodbc.connect(connectionString)
    # cursor = conn.cursor()

    selectionQuery = 'select recordDate, dayAdjClose from records where Ticker=\'' + ticker + '\' order by recordDate asc'
    xandy = pandas.read_sql_query(selectionQuery, conn)
    # print(xandy['recordDate'].tolist())
    x = xandy['recordDate'].tolist()
    y = xandy['dayAdjClose'].tolist()    
    plot(x,y)

main()