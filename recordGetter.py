# Record Getter
# Gets Records (wow!)
# Gets 5 years of stock history for inserted argument stock ticker.
# exports data to my SQL instance to use for backtesting when needed.

# Things needed - pyodbc and pandas
import pyodbc
import pandas
import os
import datetime
from datetime import date
from dateutil.relativedelta import relativedelta
import webbrowser
import time
import sys

downloadPath = 'C:/Users/Matt/Downloads/'
# ticker = 'APLE'

def get5years(ticker):

    today = date.today()
    fiveyears = date.today() - relativedelta(years=5)
    startEpoch = (datetime.datetime(fiveyears.year, fiveyears.month, fiveyears.day,0,0) - datetime.datetime(1970,1,1)).total_seconds()
    endEpoch = (datetime.datetime(today.year, today.month, today.day,0,0) - datetime.datetime(1970,1,1)).total_seconds()
    # Download a file containing the past 5 years of transaction history.
    downloadQuery = 'https://query1.finance.yahoo.com/v7/finance/download/'+ ticker +'?period1=' + str(int(startEpoch)) + '&period2=' + str(int(endEpoch)) + '&interval=1d&events=history&includeAdjustedClose=true'
    print("No data found. Attempting to download data...", end= ' ')
    webbrowser.open(downloadQuery)
    print('Success')
    # Open the file that we downloaded
    time.sleep(2)
    filepath = downloadPath + ticker + '.csv'
    history = pandas.read_csv(filepath)
    tickerList = []
    for i in range(history['Date'].count()):
        tickerList.append(ticker)
    history.insert(0,'Ticker', tickerList)
    # Delete the file
    os.remove(filepath)
    return history

def getUpdate(ticker, conn):
    
    today = date.today()
    sortedQuery = 'select * from records where Ticker=\'' + ticker + '\' order by recordDate desc'
    df = pandas.read_sql_query(sortedQuery, conn)
    mostRecentDate = df['recordDate'][0]
    # print(mostRecentDate)
    mostRecentDate = mostRecentDate.split('-')
    # print(mostRecentDate)
    startEpoch = (datetime.datetime(int(mostRecentDate[0]), int(mostRecentDate[1]), int(mostRecentDate[2]),0,0) - datetime.datetime(1970,1,1)).total_seconds()
    endEpoch = (datetime.datetime(today.year, today.month, today.day,0,0) - datetime.datetime(1970,1,1)).total_seconds()
    downloadQuery = 'https://query1.finance.yahoo.com/v7/finance/download/'+ ticker +'?period1=' + str(int(startEpoch)) + '&period2=' + str(int(endEpoch)) + '&interval=1d&events=history&includeAdjustedClose=true'
    print('Incomplete data found. Attempting to download data...', end=' ')
    webbrowser.open(downloadQuery)
    print('Success')
    # Open the file we downloaded
    time.sleep(2)
    filepath = downloadPath + ticker + '.csv'
    history = pandas.read_csv(filepath)
    tickerList = []
    for i in range(history['Date'].count()):
        tickerList.append(ticker)
    history.insert(0,'Ticker', tickerList)
    # print(str(history.count()))
    # for idx, row in history.iterrows():
    #     # Check if we have these records actually, and there's just shitty code lol
    #     selectQuery = 'select * from records where recordDate=\'' + str(row.Date) + '\'' 
    #     holder = pandas.read_sql_query(selectQuery, conn)
    #     if holder['recordDate'].count() == 0:
    #         print('true')
    os.remove(filepath)
    return history




def exportToSQL(df, cursor, conn):
    # print(df)
    for index, row in df.iterrows():
        # print(row['Adj Close'])
        # print(row)
        cursor.execute('insert into records (Ticker, recordDate, marketOpen, dayHigh, dayLow, dayClose, dayAdjClose, tradingVolume) values(?,?,?,?,?,?,?,?)', row.Ticker, row.Date, row.Open, row.High, row.Low, row.Close, row['Adj Close'], row.Volume)
    conn.commit()
    # cursor.close()

def deleteDuplicates(cursor, conn):
    # print('delete duplicates')
    cursor.execute('''
    WITH CTE AS
(
SELECT *,ROW_NUMBER() OVER (PARTITION BY Ticker, recordDate, marketOpen, dayHigh, dayLow, dayClose, dayAdjClose ORDER BY Ticker, recordDate, marketOpen, dayHigh, dayLow, dayClose, dayAdjClose) AS RN
FROM records
)

DELETE FROM CTE WHERE RN<>1''')
    conn.commit()
    # conn.close()

def main():

    #Connection string to check our data for each specific ticker
    ticker = sys.argv[1]
    connectionString = 'Driver={SQL Server};Server=DESKTOP-MAH;Database=stockInfo;Trusted_Connection=True;'
    conn = pyodbc.connect(connectionString)
    cursor = conn.cursor()
    # Queries - we get the most recent record date for the given ticker
    modifiedQuery = 'select * from records where Ticker=\'' + ticker + '\' order by recordDate desc'
    # we get the amount of records present in the db for the ticker in the db
    countQuery = 'select count(Ticker) from records where Ticker=\'' + ticker + '\''
    df = pandas.read_sql_query(countQuery,conn)
    df.columns = ['Count']

    # print(df)
    countDF = df['Count']
    df = pandas.read_sql_query(modifiedQuery,conn)
    # print(df)
    today = date.today().strftime('%Y-%m-%d')
    # recorddf = []
    noRecords = False
    incorrectRecords = False
    if countDF[0] == 0: # If no records are present for the ticker, download the past 5 years of records, import it into a df, and upload it to the sql database
        noRecords = True
        recorddf = get5years(ticker)
        exportToSQL(recorddf, cursor, conn)
    elif df['recordDate'][0] != today: # If we don't have the most updated records, get all missing records (from most recent date to today)
        incorrectRecords = True
        recorddf = getUpdate(ticker, conn)
        # print(type(recorddf))
        exportToSQL(recorddf, cursor, conn)
        # print("mismatch")
    else:
        print("Data up to date.")
    
    if noRecords or incorrectRecords:
        deleteDuplicates(cursor, conn)
    cursor.close()
        
main()