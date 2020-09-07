import csv
import sqlite3
import re
from datetime import date
from forex_python import converter
from gdrived import download_file_from_google_drive


_CURRENCY_FORMATTER = converter.CurrencyRates()
get_rate = _CURRENCY_FORMATTER.get_rate


FFPath = '/mnt/d/Projects/SourceCode/FlatFiles/'
DBSave = '/mnt/d/Projects/SourceCode/Python/FinancialPy/DB/'

CreateSchema = '''
create table if not exists Ledger(
LID CHAR[5] PRIMARY KEY NOT NULL UNIQUE,
Amount FLOAT NOT NULL,
Note TEXT,
SubCategoryID INTEGER,
SpendID INTEGER,
IncomeID INTEGER,
DateID INTEGER,
CurrencyID INTEGER,
    FOREIGN KEY (SubCategoryID) REFERENCES Subcategory(SubID),
    FOREIGN KEY (SpendID) REFERENCES Accounts(AccID),
    FOREIGN KEY (IncomeID) REFERENCES Accounts(AccID),
    FOREIGN KEY (DateID) REFERENCES Dates(DID),
    FOREIGN KEY (CurrencyID) REFERENCES Currencies(CurrId)
    
);
create table if not exists Category(
CID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
Category CHAR[20] UNIQUE


);
create table if not exists Subcategory(
SubID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
SubCategory CHAR[20] UNIQUE,
CategoryID INTEGER,
    FOREIGN KEY(CategoryID) REFERENCES Category(CID)
);

create table if not exists Dates (
DID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
date DATE UNIQUE,
USDMXN Real
);

create table if not exists Accounts(
    AccID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
    AccountName CHAR[20] UNIQUE
);

create table if not exists Currencies(
    CurrID INTEGER PRIMARY KEY AUTOINCREMENT NOT NULL UNIQUE,
    Currency CHAR[20] UNIQUE
)
'''



def Financial_DML (fields, table, type='insert'):
    if type == 'insert':
        sqlDML =  'INSERT OR IGNORE INTO ' + table +'( ' +','.join(fields) + ' ) VALUES ('+ str('?,'*len(fields))[:-1] + ')'
    elif type == 'update':
        print('now updating')
        sqlDML = ''
    return sqlDML
    

with open('/mnt/d/Projects/SourceCode/FlatFiles/gdrive_files') as keys:
    reader_gdrive = csv.reader(keys)
    for row in reader_gdrive:
        if row[0] == 'andromoney':
            g_key = row[1]
            break

download_file_from_google_drive(g_key, FFPath + 'AndroMoney.csv')




fields = ['date']
table = 'Dates'
InsertDates = Financial_DML (fields, table) 

fields = ['AccountName']
table = 'Accounts'
InsertAccounts = Financial_DML (fields, table) 

fields = ['Category']
table = 'Category'
InsertCategory = Financial_DML (fields, table) 

fields = ['CategoryID','SubCategory']
table = 'Subcategory'
InsertSubCategory = Financial_DML (fields, table) 

fields = ['Currency']
table = 'Currencies'
InsertCurrency = Financial_DML (fields, table) 

fields = ['LID','Amount','Note','SubCategoryID','SpendID','IncomeID','DateID','CurrencyID']
table = 'Ledger'
InsertLedger = Financial_DML (fields, table) 

UpdateRate = '''UPDATE Dates SET USDMXN = ? WHERE Date = ?'''


conn = sqlite3.connect(DBSave + 'FinancialsDB_norm.sqlite')
cur = conn.cursor()

cur.executescript(CreateSchema)



with open(FFPath + 'AndroMoney.csv', newline='', encoding='utf-8', errors='replace') as csvfile:
    readerAM = csv.reader(csvfile)
    for row in readerAM:
        if row[0] == 'Google Documents' or row[0] == 'Id': continue
        SpendingID = 0
        IncomeID = 0
        UID = row[12][6:11]
        if row[6]: 
            cur.execute(InsertAccounts, ([row[6]]))
            cur.execute('SELECT AccID FROM Accounts Where AccountName = ?', (row[6],))
            SpendingID = cur.fetchone()[0]


        
                           
        if row[7]: 
            cur.execute(InsertAccounts, (row[7],))
            cur.execute('SELECT AccID FROM Accounts Where AccountName = ?', (row[7],))
            IncomeID = cur.fetchone()[0]
            

        DateRaw = row[5]
        Year = DateRaw[:4]
        Month = DateRaw[4:6]
        Day = DateRaw[6:]
        Date = Day + '/' + Month + '/' + Year
        Week = date(int(Year), int(Month), int(Day)).isocalendar()[1]
        t = date(int(Year), int(Month), int(Day))
        Currency = row[1]
        Amount = round(float(row[2]), 2)
        Note = re.sub("[^A-Za-z0-9]+",".",row[8])


        if row[5]: 
            cur.execute(InsertDates, (t,))
            cur.execute('SELECT DID FROM Dates Where date = ?', (t,))
            DateID = cur.fetchone()[0]


        if row[3]: 
            cur.execute(InsertCategory, (row[3],))
            cur.execute('SELECT CID FROM Category Where Category = ?', (row[3],))
            CatID = cur.fetchone()[0]

        if row[4]:
            cur.execute(InsertSubCategory, (CatID, row[4],))
            cur.execute('SELECT SubID FROM SubCategory Where SubCategory = ?', (row[4],))
            SubCatID = cur.fetchone()[0]


        if row[1]:
            cur.execute(InsertCurrency, (row[1],))
            cur.execute('SELECT CurrID FROM Currencies Where Currency = ?', (row[1],))
            CurrencyID = cur.fetchone()[0]



        cur.execute(InsertLedger, (UID,Amount, Note,SubCatID,SpendingID,IncomeID,DateID,CurrencyID,))



cur.execute('SELECT date FROM Dates where USDMXN is null')
Dates = cur.fetchall()
if len(Dates) == 0: print("USDMXN Values Filled") 
for day in Dates:
    Year = int(day[0][:4])
    Month = int(day[0][5:7])
    Day = int(day[0][8:])
    t = date(Year, Month, Day)
    try:
        EXRate = round(get_rate("USD", "MXN", t), 2)
    except KeyboardInterrupt:
        print("Terminated Keyboard")
        conn.commit()
        cur.close()
        break
    except converter.RatesNotAvailableError:
        EXRate = round(get_rate("USD","MXN",date(2016,1,1)),2)

    print(f'Date: {t} ExRate {EXRate}')
    cur.execute(UpdateRate, (EXRate,t))

conn.commit()
cur.close()


