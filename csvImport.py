import csv
import sqlite3
from datetime import date
from random import randint
from forex_python import converter
import re


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

InsertDates = '''INSERT OR IGNORE
                INTO Dates (date) VALUES (?) '''

InsertAccounts = '''INSERT OR IGNORE
                    INTO Accounts (AccountName) VALUES (?)'''

InsertCategory = '''INSERT OR IGNORE
                    INTO Category(Category) VALUES (?)'''

InsertSubCategory = '''INSERT OR IGNORE
                    INTO Subcategory(CategoryID,SubCategory) VALUES (?,?)'''

InsertCurrency = '''INSERT OR IGNORE
                    INTO Currencies(Currency) VALUES (?)'''

InsertLedger = '''INSERT OR IGNORE
                    INTO Ledger(LID,Amount,Note,SubCategoryID,SpendID,IncomeID,DateID,CurrencyID) VALUES (?,?,?,?,?,?,?,?)'''

InsertRate = '''UPDATE Dates SET USDMXN = ? WHERE Date = ?'''

conn = sqlite3.connect(DBSave + 'FinancialsDB_norm.sqlite')
cur = conn.cursor()

cur.executescript(CreateSchema)




with open(FFPath + 'AndroMoney.csv', newline='', encoding='utf-8', errors='replace') as csvfile:
    readerAM = csv.reader(csvfile)
    for row in readerAM:
        SpendingID = 0
        IncomeID = 0
        UID = row[12][6:11]
        if row[6]: 
            cur.execute(InsertAccounts, ([row[6]]))
            cur.execute('SELECT AccID FROM Accounts Where AccountName = ?', (row[6],))
            SpendingID = cur.fetchone()[0]
            # print(SpendingID)

        
                           
        if row[7]: 
            cur.execute(InsertAccounts, (row[7],))
            cur.execute('SELECT AccID FROM Accounts Where AccountName = ?', (row[7],))
            IncomeID = cur.fetchone()[0]
            # print(IncomeID)
            

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



        # try:
        #     EXRate = round(get_rate("USD", "MXN", t), 2)
        # except KeyboardInterrupt:
        #     print("Terminated Keyboard")
        #     conn.commit()
        #     cur.close()
        #     break
        # except converter.RatesNotAvailableError:
        #     EXRate = round(get_rate("USD","MXN",datetime(2016,1,1)),2)
        #     #print('Setting default {} --- {}'.format(t,EXRate))
        
        # if Currency == 'MXN':
        #     #print(Amount, '---', t)
        #     Amount = round(Amount /EXRate ,2)
        #     Currency = 'USD'


        if row[5]: 
            cur.execute(InsertDates, (t,))
            cur.execute('SELECT DID FROM Dates Where date = ?', (t,))
            DateID = cur.fetchone()[0]
            # print(DateID)

        if row[3]: 
            cur.execute(InsertCategory, (row[3],))
            cur.execute('SELECT CID FROM Category Where Category = ?', (row[3],))
            CatID = cur.fetchone()[0]
            # print(CatID)

        if row[4]:
            cur.execute(InsertSubCategory, (CatID, row[4],))
            cur.execute('SELECT SubID FROM SubCategory Where SubCategory = ?', (row[4],))
            SubCatID = cur.fetchone()[0]
            # print(SubCatID)

        if row[1]:
            cur.execute(InsertCurrency, (row[1],))
            cur.execute('SELECT CurrID FROM Currencies Where Currency = ?', (row[1],))
            CurrencyID = cur.fetchone()[0]
            # print(CurrencyID)


        cur.execute(InsertLedger, (UID,Amount, Note,SubCatID,SpendingID,IncomeID,DateID,CurrencyID,))


        # cur.execute('SELECT CID FROM Category Where Category = ?', (row[3],))
        # cur.execute('SELECT SubID FROM SubCategory Where SubCategory = ?', (row[4],))
        # 
        # cur.execute('SELECT AccID FROM Accounts Where AccountName = ?', (row[7],))
        # print(cur.fetchall()[0])


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
    cur.execute(InsertRate, (EXRate,t))
    
        

# print(EXRate, t)

conn.commit()
cur.close()


