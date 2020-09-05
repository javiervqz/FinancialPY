import csv
import sqlite3
from datetime import datetime, date
from forex_python import converter
import re


_CURRENCY_FORMATTER = converter.CurrencyRates()
get_rate = _CURRENCY_FORMATTER.get_rate


FFPath = '/mnt/d/Projects/SourceCode/FlatFiles/'
DBSave = '/mnt/d/Projects/SourceCode/Python/FinancialPy/DB/'

CreateSchema = '''
create table if not exists Spending(
Amount FLOAT NOT NULL,
Currency TEXT,
Category TEXT,
SubCategory TEXT,
Account TEXT,
Date DATE,
Note TEXT,
UID TEXT PRIMARY KEY
);
create table if not exists Income(
Amount FLOAT  NOT NULL,
Currency TEXT,
Category TEXT,
SubCategory TEXT,
Account TEXT,
Date DATE,
Note TEXT,
UID TEXT PRIMARY KEY
);
create table if not exists Transfers(
Amount FLOAT  NOT NULL,
Currency TEXT,
Category TEXT,
SubCategory TEXT,
AccountOut TEXT,
AccountIn TEXT,
Date DATE,
Note TEXT,
UID TEXT PRIMARY KEY
);
'''

conn = sqlite3.connect(DBSave + 'FinancialsDB.sqlite')
cur = conn.cursor()

cur.executescript(CreateSchema)



with open(FFPath + 'AndroMoney.csv', newline='', encoding='utf-8', errors='replace') as csvfile:
    readerAM = csv.reader(csvfile)
    for row in readerAM:
        Currency = row[1]
        Amount = round(float(row[2]), 2)
        Category = row[3]
        SubCategory = row[4]
        DateRaw = row[5]
        Year = DateRaw[:4]
        Month = DateRaw[4:6]
        Day = DateRaw[6:]
        Date = Day + '/' + Month + '/' + Year
        Week = date(int(Year), int(Month), int(Day)).isocalendar()[1]
        t = datetime(int(Year), int(Month), int(Day))
        ExpenseAccount = row[6]
        IncomeAccount = row[7]
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
        #     print(f'{SubCategory} --- {Amount} USD {EXRate}')

        Note = re.sub("[^A-Za-z]+",".",row[8])

        UID = row[12]
        if IncomeAccount and not ExpenseAccount and Year != 1010:
            cur.execute('''
                           INSERT OR IGNORE
                           INTO Income (Amount, Currency, Category, SubCategory, Account, Date, Note, UID) VALUES (?,?,?,?,?,?,?,?)''', (Amount, Currency, Category, SubCategory, IncomeAccount, t, Note,UID))
        elif not IncomeAccount and ExpenseAccount:
            cur.execute('''
                           INSERT OR IGNORE
                           INTO Spending (Amount, Currency, Category, SubCategory, Account, Date, Note, UID) VALUES (?,?,?,?,?,?,?,?)''', (Amount, Currency, Category, SubCategory, ExpenseAccount, t, Note,UID))
        else:
            cur.execute('''
                           INSERT OR IGNORE
                           INTO Transfers (Amount, Currency, Category, SubCategory, AccountOut, AccountIn, Date, Note,UID) VALUES (?,?,?,?,?,?,?,?,?)''', (Amount, Currency, Category, SubCategory, ExpenseAccount, IncomeAccount, t, Note, UID))

conn.commit()
cur.close()
