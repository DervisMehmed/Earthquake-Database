import os
import requests
from typing import re
import re
import time
import sqlite3
from sqlite3 import Error
from datetime import datetime

date        = list()
hour        = list()
latitude    = list()
longitude   = list()
depth       = list()
mD          = list()
mL          = list()
mw          = list()
place       = list()
status1     = list()

# CONNECTION WITH THE DATABASE
def create_connection():
    try:
        conn = sqlite3.connect('test.db')
        print("Opened database successfully")
        return conn
    except Error as e:
        print(e)

#   Extract Text from Website
def parseSite():
    text = None
    try:
        # REQUEST FROM WEBSITE
        x = requests.get('http://www.koeri.boun.edu.tr/scripts/lst0.asp')
        text = x.text
        # PARSING TEXT
        a = re.search("<pre>", text)
        b = re.search("</pre>", text)
        text = text[a.end():b.start()]
        a = re.search("[0-9]+[.][0-9]+[.][0-9]+\s+[0-9]+[:][0-9]+[:][0-9]+", text)
        text = text[a.start():]
    except Exception as error:
        print(error)

    return text

#   Parsing Given Text and Fill the Arrays
def parseText(text):
    for line in text.splitlines():
        temp = re.search("[0-9]+[.][0-9]+[.][0-9]+\s+[0-9]+[:][0-9]+[:][0-9]+", line)
        if temp:
            # TO FILL DATE
            #date_time_obj = datetime.strptime(temp.group(), '%Y.%m.%d %H:%M:%S')
            date.insert(0, temp.group())
            line = line[temp.end():]
            line = re.sub(r"^\s+", "", line)
            temp = line
            # TO FILL LATITUDE
            temp = re.search("[0-9]+[.][0-9]+", line)
            latitude.insert(0, temp.group())
            line = line[temp.end():]
            line = re.sub(r"^\s+", "", line)
            temp = line
            # TO FILL LONGITUDE
            temp = re.search("[0-9]+[.][0-9]+", line)
            longitude.insert(0, temp.group())
            line = line[temp.end():]
            line = re.sub(r"^\s+", "", line)
            temp = line
            # TO FILL DEPTH
            temp = re.search("[[0-9]+[.][0-9]+", line)
            depth.insert(0, temp.group())
            line = line[temp.end():]
            line = re.sub(r"^\s+", "", line)
            temp = line
            # TO FILL MAGNITUTE (MD)
            temp = re.search("([-.-]*|[0-9]+)+[.]([-.-]|[0-9])+", line)
            mD.insert(0, temp.group())
            line = line[temp.end():]
            line = re.sub(r"^\s+", "", line)
            temp = line
            # TO FILL MAGNITUTE (ML)
            temp = re.search("([-.-]*|[0-9]+)+[.]([-.-]|[0-9])+", line)
            mL.insert(0, temp.group())
            line = line[temp.end():]
            line = re.sub(r"^\s+", "", line)
            temp = line
            # TO FILL MAGNITUTE (Mw)
            temp = re.search("([-.-]*|[0-9]+)+[.]([-.-]|[0-9])+", line)
            mw.insert(0, temp.group())
            line = line[temp.end():]
            line = re.sub(r"^\s+", "", line)
            temp = line
            # TO FILL LOCATIONS
            temp = re.search("([a-zA-Z]*(.[a-zA-Z])*)+(\s|[-])*([(]*([a-zA-Z]*(.[a-zA-Z])*)*[)])*", line)
            place.insert(0, re.sub(r"\s+$", "", temp.group()))
            line = line[temp.end():]
            line = re.sub(r"^\s+", "", line)
            temp = line
            # TO FILL FINALRESULT
            status1.insert(0, temp)

#   Create "records" Table
def createTable(cursor):
    try:
        cursor.execute("""CREATE TABLE IF NOT EXISTS records(
                ID             INT    PRIMARY KEY   NOT NULL,
                EDATE          VARCHAR(128)   NOT NULL,
                LATITUDE       INT    NOT NULL,
                LONGITUDE      INT    NOT NULL,
                DEPTH          INT    NOT NULL,
                MD             INT, 
                ML             INT,
                MW             INT,
                PLACE          VARCHAR(128),
                STATUS         VARCHAR(128));""")
        print("Table created successfully")
    except Error as e:
        print(e)

#   Print Database
def printDB(cur):
    try:
        list2 = cur.execute("""SELECT * FROM records""")

        for row in list2:
            print(row)

        print("Operation done successfully")
    except Error as e:
        print(e)

#   Fill Database
def fillDB(cur, conn):
    for i in (range(len(date))):
        cur.execute("""INSERT INTO records(ID, EDATE, LATITUDE, LONGITUDE, DEPTH, MD, ML, MW, PLACE, STATUS)
            VALUES(?,?,?,?,?,?,?,?,?,?);""",
                    (cur.lastrowid, date[i],latitude[i],longitude[i],depth[i],mD[i],mL[i],mw[i],place[i],status1[i]))
    conn.commit()
    print("Database is fill with the current data!")
    return cur.lastrowid

#   Add Entry to the Database
def addEntry(cur, conn, lastentry, date, latitude, longitude, depth, md, ml, mw, place, status):
    cur.execute("""INSERT INTO records(ID, EDATE, LATITUDE, LONGITUDE, DEPTH, MD, ML, MW, PLACE, STATUS)
                VALUES(?,?,?,?,?,?,?,?,?,?);""",
                ( lastentry, date, latitude, longitude, depth, md, ml, mw, place, status))
    conn.commit()
    print("Entry is added! place: ", lastentry)
    return cur.lastrowid

#   Get Database Size
def getSizeDB(cur):
    list = cur.execute("""SELECT * FROM records ORDER BY ID DESC LIMIT 1;""")
    for row in list:
        return row[0]

#   Closing Database Connection
def closeConn(cur):
    cur.close()
    print("--Connection Ended!--")

#   Clearing Arrays
def clearArrays():
    date.clear()
    latitude.clear()
    longitude.clear()
    depth.clear()
    mL.clear()
    mD.clear()
    mw.clear()
    place.clear()
    status1.clear()

def main():
    previousText = parseSite()
    #   Create Table and Connection
    conn = create_connection()
    cur = conn.cursor()
    createTable(cur)
    lastRowNum = getSizeDB(cur)

    #   if no database is set
    if lastRowNum == None:
        parseText(previousText)
        lastRowNum = fillDB(cur, conn)
        clearArrays()
    else:
        lastRowNum += 1

    while True:
        print("STARTED!")
        time.sleep(30)
        currentText = parseSite()

        if previousText != currentText:
            parseText(currentText)
            print("Text is changed!")

            #   CHECKS IF THERE ARE ANY NEW ENTRY
            #   IF YES, ADDS THAT ENTRY TO DATABASE
            for i in range(len(date)):
                cur.execute("SELECT * FROM records WHERE EDATE=? AND PLACE=?;", (date[i], place[i]))
                list3 = cur.fetchall()

                if len(list3) == 0:
                    lastRowNum = addEntry(cur, conn, lastRowNum, date[i], latitude[i], longitude[i], depth[i],
                                          mD[i], mL[i], mw[i],place[i], status1[i])
                    printDB(cur)

            #   CHECKS IF THERE ARE ANY STATUS CHANGE
            #   IF YES, UPDATES THE STATUS OF THAT ENTRY
            for i in range(len(date)):
                list3 = cur.execute("SELECT STATUS FROM records WHERE EDATE=? AND PLACE=?;", (date[i], place[i],))
                list3 = cur.fetchall()

                for statusEntry in list3:
                    if statusEntry != status1[i]:
                        print("Status changed!")
                        cur.execute("UPDATE records set STATUS=? where EDATE=? AND PLACE=?;",
                                    (status1[i], date[i], place[i]))
                        conn.commit()
                        printDB(cur)
                        break

        clearArrays()
        previousText = currentText
    closeConn(cur)

if __name__ == '__main__':
    main()