#!/usr/bin/env python3

import MySQLdb
import csv


def pN(inp):
    if len(str(inp).strip()) == 0:
        return -99.99

    return inp


conn = MySQLdb.connect(
    host="localhost",
    user="root",
    passwd="",
    unix_socket="/home/user/mysql/mysql.sock ",
    read_default_file="/home/user/mysql/my.cnf "
)

cursor = conn.cursor()
cursor.execute("USE PROB")
cursor.execute("DROP TABLE TWOMASS")

create_str = "CREATE TABLE TWOMASS ( ID INT , RA2000 DOUBLE , DEC2000 DOUBLE , TWOMASSID CHAR (18), JMAG DOUBLE , EJMAG DOUBLE , HMAG DOUBLE , EHMAG DOUBLE , KMAG DOUBLE , EKMAG DOUBLE , Qflg CHAR (3) , Rflg CHAR (3) , Bflg CHAR (3) , Cflg CHAR (3) , Xflg INT , Aflg INT , PRIMARY KEY ( ID ))"
cursor.execute(create_str)

inp_file = csv.reader(open('2mass.tsv'), delimiter="|", quoting=csv.QUOTE_NONE)

i = 0
for row in inp_file:
    if len(row) == 0:
        break

    i = i + 1

    ins_str = "INSERT INTO TWOMASS VALUES (%i ,%s ,%s ,\ ’% s\ ’ ,%s ,%s ,%s ,%s ,%s ,%s ,\ ’%s\ ’ ,\ ’%s\ ’ ,\ ’%s\ ’ ,\ ’%s\ ’ ,%s ,% s )" % (
        i, row[3], row[4], row[5], pN(row[6]), pN(row[7]), pN(row[8]), pN(row[9]), pN(row[10]), pN(row[11]), row[12], row[13], row[14], row[15], row[16], row[17])

    print(ins_str)
    cursor.execute(ins_str)

cursor.close()
conn.close()
