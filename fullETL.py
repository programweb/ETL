#!/usr/bin/python

import pymysql
import numpy as np


# next 3 to help with naming
import string
import random
from datetime import datetime

# note: various isoforms of the protein eg P92177-2 yet you use just P92177 in the link: https://www.uniprot.org/uniprot/P92177
# make the 'primary_accession' in python as part of the TRANSFORM (and maybe just take unique w/o isoforms)


### EXTRACT
sql = """
SELECT vp.acc AS 'primary_accession', pet.val AS evidence, vp.val AS 'amino_acids'
FROM varProtein vp
LEFT JOIN proteinEvidence pe ON pe.acc = SUBSTR(vp.acc, 1, LOCATE('-', vp.acc) - 1)
LEFT JOIN proteinEvidenceType pet ON pet.id = pe.proteinEvidenceType
LIMIT 10;
"""

mydb = pymysql.connect(
    host='genome-mysql.soe.ucsc.edu',
    port=3306,
    user='genome',
    database='uniProt')

mycursor = mydb.cursor()
mycursor.execute(sql)
myresult = mycursor.fetchall()
mycursor.close()


### TRANSFORM
i = 0

accessionList = []
for x in myresult: # x is a tuple
    primary_accession = x[0]
    evidence = x[1]
    amino_acids = x[2]

    # TRANSFORM enrich
    lenaa = len(amino_acids)

    # TRANSFORM validate
    if lenaa < 1:
        continue

   # TRANSFORM convert
    accession_isoform_delim = primary_accession.find('-')
    if accession_isoform_delim == -1:
        accession = primary_accession
    else:
        accession = primary_accession[:accession_isoform_delim]

    if accession not in accessionList:
        accessionList.append( accession )

    tempList = [primary_accession, accession, evidence, lenaa, amino_acids]

    if i == 0:
        arr = tempList
    else:
        arr = np.vstack((arr, tempList))

    i = i + 1

# print(arr)  # ['P48347-2', 'P48347', 'Evidence at transcript level', 254, 'MENERE....' ]
print(accessionList)





### LOAD
# A new table will be created with a unique name
dt = datetime.now()
loadProteins = 'proteins_' + dt.strftime('%Y%m%d%H%M%S')

try:
    """
    CREATE NEW DATABASE TABLE IN A CONTAINER
    """

    dbconn = pymysql.connect(
        host='127.0.0.1',
        port=33061,
        user='root',
        password='ABC',
        database='homestead',
        autocommit=True)

    cursorObject = dbconn.cursor()

    sqlCreateTableCommand   = """CREATE TABLE {table}(
                                    id int(11) AUTO_INCREMENT PRIMARY KEY,
                                    primary_accession varchar(32),
                                    accession varchar(32),
                                    evidence varchar(32),
                                    lenaa int,
                                    amino_acids text)"""
                                        
    sqlCreateTableCommand   = sqlCreateTableCommand.format(table=loadProteins)
    cursorObject.execute(sqlCreateTableCommand)

      
      
    """
    LOAD THE NEW TABLE WITH A LOOP
    """
    for i,a in enumerate(arr):
        insertSQL = """INSERT INTO {table}(
                            primary_accession,
                            accession,
                            evidence,
                            lenaa,
                            amino_acids)
                        VALUES(
                            '{pa}',
                            '{accession}',
                            '{ev}',
                            {lenaa},
                            '{aa}')"""

        insertSQL = insertSQL.format(table=loadProteins, pa=a[0], accession=a[1], ev=a[2], lenaa=a[3], aa=a[4])
        # print(insertSQL)

        cursorObject.execute( insertSQL )

    # SEE WHAT WE HAVE (temp probably want to use another function with Flask and pagination)
    displaySQL = "SELECT COUNT(*) FROM {}"
    displaySQL = displaySQL.format(loadProteins)
    cursorObject.execute(displaySQL)

    row = cursorObject.fetchone()
    print("LOAD COMPLETED")
    print(str(row[0]) + " rows added to the newly created database table: " + loadProteins)
    
    # result = cursorObject.fetchall()
    #for r in result:
    #    print(r)

except Exception as e:
    print("Exeception occured:{}".format(e))

finally:
    cursorObject.close()
