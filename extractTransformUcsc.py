#!/usr/bin/python

import pymysql
import numpy as np

# note: various isoforms of the protein eg P92177-2 yet you use just P92177 in the link: https://www.uniprot.org/uniprot/P92177
# make the 'primary_accession' in python as part of the TRANSFORM (and maybe just take unique w/o isoforms)


### EXTRACT
sql = """
SELECT vp.acc AS 'primary_accession', pet.val AS evidence, vp.val AS 'amino_acids'
FROM varProtein vp 
LEFT JOIN proteinEvidence pe ON pe.acc = SUBSTR(vp.acc, 1, LOCATE('-', vp.acc) - 1)
LEFT JOIN proteinEvidenceType pet ON pet.id = pe.proteinEvidenceType
LIMIT 1;
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

print(arr)
print(accessionList)

### LOAD into a local database
# for x in myresult:
#   print(x)

