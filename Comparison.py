# Comparison Tool: Written for comparing large datasets for FRO program

# Use the configuration excel "CompareToolConfig.xlsx" to provide input file names, delimiter and primary / unique key information

# In case the column names are different between two files, they can be provided in the columns tab of CompareToolConfig

# The program and files for comparison are expected to be in the same folder

# The program assumes a header record, one or more detailed records and no trailer record

 

import pandas as pd

import numpy as np

import time

import datetime

import string

import re

import os

from os.path import isfile, join

import io

import dask.dataframe as dd

import xlrd

from pandas import ExcelWriter

from pandas import ExcelFile

import xlsxwriter

from pathlib import Path

 

startTime = time.time()

mypath = os.getcwd()

 

print("Program starts ....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

 

#Define output summary report file, will be created with a timestamp for every run

workbook = xlsxwriter.Workbook('Summary and Error Report - '+str(datetime.datetime.now()).split(".")[0].replace(':','')+'.xlsx')

worksheet = workbook.add_worksheet()

writer = pd.ExcelWriter('Summary and Error Report - '+str(datetime.datetime.now()).split(".")[0].replace(':','')+'.xlsx', engine = 'xlsxwriter')

 

#Define variables

nomatchlist = []

nomatchlistunique = []

nomatchlistsum = []

nomatchlistsumdf = pd.DataFrame(columns = ['Input 1 - Column','Input 2 - Column',

                                          'Input 1 - No match amount','Input 2 - No match amount',

                                           'Input 1 - Match amount','Input 2 - Match amount'])

matchnomatchlist = []

matchnomatchdf = pd.DataFrame()

emptycoldf = pd.DataFrame()

emptycollist = []

comparereportmatchdf = pd.DataFrame()

comparereportnomatchdf = pd.DataFrame()

nomatchdf1 = pd.DataFrame()

nomatchdf2 = pd.DataFrame()

enumerationsdf = pd.DataFrame()

colsin1only = []

colsin2only = []

#For large files, read few columns at a time, for client info (5GB), read 15 at a time

#Enhancement: Make it as a config. input -> Potential for user error

noofcolstocompare = 12 # no. of columns to read at one time -> sweet spot for small/big files

colsinonlydf = pd.DataFrame(columns = ['Columns in input 1 only','Columns in input 2 only'])

inputdfljoindf = pd.DataFrame(columns = ['Rows in input 1 only','Rows in input 2 only'])

 

#Read contents of config file

configfilename = 'CompareToolConfig.xlsx'

configdfdash = pd.read_excel(configfilename, sheet_name='Dashboard', header=None)

input1filename     = configdfdash[2][5]

input1delimiter    = configdfdash[2][6]

keytojoin1str      = configdfdash[2][7]

input2filename     = configdfdash[2][8]

input2delimiter    = configdfdash[2][9]

keytojoin2str      = configdfdash[2][10]

nooferrorstoreport = configdfdash[2][11]

 

if nooferrorstoreport == 0 or "" or (not isinstance(nooferrorstoreport, int)):

    nooferrorstoreport = 1000

 

fileerror = False

    

input1filepath = Path(join(mypath,input1filename))

if not input1filepath.is_file():

    fileerror = True

    print("Configuration Error: Input file %s does not exist."%input1filename)

 

input2filepath = Path(join(mypath,input2filename))

if not input2filepath.is_file():

    fileerror = True

    print("Configuration Error: Input file %s does not exist."%input2filename)

 

if not fileerror:   

    

    if input1delimiter not in ('|',',',';'):

        input1delimiter = '|'

 

    if input2delimiter not in ('|',',',';'):

        input2delimiter = '|'

 

    keytojoin1 = keytojoin1str.split(',')

    for i in range(len(keytojoin1)):

        keytojoin1[i] = keytojoin1[i].strip()

 

    keytojoin2 = keytojoin2str.split(',')

    for i in range(len(keytojoin2)):

        keytojoin2[i] = keytojoin2[i].strip()

 

 

    enumlist = []

    amountlist = []

    ignorelist = []

 

    configdfcols = pd.read_excel(configfilename, sheet_name='Columns', header=None)   

    for i in configdfcols.index:

        if i > 7 and configdfcols[1][i] != "" and configdfcols[3][i] == 'Yes' and not pd.isnull(configdfcols[1][i]):

            enumlist.append(configdfcols[1][i])

        if i > 7 and configdfcols[1][i] != "" and configdfcols[4][i] == 'Yes' and not pd.isnull(configdfcols[1][i]):

            amountlist.append(configdfcols[1][i])

        if i > 7 and configdfcols[1][i] != "" and configdfcols[5][i] == 'Yes' and not pd.isnull(configdfcols[1][i]):

            ignorelist.append(configdfcols[1][i])

 

    #Define colummn names

    comparestrdf = pd.DataFrame(columns=['Match','NoMatch'],dtype='str')

    summaryreportdf = pd.DataFrame(columns = ['Summary Report - Attribute','Input file 1','Input file 2'])

    summaryreportdf.to_excel(writer,"Summary")

 

    #Get file paths, assumption, they are in same working folder

    input1file = open(join(mypath,input1filename),encoding="utf8")

    input2file = open(join(mypath,input2filename),encoding="utf8")

 

    #Read file headers for column names

    print("Read files starts for column names")

    print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

    input1filecolumns = pd.read_csv(input1file,delimiter=input1delimiter,nrows=0).columns.values.tolist()

    input2filecolumns = pd.read_csv(input2file,delimiter=input2delimiter,nrows=0).columns.values.tolist()

 

    #Enhancement: read data types and enum/amount fields from config. file

    types_dict1 = {}

    for i in range(len(keytojoin1)):

        types_dict1[keytojoin1[i]] = str

   

    for i in range(len(enumlist)):

        types_dict1[enumlist[i]] = str

       

    for i in range(len(amountlist)):

        types_dict1[amountlist[i]] = float

   

    types_dict2 = {}

    for i in range(len(keytojoin2)):

        types_dict2[keytojoin2[i]] = str   

 

    input1file.seek(0)

    input2file.seek(0)

 

    colnamedict = {}

    for i in configdfcols.index:

        if i > 7 and configdfcols[1][i] != "" and configdfcols[2][i] != "" and not pd.isnull(configdfcols[1][i]) and not pd.isnull(configdfcols[2][i]):

            colnamedict[configdfcols[1][i]] = configdfcols[2][i]

 

    for i in range(len(enumlist)):

        if enumlist[i] in input2filecolumns:

            types_dict1[enumlist[i]] = str       

        elif enumlist[i] in colnamedict:

            types_dict1[colnamedict.get(enumlist[i])] = str       

    

    for i in range(len(amountlist)):

        if amountlist[i] in input2filecolumns:

            types_dict1[amountlist[i]] = str       

        elif amountlist[i] in colnamedict:

            types_dict1[colnamedict.get(amountlist[i])] = str       

            

    #Get index of keytojoin and add it to the mini list of columns used for comparison (keytojoin + 'n' columns at a time)

    keytojoincol1 = []

   

    keycolerror = False

    for i in range(len(keytojoin1)):

        if keytojoin1[i] not in input1filecolumns:

            keycolerror = True

            print("Configuration Error: Key column %s does not exist in Input file 1."%keytojoin1[i])

        else:

            keytojoincol1.append(input1filecolumns.index(keytojoin1[i]))

 

    keytojoincol2 = []

    for i in range(len(keytojoin2)):

        if keytojoin2[i] not in input2filecolumns:

            keycolerror = True

            print("Configuration Error: Key column %s does not exist in Input file 2."%keytojoin2[i])

        else:

            keytojoincol2.append(input2filecolumns.index(keytojoin2[i]))   

            

    if not keycolerror:

 

        #Write a tab for columns in input1 but not in input2

        for col1 in input1filecolumns:

            if col1 not in keytojoin1 and col1 not in input2filecolumns and col1 not in colnamedict:

                colsin1only.append(col1)

 

        #Write a tab for columns in input1 but not in input2

        for col2 in input2filecolumns:

            if col2 not in keytojoin2 and col2 not in input1filecolumns and col2 not in colnamedict.values():

                colsin2only.append(col2)

 

        print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

        print("Read files ends for column names")

 

        print("Read input files starts")

        print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

        input1df = dd.read_csv(input1filename,delimiter=input1delimiter,usecols=keytojoincol1,dtype=types_dict1).compute()

        input2df = dd.read_csv(input2filename,delimiter=input2delimiter,usecols=keytojoincol2,dtype=types_dict2).compute()

        print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

        print("Read input files ends")

 

        #Capture records that are in input 1 but not in input 2 and other way round

        #Enhancement: if the no. of key cols are different in file 1 vs file 2

        print("Input df left outer join starts")

        print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

       

        print(keytojoin1)

        print(keytojoin2)

        input1dfljoin = input1df.merge(input2df,how='left',left_on=keytojoin1, right_on=keytojoin2)

        print('PASSSSS')

       

        #input1dfljoin = input1df.merge(input2df,indicator = True, how='left',left_on=keytojoin1, right_on=keytojoin2).loc[lambda x : x['_merge']!='both']

        print(input1dfljoin.shape[0])

        input2dfljoin = input2df.merge(input1df,indicator = True, how='left',left_on=keytojoin2, right_on=keytojoin1).loc[lambda x : x['_merge']!='both']

        print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

        print("Input df left outer join ends")

 

        input1file.seek(0)

        input2file.seek(0)

 

        print("File Report Start 1")

        print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

 

        #Write File Report 1 - Details

        fileReport = [] #initialize the compare report

        fileReport.extend(["File Name",input1filename,input2filename])

        fileReport.extend(["Primary column(s)",keytojoin1str,keytojoin2str])

        fileReport.extend(["Column Count",len(input1filecolumns),len(input1filecolumns)])

        fileReport.extend(["Row Count",input1df.shape[0],input2df.shape[0]])

        #print(fileReport)

 

        print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

        print("File Report End 1")

 

        compareReport = []

        errorReport = ""

        errorFound = 0

 

 

        #print(fileReport)

 

        #Write File Report 2 - Details

        print("File Report Start 2")

        print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

 

        if input1df.shape[0] == input1df.groupby(keytojoin1).size().shape[0]:

            inputfile1hasdups = "No"

        else:

            inputfile1hasdups = "Yes"

            

        if input2df.shape[0] == input2df.groupby(keytojoin2).size().shape[0]:

            inputfile2hasdups = "No"

        else:

            inputfile2hasdups = "Yes"

 

 

        #if inputfile1hasdups or inputfile2hasdups:   

            #fileReport.append("Removing duplicates for comparison" + "|")

 

        #Removing duplicates from both files

        #Enhancement: Write dups report

        #if inputfile1hasdups:

        #    input1df = input1df.drop_duplicates(subset=keytojoin1,keep='first')

        #    #fileReport.append("Input File 1 Row Count w/o Dups"  + "|" + str(input1df.shape[0])) # row count

        #if inputfile2hasdups:

        #    input2df = input2df.drop_duplicates(subset=keytojoin2,keep='first')

        #    #fileReport.append("Input File 2 Row Count w/o Dups" + "|" + str(input2df.shape[0])) # row count

 

        fileReport.extend(["Duplicates found",inputfile1hasdups,inputfile2hasdups])

        #fileReport.append("No. of rows without dups" + "|" + str(input1df.shape[0]) + "|" + str(input2df.shape[0]))

 

        if inputfile1hasdups == "Yes" or inputfile2hasdups == "Yes":

            print("Program terminated due to duplicate records")

            print("Input file 1 duplicates " +input1df[input1df.duplicated(keep=False)])

            print("Input file 2 duplicates " +input2df[input2df.duplicated(keep=False)])

           

        else:

            fileReport.extend(["Rows only in",input1dfljoin.shape[0],input2dfljoin.shape[0]])

            print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

            print("File Report End 2")

 

            print("Column names compare and arranging starts")

            print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

 

            #Compare column names and arrange them to compare

            input1comparelist = []

            input2comparelist = []

            for col in input1filecolumns:

                if col not in keytojoin1 and col not in keytojoin2 and "Unnamed:" not in col and col not in ignorelist:

                    if col in input2filecolumns:

                  

                        input1comparelist.append(input1filecolumns.index(col))

                        input2comparelist.append(input2filecolumns.index(col))

                    else:

                        if col in colnamedict:

                            input1comparelist.append(input1filecolumns.index(col))

                            input2comparelist.append(input2filecolumns.index(colnamedict.get(col)))

 

            input1comparenlist = []

            for d in list(zip(*([iter(input1comparelist)]*noofcolstocompare))):

                templist = list(d)

                for i in range(len(keytojoincol1)):

                    templist.insert(i,keytojoincol1[i])

                input1comparenlist.append(templist)

            if (len(input1comparenlist)-1)*noofcolstocompare < len(input1comparelist): #add remaining elements, ensure the keytojoin col is removed from count

                templist = input1comparelist[len(input1comparenlist)*noofcolstocompare:len(input1comparelist)]

                for i in range(len(keytojoincol1)):

                    templist.insert(i,keytojoincol1[i])

                input1comparenlist.append(templist)

 

            #Break input 2 into mini list, inline with input 1

            input2comparenlist = []

            for d in list(zip(*([iter(input2comparelist)]*noofcolstocompare))):

                templist = list(d)

                for i in range(len(keytojoincol2)):

                    templist.insert(i,keytojoincol2[i])

                input2comparenlist.append(templist)

            if (len(input2comparenlist)-1)*noofcolstocompare < len(input2comparelist): #add remaining elements, ensure the keytojoin col is removed from count

                templist = input2comparelist[len(input2comparenlist)*noofcolstocompare:len(input2comparelist)]

                for i in range(len(keytojoincol2)):

                    templist.insert(i,keytojoincol2[i])

                input2comparenlist.append(templist)

 

            print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

            print("Column names compare and arranging ends")

 

            print("Column comparing starts now")

            print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

 

            #Read minilist from input 1 and input 2, iterate thru them

            for col1n, col2n in zip(input1comparenlist,input2comparenlist):

 

                print("Columns being compared")

                print(col1n)

 

                print("Reading columns start")

                print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))

 

                #Read input files for 'n' columns, dask read for quick reading, copy to pandas for concats

                input1df = dd.read_csv(input1filename,delimiter=input1delimiter,usecols=col1n,dtype=object).compute()

                input2df = dd.read_csv(input2filename,delimiter=input2delimiter,usecols=col2n,dtype=object).compute()

 

                print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))   

                print("Reading columns end")

 

                print("dynamic n col column concatenate - start")

                print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))               

                tempcolname1 = []

                tempcolname2 = []

                i = 0

 

                for colno in col1n:

                    print('999999999')

                    print(colno)

                    tempcolname1.append(input1df.columns[i])

                    print(tempcolname1)

                    i = i + 1

                print(tempcolname1)

 

                i = 0

                for colno in col2n:

                    tempcolname2.append(input2df.columns[i])

                    i = i + 1

                print(tempcolname2)   

 

                #Get column names in above step and concat 'n' columns at a time

                df_all = pd.concat([input1df[tempcolname1].set_index(keytojoin1),

                                            input2df[tempcolname2].set_index(keytojoin2)],

                                           axis='columns',join='inner',keys=['Input 1','Input 2'],sort=False)

 

                print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))               

                print("dynamic n col column concatenate - end")

               

                if df_all.shape[0] == 0:

                    print("Program terminated: No matching rows found")

                else:

                    #check for dups, if found, remove dups, keep the first row based on keytojoin

                    #if inputfile1hasdups == "Yes":

                    #    input1df = input1df.drop_duplicates(subset=keytojoin1,keep='first')

                    #if inputfile2hasdups == "Yes":

                    #    input2df = input2df.drop_duplicates(subset=keytojoin2,keep='first')

 

                    i = 0

                    #for each column in minilist

                    for col1n1,col2n2 in zip(col1n,col2n):

                        col1name = input1df.columns[i]

                        for j in range(len(list(input2df.columns.values))):

                            if input2df.columns[j] == col1name or input2df.columns[j] == colnamedict.get(col1name):

                                col2name = input2df.columns[j]

                       if col1name not in keytojoin1:

                            if input1df[col1name].isna().all() and input2df[col2name].isna().all():

                                print("compare column " + input1df.columns[i] + " is blank, no need to merge")

                                emptycollist.append(col1name)

                                emptycollist.append(col2name)

 

                            else:

                                #if any of input1 and input2 are not null, fillna to ensure comparison

                                df_all[[('Input 1', col1name)]] = df_all[[('Input 1', col1name)]].fillna("")

                                df_all[[('Input 2', col2name)]] = df_all[[('Input 2', col2name)]].fillna("")

                                matchCount = 0

                                nomatchCount = 0

 

                                df_all['compare'] = np.where(df_all[[('Input 1', col1name)]].values== df_all[[('Input 2', col2name)]].values,"Match", "NoMatch")

                                nomatchCount = 0

                                #Get value counts for match and nomatch

                                comparestr = df_all['compare'].value_counts().to_string()

                                if '\n' in comparestr:

                                    nomatchCount = 1

                                    print(comparestr)

                                    if 'NoMatch' in comparestr.split('\n')[0]:

                                        nomatchCount = comparestr.split('\n')[0].split()[1]

                                        matchCount = comparestr.split('\n')[1].split()[1]

                                    else:

                                        matchCount = comparestr.split('\n')[0].split()[1]

                                        nomatchCount = comparestr.split('\n')[1].split()[1]

                                elif 'NoMatch' in comparestr:

                                    matchCount = 0

                                    nomatchCount = comparestr.split()[1]

                                else:

                                    matchCount = comparestr.split()[1]

                                    nomatchCount = 0

                                compareReport.append(col1name + "|" + col2name + "|" + str(matchCount) + "|" + str(nomatchCount))

 

                                print("error file report start")

                                print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))                                                

 

                                if col1name in amountlist:

                                    testdf = pd.DataFrame()

                                    testdf = df_all.copy()

 

                                    df_all['difference'] = np.where(0,0,

                                                                df_all[[('Input 1', col1name)]].values.astype(np.float)

                                                                    -df_all[[('Input 2', col2name)]].values.astype(np.float))

                                    df_all.sort_values(by=['difference'], inplace=True, ascending=False)

 

                                    nomatchlistsum.append(col1name)

                                    nomatchlistsum.append(col2name)

                                   nomatchamount1 = pd.to_numeric(df_all[('Input 1', col1name)].loc[df_all['compare'] == "NoMatch"]).sum()

                                    nomatchamount2 = pd.to_numeric(df_all[('Input 2', col2name)].loc[df_all['compare'] == "NoMatch"]).sum()

                                    matchamount1 = pd.to_numeric(df_all[('Input 1', col1name)].loc[df_all['compare'] == "Match"]).sum()

                                    matchamount2 = pd.to_numeric(df_all[('Input 2', col2name)].loc[df_all['compare'] == "Match"]).sum()

                                    nomatchlistsum.append(nomatchamount1)

                                    nomatchlistsum.append(nomatchamount2)

                                    nomatchlistsum.append(matchamount1)

                                    nomatchlistsum.append(matchamount2)

 

                                if col1name in enumlist:

 

                                    nomatchlistunique.append(col1name)

                                    nomatchlistunique.append(col2name)

                                    #np.unique(df_all[('Input 1', 'AX_FIN_INST')].values.tolist()).tolist()

                                    #np.unique(df_all[('Input 1', 'AX_FIN_INST')].values)

                                    #nomatchlistunique.append(np.unique(df_all[('Input 1', col1name)].values))

                                    #nomatchlistunique.append(np.unique(df_all[('Input 2', col2name)].values))

                                    ##nomatchlistunique.append(np.unique(df_all[('Input 2', col2name)].values.tolist()).tolist())

                                    nomatchlistunique.append(df_all[[('Input 1', col1name)]].drop_duplicates(subset=None,keep='first').iloc[0:1000])

                                    nomatchlistunique.append(df_all[[('Input 2', col2name)]].drop_duplicates(subset=None,keep='first').iloc[0:1000])

                                    #df_amounts = df_all[[('Input 1', col1name),('Input 2', col2name)]].loc[df_all['compare'] == "NoMatch"]

 

 

                                #write sample no-match records (count is configurable)

                                if int(nomatchCount) > 0:

                                    nomatchlist.append(col1name)

                                    nomatchlist.append(col2name)

                                    nomatchlist.append(df_all[[('Input 1', col1name),('Input 2', col2name)]].loc[df_all['compare'] == "NoMatch"].iloc[0:nooferrorstoreport])

                                print("error file report end")

                                print("....Time taken %s seconds ...." % (round(time.time() - startTime,2)))                                                               

                        i = i + 1

                    input1file.seek(0)

                    input2file.seek(0)

 

            colmatchcount = 0

            colnomatchcount = 0

            for i in compareReport:

                if i.split('|')[3] == '0':

                    colmatchcount = colmatchcount + 1

                else:

                    colnomatchcount = colnomatchcount + 1

 

            fileReport.extend(["Columns only in",len(colsin1only),len(colsin2only)])

            fileReport.extend(["Columns with matching values",colmatchcount,colmatchcount])

            fileReport.extend(["Columns with non-matching values",colnomatchcount,colnomatchcount])

            fileReport.extend(["Columns with empty values",int(len(emptycollist)/2),int(len(emptycollist)/2)])

        #    fileReport.append("Columns tagged as 'enumerations'" + "|" + str(len(enumlist)) + "|" + str(len(enumlist)))

        #    fileReport.append("Columns tagged as 'amounts'" + "|" + str(len(amountlist)) + "|" + str(len(amountlist)))

 

            for i in range(int(len(fileReport)/3)):

                summaryreportdf = summaryreportdf.append({'Summary Report - Attribute':fileReport[3*i],

                                              'Input file 1':fileReport[3*i+1],

                                              'Input file 2':fileReport[3*i+2],

                                             },ignore_index=True)

 

            summaryreportdf.to_excel(writer,"Summary")

            workbook  = writer.book

            worksheet = writer.sheets['Summary']

            worksheet.set_column('B:D', 40, None)

            worksheet.freeze_panes(1, 1)

            fileReport = []

 

            n = 7

 

            if input1dfljoin.shape[0]:

                input1dfljoin[keytojoin1].head(1000).to_excel(writer,"Only in one file 1")

                worksheet = writer.sheets['Only in one file 1']

                worksheet.set_column('B:B', 35, None)

                worksheet.freeze_panes(1, 1)

                worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                worksheet.set_column('F:F', 10, None)

                worksheet = writer.sheets['Summary']

                worksheet.write_url('C'+str(n), f"internal:'Only in one file 1'!B1")

 

            if input2dfljoin.shape[0]:

                input2dfljoin[keytojoin2].head(1000).to_excel(writer,"Only in one file 2")

                worksheet = writer.sheets['Only in one file 2']

                worksheet.set_column('B:B', 35, None)

                worksheet.freeze_panes(1, 1)

                worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                worksheet.set_column('F:F', 10, None)

                worksheet = writer.sheets['Summary']

                worksheet.write_url('D'+str(n), f"internal:'Only in one file 2'!B1")

 

            if input1dfljoin.shape[0] or input2dfljoin.shape[0]:

                n = n + 1   

 

            if len(colsin1only) > 0:

                for col1 in colsin1only:

                   colsinonlydf = colsinonlydf.append({'Columns in input 1 only':col1},ignore_index=True)

                colsinonlydf.to_excel(writer,"Columns only in")

                worksheet = writer.sheets['Columns only in']

                worksheet.set_column('B:B', 35, None)

                worksheet.freeze_panes(1, 1)

                worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                worksheet.set_column('F:F', 10, None)

                worksheet = writer.sheets['Summary']

                worksheet.write_url('C'+str(n), f"internal:'Columns only in'!B1")

 

            if len(colsin2only) > 0:

                colsinonlydf = pd.DataFrame(columns = ['Columns in input 1 only','Columns in input 2 only'])

                for col2 in colsin2only:

                    colsinonlydf = colsinonlydf.append({'Columns in input 2 only':col2},ignore_index=True)

                colsinonlydf.to_excel(writer,"Columns only in")

                worksheet = writer.sheets['Columns only in']

                worksheet.set_column('C:C', 35, None)   

                worksheet.freeze_panes(1, 1)

                worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                worksheet.set_column('F:F', 10, None)

                worksheet = writer.sheets['Summary']

                worksheet.write_url('D'+str(n), f"internal:'Columns only in'!C1")

 

            if len(colsin1only) > 0 or len(colsin2only) > 0:

                n = n + 1   

 

            for listitem in compareReport:

                if int(listitem.split('|')[3]) == 0: # seperating match-no match records

                    comparereportmatchdf = comparereportmatchdf.append({'Input 1 Column':listitem.split('|')[0],'Input 2 Column':listitem.split('|')[1]},ignore_index=True)

                else:

                    if listitem.split('|')[0] in enumlist:

                        enumcolvalue = "Enum Fields"

                    else:

                        enumcolvalue = ""

                    comparereportnomatchdf = comparereportnomatchdf.append({'Input 1 Column':listitem.split('|')[0],

                                                                            'Input 2 Column':listitem.split('|')[1],

                                                                            'Match':int(listitem.split('|')[2]),

                                                                            'No Match':int(listitem.split('|')[3]),

                                                                            'Total':int(listitem.split('|')[2])+int(listitem.split('|')[3]),

                                                                            'Match %':round(int(listitem.split('|')[2])/(int(listitem.split('|')[2])+int(listitem.split('|')[3]))*100,2),

                                                                            'No Match %':round(int(listitem.split('|')[3])/(int(listitem.split('|')[2])+int(listitem.split('|')[3]))*100,2)},

                                                                           ignore_index=True)

 

 

            if comparereportmatchdf.shape[0] > 0:

                comparereportmatchdf.to_excel(writer,"Matching Cols")

                worksheet = writer.sheets['Matching Cols']

                worksheet.set_column('B:C', 35, None)

                worksheet.freeze_panes(1, 1)

                worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                worksheet.set_column('F:F', 10, None)

                worksheet = writer.sheets['Summary']

                worksheet.write_url('C'+str(n), f"internal:'Matching Cols'!B1", string='Matching Cols')

                worksheet.write_url('D'+str(n), f"internal:'Matching Cols'!C1", string='Matching Cols')

                n = n + 1

 

            if comparereportnomatchdf.shape[0] > 0:

                comparereportnomatchdf = comparereportnomatchdf[['Input 1 Column','Input 2 Column', 'Match','No Match','Total','Match %','No Match %']]

                comparereportnomatchdf.to_excel(writer,"Not Match Cols")

                worksheet = writer.sheets['Not Match Cols']

                worksheet.set_column('B:C', 35, None)

                worksheet.set_column('D:H', 12, None)

                worksheet.set_column('J:J', 10, None)

                worksheet.freeze_panes(1, 1)

                worksheet.write_url('J1', f"internal:'Summary'!A1", string='Summary')

                worksheet = writer.sheets['Summary']

                worksheet.write_url('C'+str(n), f"internal:'Not Match Cols'!B1", string='Not Match Cols')

                worksheet.write_url('D'+str(n), f"internal:'Not Match Cols'!C1", string='Not Match Cols')

                n = n + 1

 

            if len(emptycollist) > 0:

                for i in range(int(len(emptycollist)/2)):

                    emptycoldf = emptycoldf.append({'Input 1 Column':emptycollist[2*i],'Input 2 Column':emptycollist[2*i+1]},ignore_index=True)

                emptycoldf.to_excel(writer,"Empty Cols")

                worksheet = writer.sheets['Empty Cols']

                worksheet.set_column('B:C', 35, None)

                worksheet.freeze_panes(1, 1)

                worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                worksheet.set_column('F:F', 10, None)

                worksheet = writer.sheets['Summary']

                worksheet.write_url('C'+str(n), f"internal:'Empty Cols'!B1", string='Empty Columns')

                worksheet.write_url('D'+str(n), f"internal:'Empty Cols'!C1", string='Empty Columns')

                n = n + 1

 

            if len(enumlist) > 0:

                worksheet = writer.sheets['Summary']

                worksheet.write_url('C'+str(n), f"internal:'Enumerations'!A1", string='Enumerations')

                worksheet.write_url('D'+str(n), f"internal:'Enumerations'!A1", string='Enumerations')

                n = n + 1

                for i in range(int(len(nomatchlistunique)/4)):

                    nomatchdf1 = nomatchdf1.append(nomatchlistunique[4*i+2])

                    nomatchdf2 = nomatchdf2.append(nomatchlistunique[4*i+3])

                    enumdf1 = pd.DataFrame()

                    enumdf1 = enumdf1.append(['Input 1 - ' + nomatchlistunique[4*i]])

                    enumdf1 = enumdf1.append([x for x in nomatchdf1[('Input 1', nomatchlistunique[4*i])].values.tolist() if str(x) != 'nan'])

                    enumdf2 = pd.DataFrame()

                    enumdf2 = enumdf2.append(['Input 2 - ' + nomatchlistunique[4*i+1]])

                    enumdf2 = enumdf2.append([x for x in nomatchdf2[('Input 2', nomatchlistunique[4*i+1])].values.tolist() if str(x) != 'nan'])

 

                    enumdf1.to_excel(writer,'I1 ' + nomatchlistunique[4*i])

                    worksheet = writer.sheets['I1 ' + nomatchlistunique[4*i]]

                    worksheet.set_column('B:B', 35, None)

                    worksheet.freeze_panes(2, 1)

                    worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                    worksheet.write_url('F2', f"internal:'Enumerations'!A1", string='Enumerations')

                    enumdf2.to_excel(writer,'I2 ' + nomatchlistunique[4*i+1])

                    worksheet = writer.sheets['I2 ' + nomatchlistunique[4*i+1]]

                    worksheet.set_column('B:B', 35, None)

                    worksheet.freeze_panes(2, 1)

                    worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')           

                    worksheet.write_url('F2', f"internal:'Enumerations'!A1", string='Enumerations')

                    enumerationsdf = enumerationsdf.append({'Input 1 Column':nomatchlistunique[4*i],

                                                            'Input 2 Column':nomatchlistunique[4*i+1]},

                                                               ignore_index=True)

                enumerationsdf.to_excel(writer,"Enumerations")

                worksheet = writer.sheets['Enumerations']

                worksheet.set_column('B:C', 35, None)

                worksheet.freeze_panes(1, 1)

                # give links to enumerations

                for row_cells in range(enumerationsdf.shape[0]):

                    cellcolvalue1 = enumerationsdf.iloc[row_cells,0]

                    cellcolvalue2 = enumerationsdf.iloc[row_cells,1]

                    #cellnumvalue1 = comparereportnomatchdf.iloc[row_cells,1]

                    celllinkvalue1 = 'internal:'+"'I1 "+cellcolvalue1+"'"+'!A1'

                    celllinkvalue2 = 'internal:'+"'I2 "+cellcolvalue2+"'"+'!A1'

                    worksheet.write_url('B'+str(row_cells + 2), celllinkvalue1, string=cellcolvalue1)

                    worksheet.write_url('C'+str(row_cells + 2), celllinkvalue2, string=cellcolvalue2)

                worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                worksheet.set_column('F:F', 10, None)

 

            if len(amountlist) > 0:

                worksheet = writer.sheets['Summary']

                worksheet.write_url('C'+str(n), f"internal:'Amount Fields'!B1", string='Amounts')       

                worksheet.write_url('D'+str(n), f"internal:'Amount Fields'!C1", string='Amounts')       

 

            sumwritten = 0   

            if len(nomatchlist) > 0:

                for i in range(int(len(nomatchlist)/3)):

                    nomatchdf = pd.DataFrame()

                    nomatchdf = nomatchdf.append(nomatchlist[3*(i+1)-1])

                    nomatchdf.to_excel(writer,nomatchlist[3*i])

                    worksheet = writer.sheets[nomatchlist[3*i]]

                    worksheet.set_column('A:C', 35, None)

                    worksheet.freeze_panes(2, 1)

                    worksheet.write_url('F1', f"internal:'Summary'!A1", string='Summary')

                    worksheet.write_url('F2', f"internal:'Not Match Cols'!A1", string='No-Match Columns')

                    worksheet.set_column('F:F', 20, None)

 

                    if nomatchlist[3*i] in amountlist and sumwritten == 0:

                        sumwritten = 1

                        #nomatchlistsumdf = pd.DataFrame()

                        for i in range(int(len(nomatchlistsum)/6)):

                            nomatchlistsumdf = nomatchlistsumdf.append({'Input 1 - Column':nomatchlistsum[6*i],

                                                                        'Input 2 - Column':nomatchlistsum[6*i+1],

                                                                        'Input 1 - No match amount':nomatchlistsum[6*i+2],

                                                                        'Input 2 - No match amount':nomatchlistsum[6*i+3],

                                                                        'Input 1 - Match amount':nomatchlistsum[6*i+4],

                                                                        'Input 2 - Match amount':nomatchlistsum[6*i+5]},

                                                                           ignore_index=True, sort=False)     

                            nomatchlistsumdf.to_excel(writer,'Amount Fields')

                            worksheet = writer.sheets['Amount Fields']

                            worksheet.set_column('B:C', 35, None)

                            worksheet.set_column('D:G', 23, None)

                            worksheet.freeze_panes(1, 1)

                            worksheet.write_url('I1', f"internal:'Summary'!A1", string='Summary')

                            worksheet.set_column('I:I', 10, None)                   

 

            if comparereportnomatchdf.shape[0] > 0:

                for row_cells in range(comparereportnomatchdf.shape[0]):

                    worksheet = writer.sheets['Not Match Cols']

                    cellcolvalue = comparereportnomatchdf.iloc[row_cells,0]

                    cellnumvalue = comparereportnomatchdf.iloc[row_cells,3]

                    celllinkvalue = 'internal:'+"'"+cellcolvalue+"'"+'!A1'

                    worksheet.write_url('E'+str(row_cells + 2), celllinkvalue, string=str(int(cellnumvalue)))

                    #if comparereportnomatchdf.iloc[row_cells,7] != "":

                    #    cellcolvalue = comparereportnomatchdf.iloc[row_cells,0]

                    #    celllinkvalue = 'internal:'+"'"+cellcolvalue+" enum"+"'"+'!A1'

                    #    worksheet.write_url('I'+str(row_cells + 2), celllinkvalue, string='unique mismatches')

 

            if len(enumlist) > 0:

                fileReport.extend(["Columns tagged as 'enumerations'",len(enumlist),len(enumlist)])

            if len(amountlist) > 0:

                fileReport.extend(["Columns tagged as 'amounts'",len(amountlist),len(amountlist)])

 

            for i in range(int(len(fileReport)/3)):

                summaryreportdf = summaryreportdf.append({'Summary Report - Attribute':fileReport[3*i],'Input file 1':fileReport[3*i+1],'Input file 2':fileReport[3*i+2]},ignore_index=True)

 

            summaryreportdf.to_excel(writer,"Summary")

 

 

        writer.save()   

        writer.close()

        workbook.close()

 

    input1file.close()

    input2file.close()

 

print("End of program")

print("\n....Time taken %s seconds ...." % (round(time.time() - startTime,2)))