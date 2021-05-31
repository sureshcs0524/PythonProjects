import pandas as pd
import pymysql
import numpy as np
import warnings
import shutil, os
import json
import logging
warnings.filterwarnings("ignore")

class InvoiceProcess:
    #Class Init to Initialize logging and Database connectivity
    def __init__(self):
        connection =''
        logging.basicConfig(level=logging.INFO, filename='InvoiceProcessing.log', filemode='w')
        try:
            self.connection = pymysql.connect(host='localhost',
                                          user='root',
                                          password='Start162',
                                          database='InvoiceDB',
                                          cursorclass=pymysql.cursors.DictCursor)
            logging.info("Pymql Connection Established")
        except pymysql.Error as e:
            logging.getLogger("Pymql Connection Failure %d %s" %(e.args[0], e.args[1]))
    # Connect to Database
    def connect(self):
        try:
            self.connection = pymysql.connect(host='localhost',
                                          user='root',
                                          password='Start162',
                                          database='InvoiceDB',
                                          cursorclass=pymysql.cursors.DictCursor)
            logging.info("Pymql Connection Established" )
        except pymysql.Error as e:
            self.conn.rollback()
            logging.getLogger("Pymql Connection Failure %d %s" %(e.args[0], e.args[1]))

    # Fetch all the Input files(CSV files) from the input directory and Call the Execution for Read, Load files into database and Move processed files
    def readInfiles(self) -> object:
        try:
            Inpath = "../input"
            for file in os.listdir(Inpath):
                if file.endswith(".csv"):
                    Infile_path = Inpath+'/'+file
                    # call read text file function
                    self.read_invoice(Infile_path)
                    self.writeOutfile(Infile_path)
            logging.info("Open, read and Load Input files into database done!!!")
        except:
            logging.DEBUG("Loading and/or reading file Failures")

    # Move the Processed Invoice File into Output directory
    def writeOutfile(self,Infile_path) -> object:
        try:
            outpath = "../output"
            shutil.move(Infile_path, outpath)
        except:
            logging.DEBUG("Failed to Move the file to output folder")

    #Read the Invoices from the CSV file and load into to MySQL Data Base -Table: INVOICETAB
    def read_invoice(self, Infile_path) -> object:
        try:
            data = pd.read_csv(fr'{Infile_path}', converters={'CONTRACT_NO': lambda x: str(x)})
            df = pd.DataFrame(data, columns=['ID', 'INVOICE_NO', 'DATE', 'TYPE', 'CONTRACT_NO', 'DEBIT', 'CREDIT', 'DUE_DATE', 'DOCUMENT', 'LETTERING', 'RECOVERY'])
            df.INVOICE_NO = df.INVOICE_NO.astype('str')
            df['DATE'] = pd.to_datetime(df['DATE'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
            df['DUE_DATE'] = pd.to_datetime(df['DUE_DATE'], format='%d/%m/%Y').dt.strftime('%Y-%m-%d')
            df['ID'] = df['ID'].astype(int)
            df['DEBIT'] = pd.to_numeric(df['DEBIT'], errors='coerce')
            df['DEBIT'] = df['DEBIT'].replace(np.nan, 0, regex=True)
            df['DEBIT'] = df['DEBIT'].astype(float)
            df['CREDIT'] = pd.to_numeric(df['CREDIT'], errors='coerce')
            df['CREDIT'] = df['CREDIT'].replace(np.nan, 0, regex=True)
            df['CREDIT'] = df['CREDIT'].astype(float)
            logging.info("Read the Invoice Input file, parsing and loading into dataframe done!!!")
        except:
            logging.DEBUG("Failed to Read and load into Dataframe")
        with self.connection:
            try:
                with self.connection.cursor() as cursor:
                    # Load the data into Invoice Table
                    try:
                        for index, record in df.iterrows():
                            sql = '''INSERT INTO INVOICETAB (  INVOICE_NO, DATE, TYPE, CONTRACT_NO, DEBIT, CREDIT, DUE_DATE, DOCUMENT, LETTERING, RECOVERY)
                                                 VALUES ( '{}', '{}', '{}', '{}', '{}', '{}', '{}', '{}','{}', '{}')
                                              '''.format(record[1], record[2], record[3], record[4], record[5], record[6], record[7], record[8], record[9], record[10])
                            cursor.execute(sql)
                    except pymysql.Error as e:
                        self.conn.rollback()
                        logging.getLogger("Pymql Insert into Invoice table failure %d %s" % (e.args[0], e.args[1]))
                self.connection.commit()
            except pymysql.Error as e:
                self.conn.rollback()
                logging.getLogger("Pymql Insert into Invoice table or cursor failure - After commit %d %s" % (e.args[0], e.args[1]))

    # Fetch all the Records of Invoices & Payment - Based on Lettering/Association ID & Calculate Debit and Credit Amounts
    def fetch_all(self) -> object:
        if not self.connection.open:
            self.connect()
            with self.connection:
                try:
                    with self.connection.cursor() as cursor:
                        try:
                            sql = "SELECT * FROM `INVOICETAB`"
                            cursor.execute(sql)
                            logging.info("Fetch All the records from Invoice Table done!!!")
                        except pymysql.Error as e:
                            logging.getLogger("Pymql Fetch all records from Invoice table failure %d %s" % (e.args[0], e.args[1]))
                        resultset = cursor.fetchall()
                        assert isinstance(resultset, object)
                        print("Result Set ")
                        allRecordsDF = pd.DataFrame(resultset)
                        print(allRecordsDF)
                        allRecordsDF.INVOICE_NO = allRecordsDF.INVOICE_NO.astype('str')
                       #  result = allRecordsDF.to_json(orient="split")
                        allRecordsSetJson = allRecordsDF.to_json()
                        parsedAll = json.loads(allRecordsSetJson)
                        print(parsedAll)
                        with open('../output/AllRecords.json', 'w') as outfile:
                            json.dump(parsedAll, outfile)
                    logging.info("Fetch All records method executed successfully including creation of Json file with result set!!!")
                except:
                    logging.DEBUG("Fetch All records method failed")

    # Fetch all Matched Records of Invoices & Payment - Based on Lettering/Association ID & Calculate Debit and Credit Amounts
    def fetch_matchedInvoices(self) -> object:
        if not self.connection.open:
            self.connect()
            with self.connection:
                try:
                    with self.connection.cursor() as cursor:
                        try:
                            sql = '''select * from invoicetab it where it.lettering in (select itt.lettering from invoicetab itt where itt.lettering <> 'nan' and itt.lettering = it.lettering) order by it.lettering, it.date'''
                            cursor.execute(sql)
                            logging.info("Fetch All the Matched Invoices from Invoice Table done!!!")
                        except pymysql.Error as e:
                            logging.getLogger("Pymql Fetch all the Matched Invoices from Invoice table failure %d %s" % (e.args[0], e.args[1]))
                        resultset = cursor.fetchall()
                        assert isinstance(resultset, object)
                        print("Result Set")
                        matchedDF = pd.DataFrame(resultset)
                        matchedDF = matchedDF.groupby(['LETTERING'])['DEBIT','CREDIT'].sum().reset_index().set_index('LETTERING')
                        print("Matched Data Frame :", matchedDF)
                        print("MatcehdDF to_json format: ", matchedDF.to_json())
                        # matchedDF.to_html(header="true", table_id="table")
                        #matchedSetJson = matchedDF.to_json(orient="split")
                        matchedSetJson = matchedDF.to_json()
                        parsedMatch = json.loads(matchedSetJson)
                        print(parsedMatch)
                        with open('../output/MatchedRecords.json', 'w') as outfile:
                            json.dump(parsedMatch, outfile)
                        logging.info("Fetch Matched Invoices method executed successfully including creation of Json file with result set!!!")
                except:
                    logging.DEBUG("Fetch Matched Invoices records method failed")

    # Fetch all Unmatched Records of Invoices & Payment - Calculate Debit and Credit Amounts
    def fetch_unMatchedInvoices(self) -> object:
        if not self.connection.open:
            self.connect()
            with self.connection:
                try:
                    with self.connection.cursor() as cursor:
                        try:
                            sql = '''select * from invoicetab it where it.lettering = 'nan' order by it.recovery, it.date'''
                            cursor.execute(sql)
                            logging.info("Fetch All the UnMatched Invoices from Invoice Table done!!!")
                        except pymysql.Error as e:
                            logging.getLogger("Pymql Fetch all the UnMatched Invoices from Invoice table failure %d %s" % (e.args[0], e.args[1]))
                        resultset = cursor.fetchall()
                        assert isinstance(resultset, object)
                        print("Unmatched Result Set", resultset)
                        unmatchedDF = pd.DataFrame(resultset)
                        unmatchedDF = unmatchedDF.groupby([ 'INVOICE_NO','RECOVERY'])['DEBIT', 'CREDIT'].sum().reset_index()
                        print(unmatchedDF)
                        unmatchedDF.INVOICE_NO  = unmatchedDF.INVOICE_NO.astype('str')
                        # print(unmatchedDF.to_json())
                        unMatchedInvcSetJson = unmatchedDF.to_json()
                        parsedUnMatchInvc = json.loads(unMatchedInvcSetJson)
                        print(parsedUnMatchInvc)
                        with open('../output/UnMatchedInvcRecords.json', 'w') as outfile:
                            json.dump(parsedUnMatchInvc, outfile)
                        logging.info("Fetch UnMatched Invoices method executed successfully including creation of Json file with result set!!!")
                except:
                        logging.DEBUG("Fetch UnMatched Invoices records method failed")
    # Fetch all Unmatched Records of Invoices & Payment - Group by Recovery & Calculate Debit and Credit Amounts
    def fetch_unMatchedSummary(self) -> object:
        if not self.connection.open:
            self.connect()
            with self.connection:
                try:
                    with self.connection.cursor() as cursor:
                        try:
                            sql = '''select * from invoicetab it where it.lettering = 'nan' order by it.recovery, it.date'''
                            cursor.execute(sql)
                        except pymysql.Error as e:
                            logging.getLogger("Pymql Fetch all the UnMatched Invoices for Summary Calculation from Invoice table failure %d %s" % (e.args[0], e.args[1]))
                        resultset = cursor.fetchall()
                        assert isinstance(resultset, object)
                        print("Unmatched Result Set", resultset)
                        unmatchedSumDF = pd.DataFrame(resultset)
                        unmatchedSumDF = unmatchedSumDF.groupby(['RECOVERY'])['DEBIT', 'CREDIT'].sum().reset_index().set_index('RECOVERY')
                        print(unmatchedSumDF)
                        print(unmatchedSumDF.to_json())
                        unMatchedSumSetJson = unmatchedSumDF.to_json()
                        parsedUnMatchSum = json.loads(unMatchedSumSetJson)
                        print(parsedUnMatchSum)
                        with open('../output/UnMatchedSumRecords.json', 'w') as outfile:
                            json.dump(parsedUnMatchSum, outfile)
                        logging.info(
                            "Fetch UnMatched Summary method executed successfully including creation of Json file with result set!!!")
                except:
                    logging.DEBUG("Fetch UnMatched Summary records method failed")
