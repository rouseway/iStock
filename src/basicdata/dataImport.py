#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-19

import sys,os,logging,csv,ConfigParser
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo

class dataImport:

    def __init__(self, confPath, logPath, dataPath, endDay):
        self.conf_path = confPath
        self.log_path = logPath
        self.data_path = dataPath

        self.end_day = endDay

        config = ConfigParser.ConfigParser()
        config.read(self.conf_path + "/istock.conf")
        self.conn = pymongo.MongoClient(config.get("database", "host"), int(config.get("database", "port")))
        self.db = self.conn.get_database(config.get("database", "dbname"))


    def _checkValid(self, dayStr):
        endDay = self.end_day[:4]+'-'+self.end_day[4:6]+'-'+self.end_day[-2:]
        if endDay == dayStr:
            return True
        else:
            return False


    def _insertCSVData(self, coll, csvData):
        for i in range(1,len(csvData)):
            oneDay = csvData[i]
            try:
                if float(oneDay[6]) == 0: continue
                uname = oneDay[2].decode("gbk")
                oneRecord = dict(date=oneDay[0], stock_name=uname.encode("utf-8"), close_price=float(oneDay[3]), high_price=float(oneDay[4]), \
                        low_price=float(oneDay[5]), open_price=float(oneDay[6]), pre_price=float(oneDay[7]), \
                        rise_amount=float(oneDay[8]), rise_rate=float(oneDay[9]), turn_over=float(oneDay[10]), volume=float(oneDay[11]))
            except Exception,e:
                logging.warning("Invalid data format {%s} for [%s]." % (csvData[i], coll.name))
                continue
            coll.insert(oneRecord)
        logging.info("Insert [%d]-days' data for stock of [%s] into database." % (len(csvData), coll.name))



    def _importHistoryData(self):
        validCodes = []
        fileList = os.listdir(self.data_path + "/history/")
        for f in fileList:
            if len(f) < 7 or f[-4:] != ".csv": continue
            csvfile = file(self.data_path + "/history/" + f, 'rb')
            quoteList = list(csv.reader(csvfile))
            if len(quoteList) < 2 or self._checkValid(quoteList[1][0]) == False: 
                csvfile.close()
                continue
            validCodes.append(f[0:7])
            collName = "stock_" + f[0:7]
            coll = self.db.get_collection(collName)
            self._insertCSVData(coll, quoteList)
            csvfile.close()
        fout = open(self.log_path + "/valid.code", 'w')
        for code in validCodes:
            fout.write("%s\n" % code)
        fout.close()


    def _extendQuotationData(self):
        collectionList = self.db.collection_names()
        for collName in collectionList:
            if len(collName)<5 or collName[0:5] != "stock": continue
            csvName = collName[6:] + ".csv"
            try:
                csvfile = file(self.data_path + "/history/"+csvName, 'rb')
            except Exception,e:
                logging.warning("There is no quote data file of [%s]." % csvName)
                continue
            quoteList = list(csv.reader(csvfile))
            if len(quoteList) < 2:
                csvfile.close()
                continue
            coll = self.db.get_collection(collName)
            self._insertCSVData(coll, quoteList)
            csvfile.close()


    def importData(self, init):
        if init == True:
            logging.info("Initialize the history quotation for stocks.")
            self._importHistoryData()
        else:
            logging.info("Incrementalize latest quotation for stocks.")
            self._extendQuotationData()
        self.conn.close()



if __name__ == "__main__":
    dimp = dataImport("../../conf", "../../log", "../../data", "20160120")
    dimp.importData(True)
