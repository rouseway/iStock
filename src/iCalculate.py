#!/bin/env python
# -*- coding: utf-8 -*-
#author: raosiwei
#date: 2016-01-13

import sys,logging,ConfigParser
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
from indicator import ma,kdj,macd,boll 
import pymongo


class iCalculate():

    def __init__(self, confPath, logPath):
        self.conf_path = confPath
        self.log_path = logPath
        config = ConfigParser.ConfigParser()
        config.read(self.conf_path+"/istock.conf")
        self.conn = pymongo.MongoClient(config.get("database", "host"), int(config.get("database", "port")))
        self.db = self.conn.get_database(config.get("database", "dbname"))
        self.colls = self.db.collection_names()
        
        self.latest_date = "0000-00-00"

        fin = open(self.log_path + "/deal.log")
        sline = fin.readline().strip()
        self.last_date = sline
        fin.close()


    def _setNum(self, collStr):
        coll = self.db.get_collection(collStr)
        try:
            results = list(coll.find().sort("date", pymongo.DESCENDING))
            curDate = results[0]['date']
        except Exception,e:
            logging.warning("Access collection of [%s] in database failed." % collStr)
            return -1

        if self.latest_date < curDate:
            self.latest_date = curDate

        if self.last_date == None or self.last_date == '':
            self.num = len(results)
            self.init = True
            logging.info("Initialize all indicators for stock of [%s] by date:[%s]" % (collStr, curDate))
        elif curDate > self.last_date:
            self.init = False
            dealCnt = 0
            for result in results:
                if result['date'] == self.last_date: break
                dealCnt += 1
            self.num = dealCnt
            logging.info("Incrementalize %d-days' indicators for stock of [%s] by date:[%s]" % (dealCnt, collStr, curDate))
        return 0


    def _oneStockIndic(self, coll):
        ma_index = ma.MA(coll, self.init)
        ma_index.calMA(5, self.num)
        ma_index.calMA(10, self.num)
        ma_index.calMA(15, self.num)
        ma_index.calMA(20, self.num)
        ma_index.calMA(25, self.num)
        ma_index.calMA(30, self.num)
        ma_index.calMA(60, self.num)

        kdj_index  = kdj.KDJ(coll, self.init)
        kdj_index.calKDJ(self.num)

        macd_index = macd.MACD(coll, self.init)
        macd_index.calMACD(self.num)

        boll_index = boll.BOLL(coll, self.init)
        boll_index.calBOLL(self.num)



    def calIndicators(self):
        for collStr in self.colls:
            if len(collStr)>6 and collStr[0:5] != "stock": continue
            ret = self._setNum(collStr)
            if ret == -1 or self.num == 0: continue
            self._oneStockIndic(self.db.get_collection(collStr))

        fout = open(self.log_path + "/deal.log", 'w')
        fout.write("%s\n" % self.latest_date)
        fout.close()
        self.conn.close()




if __name__ == "__main__":
    ical = iCalculate("../conf", "../log")
    ical.calIndicators()



