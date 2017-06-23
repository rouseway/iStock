#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-17


import ConfigParser,datetime
import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
from strategy import maStrategy,kdjStrategy,macdStrategy,bollStrategy
import pymongo


class iAnalysis():

    def __init__(self, confPath, logPath):
        self.conf_path = confPath
        self.log_path = logPath
        config = ConfigParser.ConfigParser()
        config.read(self.conf_path+"/istock.conf")
        self.conn = pymongo.MongoClient(config.get("database", "host"), int(config.get("database", "port")))
        self.db = self.conn.get_database(config.get("database", "dbname"))
        self.colls = self.db.collection_names()
        
        fin = open(self.log_path+"/deal.log")
        sline = fin.readline().strip()
        self.last_date = sline
        fin.close()


    def _getQuotation(self, collStr):
        results = []
        try:
            curDate = datetime.datetime.strptime(self.last_date, "%Y-%m-%d")
        except Exception,e:
            logging.warning("Last deal date infomation is wrong.")
            return results

        self.long_date = (curDate + datetime.timedelta(days=-13)).strftime("%Y-%m-%d")
        self.short_date = (curDate + datetime.timedelta(days=-2)).strftime("%Y-%m-%d")
        coll = self.db.get_collection(collStr)
        try:
            results = list(coll.find({"date":{"$gte":self.long_date, "$lte":self.last_date}}).sort("date", pymongo.DESCENDING))
        except Exception,e:
            logging.warning("Search database failed for quotation from [%s] to [%s]" % (self.long_date, self.last_date))
        return results


    def _checkDates(self, quotes):
        lastDate = quotes[0]['date']
        if lastDate == self.last_date:
            return True
        else:
            return False


    def runAnalysis(self):
        maStrg = maStrategy.maStrategy()
        kdjStrg = kdjStrategy.kdjStrategy()
        macdStrg = macdStrategy.macdStrategy()
        bollStrg = bollStrategy.bollStrategy()

        for collStr in self.colls:
            if len(collStr)<5 or collStr[0:5] != "stock": continue
            quotes = self._getQuotation(collStr)
            if len(quotes) == 0 or not self._checkDates(quotes): continue
            maStrg.runStrategy(quotes, collStr)
            kdjStrg.runStrategy(quotes, collStr)
            macdStrg.runStrategy(quotes, collStr)
            bollStrg.runStrategy(quotes, collStr)

        self.conn.close()



if __name__ == "__main__":
    anly = iAnalysis("../conf", "../log")
    anly.runAnalysis()
