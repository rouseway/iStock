#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-17

import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo


class macdStrategy():

    def __init__(self):
        self.dateJC = []
        self.dateDBL = []


    def _getMACD(self, quotes):
        macds = []
        for quote in quotes:
            macds.append([quote['date'], quote['MACD_DIF'], quote['MACD_DEA'], quote['MACD_BAR'], quote['close_price']])
        return macds


    def _checkJinCha(self, macds, stockID):
        dates = map(lambda x: x[0], macds)
        diff = map(lambda x: x[1], macds)
        dea = map(lambda x: x[2], macds)

        belowDEA = False
        idx = len(dates) - 1
        while idx >= 0:
            if diff[idx]<0 and dea[idx]<0:
                if diff[idx]<dea[idx]: belowDEA = True
                if belowDEA == True and diff[idx]>=dea[idx]:
                    self.dateJC.append(dates[idx])
                    belowDEA = False
                    logging.info("Stock of [%s] occurs a MACD-JinCha at date:[%s]" % (stockID, dates[idx]))
            idx -= 1


    def runStrategy(self, quotes, stockID):
        macds = self._getMACD(quotes)
        self._checkJinCha(macds, stockID)




if __name__ == "__main__":
    conn = pymongo.MongoClient("127.0.0.1", 10001)
    db = conn.istock
    coll = db.stock_601398
    results = list(coll.find({"date":{"$gte":"2015-12-10", "$lte":"2016-01-11"}}))
    macdStrg = macdStrategy()
    macdStrg.runStrategy(results, "stock_601398")
    conn.close()
