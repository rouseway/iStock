#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-17

import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo


class kdjStrategy():

    def __init__(self):
        self.dateJC = []
        self.dateDBL = []


    def _getKDJ(self, quotes):
        kdjs = []
        for quote in quotes:
            kdjs.append([quote['date'], quote['KDJ_K'], quote['KDJ_D'], quote['KDJ_J'], quote['close_price']])
        return kdjs


    def _checkJinCha(self, kdjs, stockID):
        dates = map(lambda x: x[0], kdjs)
        k = map(lambda x: x[1], kdjs)
        d = map(lambda x: x[2], kdjs)
        j = map(lambda x: x[3], kdjs)

        belowD = False
        idx = len(dates) - 1
        while idx >= 0:
            if j[idx]<50 and k[idx]<50:
                if k[idx]<d[idx]: belowD = True
                if belowD == True and j[idx]>=d[idx] and k[idx]>=d[idx]:
                    self.dateJC.append(dates[idx])
                    belowD = False
                    logging.info("Stock of [%s] occurs a KDJ-JinCha at date:[%s]" % (stockID, dates[idx]))
            idx -= 1


    def runStrategy(self, quotes, stockID):
        kdjs = self._getKDJ(quotes)
        self._checkJinCha(kdjs, stockID)


        
if __name__ == "__main__":
    conn = pymongo.MongoClient("127.0.0.1", 10001)
    db = conn.istock
    coll = db.stock_601398
    results = list(coll.find({"date":{"$gte":"2015-12-10", "$lte":"2016-01-11"}}))
    kdjStrg = kdjStrategy()
    kdjStrg.runStrategy(results, "stock_601398")
    conn.close()
