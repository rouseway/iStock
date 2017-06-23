#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-17

import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo


class bollStrategy():

    def __init__(self):
        self.dateJC = []


    def _getBOLL(self, quotes):
        bolls = []
        for quote in quotes:
            bolls.append([quote['date'], quote['BOLL_UP'], quote['BOLL_MID'], quote['BOLL_LOW'], quote['close_price']])
        return bolls


    def _checkJinCha(self, bolls, stockID):
        dates = map(lambda x: x[0], bolls)
        up = map(lambda x: x[1], bolls)
        mid = map(lambda x: x[2], bolls)
        prices = map(lambda x: x[4], bolls)
        
        belowMid = False
        idx = len(dates) - 1
        while idx >= 0:
            if prices[idx]<mid[idx]: belowMid = True
            if belowMid == True and prices[idx]>=mid[idx]:
                self.dateJC.append(dates[idx])
                belowMid = False
                logging.info("Stock of [%s] occurs a BOLL-JinCha at date:[%s]" % (stockID, dates[idx]))
            idx -= 1


    def runStrategy(self, quotes, stockID):
        bolls = self._getBOLL(quotes)
        self._checkJinCha(bolls, stockID)



if __name__ == "__main__":
    conn = pymongo.MongoClient("127.0.0.1", 10001)
    db = conn.istock
    coll = db.stock_601398
    results = list(coll.find({"date":{"$gte":"2015-12-10", "$lte":"2016-01-11"}}))
    bollStrg = bollStrategy()
    bollStrg.runStrategy(results, "stock_601398")
    conn.close()
