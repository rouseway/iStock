#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-17

import logging
logging.basicConfig(level=logging.INFO,format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s')
import pymongo


class maStrategy():

    def __init__(self):
        self.dateJSJ = []
        self.dateDTPL = []
        self.dateLXZS = []


    def _getMAs(self, quotes):
        mas = []
        for quote in quotes:
            mas.append([quote['date'], quote['MA_5'], quote['MA_10'], quote['MA_15'], quote['MA_20'], \
                    quote['MA_25'], quote['MA_30'], quote['MA_60'], quote['close_price']])
        return mas


    def _checkJinSanJiao(self, mas, stockID):
        dates = map(lambda x: x[0], mas)
        mas_5 = map(lambda x: x[1], mas)
        mas_10 = map(lambda x: x[2], mas)
        mas_20 = map(lambda x: x[4], mas)

        below20 = False
        idx = len(dates) - 1
        while idx >= 0:
            if mas_5[idx]>mas_10[idx] and mas_5[idx]>mas_20[idx] and mas_10[idx]<mas_20[idx]:
                below20 = True
            if below20 == True and mas_10[idx] >= mas_20[idx] and mas_5[idx] > mas_10[idx]:
                self.dateJSJ.append(dates[idx])
                below20 = False
                logging.info("Stock of [%s] occurs a MA-JinSanJiao at date:[%s]" % (stockID, dates[idx]))
            idx -= 1


    def _checkDuoTouPaiLie(self, mas):
        return True


    def _checkLiuXianZhiShang(self, mas, stockID):
        dates = map(lambda x: x[0], mas)
        mas_5 = map(lambda x: x[1], mas)
        mas_10 = map(lambda x: x[2], mas)
        mas_15 = map(lambda x: x[3], mas)
        mas_20 = map(lambda x: x[4], mas)
        mas_25 = map(lambda x: x[5], mas)
        mas_30 = map(lambda x: x[6], mas)
        prices = map(lambda x: x[7], mas)
        
        idx = len(dates) - 1
        while idx >= 0:
            if mas_25[idx]>mas_30[idx] and mas_20[idx]>mas_25[idx] and mas_15[idx]>mas_20[idx] \
                    and mas_10[idx]>mas_15[idx] and mas_5[idx]>mas_10[idx] and prices[idx]>mas_5[idx]:
                self.dateLXZS.append(dates[idx])
                logging.info("Stock of [%s] occurs a MA-LiuXianZhiShang at date:[%s]" % (stockID, dates[idx]))
            idx -= 1


    def runStrategy(self, quotes, stockID):
        mas = self._getMAs(quotes)
        self._checkJinSanJiao(mas, stockID)
        self._checkLiuXianZhiShang(mas, stockID)


if __name__ == "__main__":
    conn = pymongo.MongoClient("127.0.0.1", 10001)
    db = conn.istock
    coll = db.stock_601398
    results = list(coll.find({"date":{"$gte":"2015-09-17", "$lte":"2016-01-11"}}))
    maStrg = maStrategy()
    maStrg.runStrategy(results, "stock_601398")
    conn.close()
