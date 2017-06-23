#!/usr/bin/env python
# -*- coding: utf-8 -*-
# author: raosiwei
# date: 2016-01-23

import sys
sys.path.append("./src")
import iData,iCalculate,iAnalysis

idata = iData.iData("./conf", "./log", "./data")
idata.dataProcessing()

ical = iCalculate.iCalculate("./conf", "./log")
ical.calIndicators()

ianaly = iAnalysis.iAnalysis("./conf", "./log")
ianaly.runAnalysis()

