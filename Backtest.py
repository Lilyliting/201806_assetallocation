# -*- coding: utf-8 -*-
"""
Created on Fri Jun 15 16:11:49 2018

@author: Administrator
"""

import time
import datetime
import pandas as pd
import numpy as np
import pymysql
import os
filedir = r'C:\Users\Administrator\Desktop\智能组合'
os.chdir(filedir)

#%% initial settings
#db = pymysql.connect("192.168.1.198","irichuser_dev","gxfc@20180315","cjopdb_dev" , charset='utf8')
db = pymysql.connect("127.0.0.1","root","0000","try" , charset='utf8')
dbwind = pymysql.connect(host="rm-2zey1z6px42nits51lo.mysql.rds.aliyuncs.com", user="fanzhuoidc", passwd="Fan.z@2018",db='wind',port=3306,charset='utf8')


#%% 
def safePortfolioNav(db,dbwind):
    pass
    
# 读取组合配置
allocations = pd.read_sql("SELECT `create_date`,`code`,`fund_list` FROM stg_portfolio_info;",db)
allocations = allocations.set_index('code')

# 读取所有备选基金
fundlist = pd.read_sql("SELECT `fund_code` FROM stg_fund_info;",db)

# 读取所有组合代码
ptfcodes = list(np.unique(allocations.index))
bmcodes = []
for r in range(1,11):
    bmcodes.append('bm%02d' % (r)) #基准代码

# startdate & enddate
start = min(allocations['create_date'])
end = (pd.datetime.now().strftime(format="%Y%m%d"))
sql = "select f_info_windcode,price_date,f_nav_adjusted from chinamutualfundnav \
               where price_date>='"+start+"' and price_date<'"+end+"'"
if len(fundlist):
    sql += " and f_info_windcode in ("
    for f in fundlist["fund_code"]: 
        sql += "'"+f+"',"
    sql = sql[:-1]+")"
ajN = pd.read_sql(sql,dbwind)
#ajN["price_date"] = pd.to_datetime(ajN["price_date"],format="%Y%m%d")                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                               
ajN = ajN.pivot_table(index="price_date",columns="f_info_windcode",values="f_nav_adjusted")
FundNav = ajN.fillna(method="pad") # all fund nav


# standard date sequence
dateSeq = pd.read_sql("select TRADE_DT from aindexeodprices\
                      where S_INFO_WINDCODE ='000300.SH' and TRADE_DT>='%s'\
                      and TRADE_DT<'%s' order by TRADE_DT" % (start,end),dbwind)

PtfNav = pd.DataFrame(np.zeros((len(dateSeq),len(ptfcodes)+len(bmcodes)+1)),\
                      columns = ["date"]+ptfcodes+bmcodes)
PtfNav["date"] = dateSeq
PtfNav = PtfNav.set_index('date')

# position change date
posCgDate = list(np.unique(allocations['create_date']))
posCgDate.append(dateSeq['TRADE_DT'].iloc[-1])
for code in ptfcodes:
    allocationii = allocations.loc[code]
    for ii in range(len(posCgDate)-1):
        date = posCgDate[ii]
        daten = posCgDate[ii+1]
        
        buyOrders = allocationii[allocationii['create_date']==date] # 本期买单
        buystr = buyOrders.loc[code,'fund_list'].split('/')

#        scodeii = buyOrders['scode'] # 本期买入标的
#        posii = buyOrders['pos'] # 标的仓位
#        FundNavii = FundNav[scodeii].loc[PtfNav.loc[date:daten].index] # 标的对应收盘价
        startnav = PtfNav[code].loc[date]
        for jj in range(len(buystr)):
            fname,fpct = buystr[jj].split('-')
            fpct = float(fpct)
            if fpct == 0: continue
            FundNavii = FundNav[fname].loc[PtfNav.loc[date:daten].index] # 标的对应收盘价
            ptfnavadd = FundNavii.iloc[:]/FundNavii.iloc[0]*fpct/100
            if startnav==0:
                PtfNav[code].loc[date:daten] += ptfnavadd
            else:
                ptfnavadd.iloc[0]=0
                PtfNav[code].loc[date:daten] += startnav*ptfnavadd


L1 = [100,90,80,70,60,50,40,30,20,10]

idxdata1 = pd.read_sql("select TRADE_DT,S_DQ_CLOSE from aindexeodprices where \
                      S_INFO_WINDCODE in ('H11001.CSI') and TRADE_DT>='%s' \
                      and TRADE_DT <'%s' order by TRADE_DT" %(start,end), dbwind)
idxdata2 = pd.read_sql("select TRADE_DT,S_DQ_CLOSE from aindexeodprices where \
                      S_INFO_WINDCODE in ('000300.SH') and TRADE_DT>='%s' \
                      and TRADE_DT <'%s' order by TRADE_DT" %(start,end), dbwind)
idxdata1.columns = ['TRADE_DT','zzbond']
idxdata2.columns = ['TRADE_DT','hs300']
totaldata = pd.merge(idxdata1,idxdata2)
totaldata.iloc[:,1] = totaldata.iloc[:,1]/totaldata.iloc[0,1]
totaldata.iloc[:,2] = totaldata.iloc[:,2]/totaldata.iloc[0,2]
totaldata = totaldata.set_index('TRADE_DT')

for ii in range(len(bmcodes)):
#    print(ii)
    code = bmcodes[ii]
    temp = totaldata.iloc[:,0]*L1[ii]/100 + totaldata.iloc[:,1]*(100-L1[ii])/100
    PtfNav[code] = temp.iloc[:]
    print("Bnechmark'" + code + "'completed")

PtfNav.index = pd.to_datetime(PtfNav.index)
PtfNav.to_excel(r".\backtest.xlsx")
























