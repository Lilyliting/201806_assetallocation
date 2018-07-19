# -*- coding: utf-8 -*-
"""
Created on Mon Jul  2 17:36:27 2018

@author: Administrator
"""

import time
import datetime
import copy
import pandas as pd
import numpy as np
import pymysql
import matplotlib.pyplot as plt
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

# read allocations
allocations = pd.read_csv(r".\Data\allocation_index.csv")
allocations = allocations.set_index('code')

# Read index close prices
totaldata = pd.read_csv(r'.\Data\Assetindexnav.csv')
totaldata.columns = ['Date','885009.WI','H11001.CSI','000300.SH','000905.SH','HSI.HI']
totaldata = totaldata.set_index('Date')
#totaldata.index = pd.to_datetime(totaldata.index)
#assetpct = (totaldata-totaldata.shift(1))/totaldata.shift(1)  
#assetpct = assetpct.dropna()
totaldata.index = [datetime.datetime.strptime(x,'%Y/%m/%d').strftime('%Y%m%d') for x in totaldata.index]
totaldata = totaldata.loc[str(min(allocations['date'])):]

# asset class
assetClass = {'A':('885009.WI'), 
              'B':('H11001.CSI'), 
              'C':('000300.SH'), 
              'D':('000905.SH'), 
              'E':('HSI.HI')}

ptfcodes = []
for r in range(1,11):
    ptfcodes.append('zh%02d%d' % (r,1))
        
bmcodes = []
for r in range(1,11):
    bmcodes.append('bm%02d' % (r))

totaldf = pd.DataFrame(np.zeros((len(totaldata.index),len(ptfcodes+bmcodes))),
                       columns=ptfcodes+bmcodes,index = totaldata.index)
totaldf.iloc[0,:-10]=1

for ii in ptfcodes: #ptfcodes['code']
#    ii = ptfcodes[0]
    newshiftdf = allocations.loc[ii]
#    newshiftdf = shiftdf.loc[[0]]
#    newshiftdf.iloc[0,0] = '20180101'
#    newshiftdf = newshiftdf.append(shiftdf)
    
    
    dateseq = [str(x) for x in newshiftdf['date']]
    nowDate = datetime.datetime.now().strftime('%Y%m%d')
    if dateseq[-1] != nowDate:
        dateseq.append(nowDate)
    
    for jj in range(0,len(dateseq)-1):
        datel = dateseq[jj]
        date = dateseq[jj+1]

        fundstr = newshiftdf['fundlist'].values[jj]
        fundlist = fundstr.split('/')
        for kk in fundlist:
#            kk=fundlist[0]
            fname,fpct=kk.split('-')
            if fpct==0:
                next
            fnavkk = totaldata[assetClass[fname]].loc[datel:date]
            fnavadd = totaldf[ii].loc[datel]*fnavkk/fnavkk[datel]*float(fpct)/100
            fnavadd.iloc[0]=0
            totaldf[ii].loc[datel:date] += fnavadd
            
    print("Portfolio'" + ii + "'completed")

#totaldf.to_excel('backtest20180625.xlsx')

#%%
L1 = [100,90,80,70,60,50,40,30,20,10]

bmdata = totaldata[['H11001.CSI','000300.SH']]

for ii in range(0,len(bmcodes)):
#    ii=0
#    print(ii)
    code = bmcodes[ii]
    totaldf[code] = bmdata['H11001.CSI']*L1[ii]/100/bmdata['H11001.CSI'].iloc[0] \
    + bmdata['000300.SH']*(100-L1[ii])/100/bmdata['000300.SH'].iloc[0]
    print("Bnechmark'" + code + "'completed")

totaldf.index = pd.to_datetime(totaldf.index)
totaldf.to_excel('backtest_index.xlsx')
