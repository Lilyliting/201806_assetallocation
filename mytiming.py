# -*- coding: utf-8 -*-
"""
Created on Thu Jun 14 09:41:59 2018

@author: Administrator
"""


import time
import datetime
import pandas as pd
import numpy as np
import pymysql
import os.path
# initial settings

#db = pymysql.connect("192.168.1.198","irichuser_dev","gxfc@20180315","cjopdb_dev" , charset='utf8')
db = pymysql.connect("127.0.0.1","root","0000","try", charset='utf8')

#    now = datetime.datetime.now()
#    nowDate = now.strftime('%Y%m%d')

#%% 

'''gftd timing'''
#买入启动函数
def __startbuy__(df,signalist,n1,n2,n3):#n1为模型买入启动或卖出启动形成时的价格比较滞后期数
                    #n2为模型买入启动或卖出启动形态形成的价格关系单向连续个数
#    global asset
    udb=0
    for i in range(n1,len(df.index)):
        if df.close[i]<df.close[i-n1]:
            udb=udb-1
            if udb==-n2:
                __countbuy__(df,signalist,i,n3) #调用买入计数函数
        else:
            udb=0
    return
#买入计数函数
def __countbuy__(df,signalist,i,n3):
#    global asset
#    global signalist
    count=1
    m=i+1
    for j in range(i+2,len(df.index)):
        if ((df.close[j]>=df.high[j-2]) and (df.high[j]>df.high[j-1]) and (df.close[j]>df.close[m])):
            count=count+1
            m=j
            if count==n3:
                signalist[j]=1       
                break
    return
#卖出启动函数
def __startsell__(df,signalist,n1,n2,n3):
#    global df
#    global asset
    uds=0
    for i in range(n1,len(df.index)):
        if df.close[i]>df.close[i-n1]:
            uds=uds+1
            if uds==n2:
                __countsell__(df,signalist,i,n3)
        else:
            uds=0
    return
#卖出计数函数
def __countsell__(df,signalist,i,n3):
#    global df
#    global asset
#    global signalist
    count=1
    m=i+1
    for j in range(i+2,len(df.index)):
        if ((df.close[j]<=df.low[j-2]) and (df.low[j]<df.low[j-1]) and (df.close[j]<df.close[m])):
            count=count+1
            m=j
            if count==n3:
                signalist[j]=0
                break
    return

'''dipin timing'''
def __dipintiming__(asset,totaldata):
    # 设定不同标的对应参数
    ind = {'E':[200,20,20,2],'F':[40,20,20,2]}
    basePram,kPram,bollPram,ratio = ind[asset]
    #  计算收盘价basePram日均线
    aClose = totaldata.rolling(window=basePram).mean()
    #  计算均线斜率
    akClose = aClose.rolling(window=2).apply(lambda x: x[-1]-x[0])
    # 计算均线斜率kPram日均线
    akaClose = akClose.rolling(window=kPram).mean()
    # 计算均线斜率20日均线bollPram日均值
    akaaClose = akaClose.rolling(window=bollPram).mean()
    akaaClose = akaaClose.dropna()
    dates = list(akaaClose.index)
    # 计算均线斜率20日均线bollPram日标准差
    akasClose = akaClose.rolling(window=bollPram).std()
    akasClose = akasClose.dropna()
    keep = pd.DataFrame(index=dates,columns=['sig'])
    ktype = 0
    for date in dates:
        if ktype==0:
            if akaClose.loc[date]>akaaClose.loc[date]+ratio*akasClose.loc[date]: 
                ktype=1
        elif ktype==1:
            if akaClose.loc[date]<akaaClose.loc[date]+ratio*akasClose.loc[date]: 
                ktype=0
        keep.loc[date] = ktype
    return(keep)

def getsignal(asset):
    dbwind = pymysql.connect(host="rm-2zey1z6px42nits51lo.mysql.rds.aliyuncs.com", 
                             user="fanzhuoidc", passwd="Fan.z@2018",db='wind',port=3306,charset='utf8')
    assetindex = {'C':('000300.SH','aindexeodprices'),
                  'D':('000905.SH','aindexeodprices'),
                  'E':('000071.OF')}
    if asset in ['C','D']:
#        trueStaDate = datetime.datetime.now() + datetime.timedelta(-2750)
#        trueStaDate = trueStaDate.strftime('%Y%m%d')
        trueStaDate = '20060101'
        totaldata = pd.read_sql("select S_DQ_CLOSE,S_DQ_OPEN,S_DQ_HIGH,S_DQ_LOW,TRADE_DT from %s where \
                              S_INFO_WINDCODE='%s' and TRADE_DT>='%s' \
                              order by TRADE_DT " % 
                              (assetindex[asset][1],assetindex[asset][0],trueStaDate), dbwind)
        totaldata = totaldata.set_index('TRADE_DT')
        totaldata.rename(columns={'S_DQ_CLOSE':'close','S_DQ_OPEN':'open','S_DQ_HIGH':'high','S_DQ_LOW':'low'},inplace=True)
        signalist = [np.nan]*len(totaldata.index)
        __startbuy__(totaldata,signalist,4,4,4)
        __startsell__(totaldata,signalist,5,5,5)
        sigdf = pd.DataFrame(signalist,index=totaldata.index,columns=['sig']).fillna(method='pad')
    elif asset in ['E']:
        if os.path.exists(r'.\Data\Assetindexnav.csv'):
            totaldata = pd.read_csv(r'.\Data\Assetindexnav.csv')
            totaldata.columns = ['Date','885009.WI','H11001.CSI','000300.SH','399005.SZ','HSI.HI']
            totaldata = totaldata.set_index('Date')
            totaldata.index = [datetime.datetime.strptime(x,'%Y/%m/%d').strftime('%Y%m%d') for x in totaldata.index]
            totaldata = totaldata['HSI.HI']
        else:
            trueStaDate = '20110101'
            totaldata = pd.read_sql("select PRICE_DATE,F_NAV_ADJUSTED from chinamutualfundnav where \
                                  F_INFO_WINDCODE ='%s' and PRICE_DATE>='%s' \
                                  order by PRICE_DATE " % 
                                  (assetindex[asset],trueStaDate), dbwind)
            totaldata = totaldata.set_index('PRICE_DATE')
            totaldata.rename(columns={'F_NAV_ADJUSTED':'close'},inplace=True)
        sigdf = __dipintiming__(asset,totaldata)
    return sigdf

#%% timing for portfolios
def timingsave(db):
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    for ii in ('C','D','E'):
        sigdf = getsignal(ii)#.dropna()
        sigdf.iloc[0] = 0
        sigdf = sigdf.fillna(method='pad')
        cursor = db.cursor()
        
        datatuple = (sigdf.index[1],ii,sigdf['sig'].iloc[0],nowTime,nowTime)
        sql = "INSERT INTO `stg_timing`(\
                 `create_date`, `class`, `timing_signal`, `create_time`, `update_time`) \
                select '%s', '%s', '%d', '%s', '%s' from dual " % datatuple + "where \
                not exists (select * from stg_timing where `create_date`='%s'\
                and `class` = '%s')" % datatuple[:2]
        cursor.execute(sql)
        for jj in range(1,len(sigdf.index)-1):
            sigl = sigdf['sig'].iloc[jj-1]
            sig = sigdf['sig'].iloc[jj]
            if sigl != sig:
                datatuple = (sigdf.index[jj+1],ii,sig,nowTime,nowTime)
                sql = "INSERT INTO `stg_timing`(\
                 `create_date`, `class`, `timing_signal`, `create_time`, `update_time`) \
                select '%s', '%s', '%d', '%s', '%s' from dual " % datatuple + "where \
                not exists (select * from stg_timing where `create_date`='%s'\
                and `class` = '%s')" % datatuple[:2]
                cursor.execute(sql)
        db.commit()

#%% timing for periodly invest

def pinvestsave(db):
    fund = {1:'110030.OF',
            0:'110006.OF'}
    A = getsignal('C')
    cursor = db.cursor()
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    
    datatuple = (A.index[1],fund[int(A['sig'].iloc[0])],'择时信号调仓',nowTime,nowTime)
    sql = "INSERT INTO `stg_aip_fund_info`(\
     `create_date`, `fund_code`, `change_reason`, `create_time`, `update_time`) \
    VALUES ('%s', '%s', '%s', '%s', '%s')" % datatuple
    cursor.execute(sql)
    for ii in range(1,len(A.index)-1):
    #    cursor = db.cursor()
    #    print(ii)
        sigl = A['sig'].iloc[ii-1]
        sig = A['sig'].iloc[ii]
        if sigl != sig:
            fundname = fund[int(sig)]
            datatuple = (A.index[ii+1],fundname,'择时信号调仓',nowTime,nowTime)
            sql = "INSERT INTO `stg_aip_fund_info`(\
             `create_date`, `fund_code`, `change_reason`, `create_time`, `update_time`) \
            select '%s', '%s', '%s', '%s', '%s' from dual " % datatuple + "where \
            not exists (select * from stg_portfolio_info where `create_date`='%s'\
            and `code` = '%s')" % datatuple[:2]
            cursor.execute(sql)
    db.commit()

if __name__=='__main__':
    timingsave(db)

