# -*- coding: utf-8 -*-
"""
Created on Tue May 29 16:19:55 2018

@author: Administrator
"""


import time
import datetime
import pymysql
import pandas as pd
import numpy as np

from WindPy import w
w.start()

filedir=r'C:\Users\Administrator\Desktop\智能组合'
dbwind = pymysql.connect(host="rm-2zey1z6px42nits51lo.mysql.rds.aliyuncs.com", user="fanzhuoidc", passwd="Fan.z@2018",db='wind',port=3306,charset='utf8')
db = pymysql.connect("127.0.0.1","root","0000","try" , charset='utf8')

#%% 用WindAPI取得指数数据

now = datetime.datetime.now()
nowDate = now.strftime('%Y%m%d')

asset = w.wsd("885009.WI,H11001.CSI,000300.SH,399005.SZ,HSI.HI", "close", "2006-01-01", now, "Fill=Previous;Currency=CNY")
assetdf = pd.DataFrame(asset.Data).T
assetdf.index = asset.Times
assetdf.columns = asset.Codes
assetdf.to_csv(filedir+r"\Data\Assetindexnav.csv")


#%% 存放备选基金
ptfinfo = pd.read_excel(filedir+r'\Data\fundselect.xlsx',sheetname='Sheet1')
#ptfinfo = ptfinfo.set_index('类别')
cursor = db.cursor()
#cursor.execute("truncate stg_portfolio_info;")
#db.commit()

for ii in ptfinfo.index:
#    print(ii)
    pt = ptfinfo.loc[ii].tolist()
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    datatuple = (pt[0],pt[1],pt[2],nowTime,nowTime)
    sql = "INSERT INTO `stg_fund_info`(\
     `class`, `fund_code`, `fund_name`, `create_time`, `update_time`) \
    VALUES ('%s', '%s', '%s', '%s', '%s')" % datatuple
    cursor.execute(sql)
db.commit()

#%% 关联组合和基准
# all portfolio codes
ptfCode = []
for r in range(1,11):
    for p1 in [1,2]:
            ptfCode.append('zh%02d%d' % (r,p1))
                    
phrase1 = {'01':'组合%s投资风格稳健','02':'组合%s投资风格稳健','03':'组合%s投资风格稳健',
           '04':'组合%s投资风格平衡','05':'组合%s投资风格平衡','06':'组合%s投资风格平衡',
           '07':'组合%s投资风格平衡',
           '08':'组合%s投资风格积极','09':'组合%s投资风格积极','10':'组合%s投资风格积极'}

phrase2 = {'1':'，通过均衡配置于最具潜力的大类资产把握市场上涨机会',
           '2':'，通过广泛配置于多项大类资产获取最优收益风险比'}
phrase3 = {'1':'，适当配置优质指数基金获取平稳市场收益。',
           '2':'，优中选的基金产品有条件战胜市场平均水获取超额收益。'}
phrase4 = {'1':'，并进行主动市场择时积极应对市场变化。',
           '2':'，并通过适当进行资产调整及再平衡实施风险控制。'}
cursor = db.cursor()
for ii in ptfCode:
    riskcode = 'R'+ ii[2:4]
    p1 = ii[4]
    
    detail = phrase1[ii[2:4]] % ii + phrase3[p1]
    nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    datatuple = (ii,'bm'+ii[2:4],1000,int(ii[2:4]),detail,nowTime,nowTime)
    sql = "INSERT INTO `stg_portfolio_detail_info`(\
     `code`, `refer_base`, `buy_amount_min`, `risk_level`, `description`, `create_time`, `update_time`) \
    VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s')" % datatuple
    cursor.execute(sql)
db.commit()
    


