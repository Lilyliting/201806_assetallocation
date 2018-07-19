# -*- coding: utf-8 -*-
"""
Created on Fri Jun 22 08:40:28 2018

@author: Administrator
"""

import time
import datetime
import copy
import pandas as pd
import numpy as np
from cvxopt import matrix,solvers
import pymysql
import os
filedir = r'C:\Users\Administrator\Desktop\智能组合' # 工作目录
os.chdir(filedir)

import mytiming

#%% initial settings

# database
#db = pymysql.connect("192.168.1.198","irichuser_dev","gxfc@20180315","cjopdb_dev" , charset='utf8')
db = pymysql.connect("127.0.0.1","root","0000","try" , charset='utf8')
dbwind = pymysql.connect(host="rm-2zey1z6px42nits51lo.mysql.rds.aliyuncs.com", user="fanzhuoidc", passwd="Fan.z@2018",db='wind',port=3306,charset='utf8')

# all ptf codes
ptfCode = []
for r in range(1,11):
    ptfCode.append('zh%02d%d' % (r,1))
            
# asset index
assetindex = {'A':('885009.WI','AIndexWindIndustriesEOD'),
              'B':('H11001.CSI','aindexeodprices'),
              'C':('000300.SH','aindexeodprices'),
              'D':('000905.SH','aindexeodprices'),
              'E':('000071.OF','HSI.HI')}

# V1 ------------
riskClassList = {'R01':(['A','B','C'],(1,2,1),(50,40,10)), 
                 'R02':(['A','B','C'],(1,2,2),(50,30,20)),
                 'R03':(['A','B','C','D'],(1,2,2,1),(40,30,20,10)),
                 'R04':(['A','B','C','D'],(1,2,2,1),(30,30,20,20)),
                 'R05':(['A','B','C','D','E'],(1,2,2,1,1),(30,20,20,20,10)),
                 'R06':(['A','B','C','D','E'],(1,2,2,1,1),(20,20,30,20,10)),
                 'R07':(['A','B','C','D','E'],(1,1,2,2,1),(10,20,30,30,10)),
                 'R08':(['A','B','C','D','E'],(1,1,2,2,1),(10,10,30,40,10)),
                 'R09':(['B','C','D','E'],(1,2,2,1),(10,40,40,10)),
                 'R10':(['B','C','D','E'],(1,2,2,1),(5,40,45,10))}

#%% Opening ceremony
print('----Start!---')
now = datetime.datetime.now()# + datetime.timedelta(-250)
nowDate = now.strftime('%Y%m%d')
print(now)

#%% Timing 
#-----------------------------Everyday-----------------------------------------
mytiming.timingsave(db)
print('Timing completed')

#%% Asset allocation 
#-----------------------------Per month?(1st)----------------------------------

# Read index close prices
totaldata = pd.read_csv(r'.\Data\Assetindexnav.csv')
totaldata.columns = ['Date','885009.WI','H11001.CSI','000300.SH','000905.SH','HSI.HI']
totaldata = totaldata.set_index('Date')
totaldata.index = pd.to_datetime(totaldata.index)
assetpct = (totaldata-totaldata.shift(1))/totaldata.shift(1)  
assetpct = assetpct.dropna()
    
# Prepare data
dateseq = pd.read_sql("select TRADE_DT from aindexeodprices\
                                where S_INFO_WINDCODE ='000300.SH' and TRADE_DT>='20120101'\
                                and TRADE_DT<='%s' order by TRADE_DT;" % nowDate, dbwind)
dateseq['year'] = [time.strptime(x,'%Y%m%d').tm_year for x in dateseq['TRADE_DT']]
dateseq['mon'] = [time.strptime(x,'%Y%m%d').tm_mon for x in dateseq['TRADE_DT']]
dateseq = dateseq[dateseq['mon'] % 3 ==2]

shiftDate = dateseq.groupby(['year','mon']).first()

shiftlist = shiftDate['TRADE_DT'].values.tolist()
resultdf = pd.DataFrame(index=range(0,len(shiftlist)*len(ptfCode)),columns = ['date','code','fundlist'])
kk = 0
tilt = 1

# Risk aversion
#        delta = abs(assetpct['000300.SH']-assetpct['885009.WI']).mean()*250/(assetpct['000300.SH'].std()*(250**0.5))**2
delta = 3

for tradedate in shiftDate['TRADE_DT']:
#    tradedate = shiftDate['TRADE_DT'][0]
    tradedatestr = datetime.datetime.strptime(tradedate,'%Y%m%d')
    tracestr = tradedatestr + datetime.timedelta(days = -365*5)
#    tracestr = tracestr.strftime('%Y%m%d')
    
    # historical return for ptf assets
    assetpct5y = assetpct.loc[tracestr:tradedatestr]
    assetReturn = assetpct5y[-120:].mean()*240 # historical return

    # Timing signals
    timingdata = pd.read_sql("select a.create_date,a.class,a.timing_signal \
                      from stg_timing a where a.create_date = \
                      (select max(create_date) from stg_timing \
                      where a.class = class and create_date <= %s);" % tradedate,db)
    timingdata = timingdata.set_index('class')
    timingdata.columns = ['d','sig']
    
    # Asset: indexcode, minmaxpos, timing, risk
    assetClass = {'A':('885009.WI',np.nan), 
                  'B':('H11001.CSI',np.nan), 
                  'C':('000300.SH',int(timingdata.loc['C'][1])), 
                  'D':('000905.SH',int(timingdata.loc['D'][1])), 
                  'E':('HSI.HI',int(timingdata.loc['E'][1]))}
    
    for code in ptfCode: 
#        code = ptfCode[15]
        riskcode = 'R'+ code[2:4]
        p1 = int(code[4])
        
        riskClasses = copy.deepcopy(riskClassList[riskcode][0])
        

        
        # Asset included
        assets = []
        timingsig = []
        poslimit = []
        for ii in range(0,len(riskClasses)):
            rclass = riskClasses[ii]
            assetii,timingii = assetClass[rclass]
            assets.append(assetii)
            assetret = assetReturn[assetii]
            if assetret>0:
                timingsig.append(assetret*(1+tilt)**(timingii*2-1))
            else:
                timingsig.append(assetret*(1+tilt)**(-timingii*2+1))
            if rclass in ['A','B']:
                poslimit.append(1)
            else:
                poslimit.append((riskClassList[riskcode][2][ii]*1.3)/100)
        timingsig = [x for x in timingsig if ~np.isnan(x)]
        n = len(assets)
#        assetReturn = assetReturn[assets]
        
        
        # historical covirance for ptf assets
        assetCov = abs(assetpct[assets][-240:].cov().values*np.sqrt(240))
        
        # Implied expected equilibrium returns
        wmarket = np.matrix([x/100 for x in riskClassList[riskcode][2]])
        PI = matrix(delta*np.dot(assetCov,wmarket.T))
#        PI = matrix(assetReturn)
        
    
        # Prepare coefficients
        P = matrix(assetCov)
        #    q = matrix(0.0,(n,1))
        A = matrix(np.ones((1, n)))
        b = matrix([1.0])
        
        G = np.concatenate((-np.eye(n),np.eye(n)),axis=0)
        G = matrix(G)
        h = matrix([0]*n+poslimit,(n*2,1),'d')
        
        LC = 0.5 # level of confidence
        Pview = matrix(0.0,(len(timingsig),n))
        for ii in range(1,len(timingsig)+1):
            Pview[(-ii,-ii)] = 1
        Scalar = 0.5
        
        SIGMA = P
        OMEGA = Scalar*np.dot(np.dot(Pview,SIGMA),Pview.T)
        #    CF = Scalar/(1/LC)
        #    OMEGA = matrix(np.eye(len(timingsig))*CF/LC)
        Q = matrix(timingsig)
        
        
        #E_bl=pi+np.dot(t*np.dot(np.dot(sigma,P.T),np.linalg.inv(t*np.dot(np.dot(P,sigma),P.T)+omega)),(Q-np.dot(P,pi)))
        E_bl = np.dot(np.linalg.inv(np.linalg.inv(Scalar*SIGMA)+
                                    np.dot(np.dot(Pview.T,np.linalg.inv(OMEGA)),Pview)),
            (np.dot(np.linalg.inv(Scalar*SIGMA),PI)+
             np.dot(np.dot(Pview.T,np.linalg.inv(OMEGA)),Q)))
        
        #    w = np.dot(np.linalg.inv(delta*SIGMA),E_bl)
        
        q=matrix(E_bl*(-1/delta))
        
        solvers.options['show_progress'] = False
        sol = solvers.qp(P, q, G, h, A, b)
        w_hat = list(sol['x'])
        w_hat = [100*round(x,3) for x in w_hat]
        w_hat[0] += 100 - sum(w_hat)
        w_hat = [abs(round(x,2)) for x in w_hat]
        

        
        fundtext = str()
        fundtext2 = str()
        for ii in range(0,len(riskClasses)):
            assetc = riskClasses[ii]
            assetn = 1
            if assetc in ['C','D','E']:
                assetc += str(p1)
#            fundtext += funds[jj][1] +' (' + funds[jj][0] + ') ' + str(w_hat[0]) + '%, '
            fundtext2 += riskClasses[ii] + '-' + str(w_hat[ii]) + '/'
#        fundtext = fundtext.rstrip(', ')
        fundtext2 = fundtext2.rstrip('/')
        
#        nowDate = datetime.datetime.now().strftime('%Y%m%d')
#        nowDate = (datetime.datetime.now() + datetime.timedelta(-100)).strftime('%Y%m%d')
        nowTime = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        resultdf.loc[kk]=[tradedate,code,fundtext2]
        kk = kk+1
        
    print(tradedate + ' allocation completed')

resultdf.to_csv(r".\Data\allocation_index.csv",index = False) # 如果要存csv，需要先在文件开头修改工作目录

print('All asset allocation completed')
db.close()
