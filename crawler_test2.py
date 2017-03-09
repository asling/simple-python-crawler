# -*- coding: utf8 -*-
import urllib 
import urllib2
import cookielib
import time
import io
import Image
import json
import sys
import re
import os
import MySQLdb
import random

class Dav(object):
    def login(self):     
        jsondata = json.dumps(self.postdata)
        loginUrl = 'http://y.davdian.com/suppliers/user/login'
        loginReq = urllib2.Request(loginUrl,data=jsondata.encode('utf-8'),headers=self.checkcode_headers)
        cookie = cookielib.MozillaCookieJar(self.filename)
        self.opener = urllib2.build_opener(urllib2.HTTPCookieProcessor(cookie))
        result = self.opener.open(loginReq)
        cookie.save(ignore_discard=True,ignore_expires=True)
        self.cookieStr = ''
        for item in cookie:
                self.cookieStr = self.cookieStr+item.name+'='+item.value+';'
        
    def getDatas(self,start,end):
        self.endTime = int(end) / 1000

        normalUrl = 'http://y.davdian.com/suppliers/finance/line/page?pageNo=1&pageSize=10&payTimeStart='+start+'&payTimeEnd='+end+'&status=3'
        #print normalUrl
        f1 = self.opener.open(normalUrl)
        data1 = f1.read().replace("'","\"")
        #print data1
        data_json1 = json.loads(data1)
        self.pageSize = data_json1['data']['totalPages']
        print u'总共有'+str(self.pageSize)
        datas_formated = []
        settlements_formated = []
        special_formated = []
        orders_formated = []
        holdings_formated = []
        for page in range(1,self.pageSize+1):
            currentUrl = 'http://y.davdian.com/suppliers/finance/line/page?status=3&payTimeStart='+start+'&payTimeEnd='+end+'&pageNo='+str(page)+'&pageSize=10'
            #print currentUrl
            f2 = self.opener.open(currentUrl)
            data2 = f2.read().replace("'","\"")
            data_json2 = json.loads(data2)
            for dataItem in data_json2['data']['data']:
                self.removeSettlement(dataItem['financeNum'])
                settlements_formated.append(self.getSettlements(dataItem))
                self.removeThisFinanceNum(dataItem['financeNum'])
                self.removeSettleOrder(dataItem['financeNum'])
                for numItem in range(1,6):
                    print 'numItem'+str(numItem)
                    if numItem == 4:
                        for subDataItem in self.getWrapper(dataItem['financeNum'],'special',numItem):
                            special_formated.append(subDataItem)
                    elif numItem == 1:
                        for subDataItem in self.getWrapper(dataItem['financeNum'],'orders',numItem):
                            orders_formated.append(subDataItem) 
                    elif numItem == 5:
                        for subDataItem in self.getWrapper(dataItem['financeNum'],'withhold',numItem):
                            holdings_formated.append(subDataItem) 
                    else:
                        for subDataItem in self.getWrapper(dataItem['financeNum'],'details',numItem):
                            datas_formated.append(subDataItem)
                    
                for subDataItem in self.getWrapper(dataItem['financeNum'],'rebate',6):
                    datas_formated.append(subDataItem)
            print str(page)+u'页同步成功'
        
        print u'获取同步结算单'
        print u'开始导入'
        self.postOrders(orders_formated)
        self.postSettles(settlements_formated)
        self.postData(datas_formated)
        self.postData(holdings_formated)
        self.postSpecial(special_formated)
        self.writeSyncTime(self.endTime)
        print 'ok'
    
    def getWrapper(self,orderDetailId,funType,type_num):
        datas_formated = []
        pageNo = 0
        pageSize = 1
        page = 1
        result = dict()
        
        while pageNo < pageSize+1:
            if funType == 'orders':
                result = self.getOrders(orderDetailId,type_num,page)
            elif funType == 'details':
                result = self.getDetail(orderDetailId,type_num,page)
            elif funType == 'special':
                result = self.getSpecial(orderDetailId,type_num,page)
            elif funType == 'rebate':
                result = self.getRebate(orderDetailId,type_num,page)
            elif funType == 'withhold':
                result = self.getHoldings(orderDetailId,type_num,page)
            #print result
            for dataItem in result['data']:
                datas_formated.append(dataItem)
            pageNo = result['pageNo'] + 1
            pageSize = result['pageSize']
            page = pageNo
        return datas_formated
      
    def getHoldings(self,orderDetailId,type_num,pageNo=1):
        postdata = json.dumps({
            'financeNum':orderDetailId,
            'pageNo':pageNo,
            'pageSize':'20',
            'payment':'6',
            'type':type_num
        })
        #print postdata
        checkcode_headers = {
            'Accept': 'application/json, */*; q=0.01',
            'Host': 'y.davdian.com',
            'Content-Type': 'application/json',
            'Referer': 'http://y.davdian.com/suppliers/client/html/details_pages',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'X-Requested-With': 'XMLHttpRequest',
        }
        detailUrl = 'http://y.davdian.com/suppliers/finance/settleOrder/sendOrderPage'
        data_req = urllib2.Request(detailUrl,data=postdata.encode('utf-8'),headers=checkcode_headers)
        result = self.opener.open(data_req)
        data = result.read()
        result_json = json.loads(data)
        #print result_json

        pageSize = int(result_json['data']['totalPages'])
        currentPageNo = int(result_json['data']['pageNO'])
        print 'pageSize'+str(pageSize)
        print currentPageNo
        datas_formated = []
        for dataItem in result_json['data']['data']:
            datas_formated.append(self.holdingFormating(dataItem,orderDetailId,type_num))
        result = dict()
        result['data'] = datas_formated
        result['pageSize'] = pageSize
        result['pageNo'] = currentPageNo
        
        return result        
        
    def getOrders(self,orderDetailId,type_num,pageNo=1):
        postdata = json.dumps({
            'financeNum':orderDetailId,
            'pageNo':pageNo,
            'pageSize':'20',
            'payment':'6',
            'type':type_num
        })
        #print postdata
        checkcode_headers = {
            'Accept': 'application/json, */*; q=0.01',
            'Host': 'y.davdian.com',
            'Content-Type': 'application/json',
            'Referer': 'http://y.davdian.com/suppliers/client/html/details_pages',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'X-Requested-With': 'XMLHttpRequest',
        }
        detailUrl = 'http://y.davdian.com/suppliers/finance/settleOrder/sendOrderPage'
        data_req = urllib2.Request(detailUrl,data=postdata.encode('utf-8'),headers=checkcode_headers)
        result = self.opener.open(data_req)
        data = result.read()
        result_json = json.loads(data)
        #print result_json

        pageSize = int(result_json['data']['totalPages'])
        currentPageNo = int(result_json['data']['pageNO'])
        print 'pageSize'+str(pageSize)
        print currentPageNo
        datas_formated = []
        for dataItem in result_json['data']['data']:
            datas_formated.append(self.orderFormating(dataItem,orderDetailId,type_num))
        result = dict()
        result['data'] = datas_formated
        result['pageSize'] = pageSize
        result['pageNo'] = currentPageNo
        
        return result
    
    def getSpecial(self,orderDetailId,type_num,pageNo=1):
        postdata = json.dumps({
            'financeNum':orderDetailId,
            'pageNo':pageNo,
            'pageSize':'20',
            'payment':'6',
            'type':type_num
        })
        #print postdata
        checkcode_headers = {
            'Accept': 'application/json, */*; q=0.01',
            'Host': 'y.davdian.com',
            'Content-Type': 'application/json',
            'Referer': 'http://y.davdian.com/suppliers/client/html/details_pages',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'X-Requested-With': 'XMLHttpRequest',
        }
        detailUrl = 'http://y.davdian.com/suppliers/finance/settleOrder/sendOrderPage'
        data_req = urllib2.Request(detailUrl,data=postdata.encode('utf-8'),headers=checkcode_headers)
        result = self.opener.open(data_req)
        data = result.read()
        result_json = json.loads(data)
        #print result_json

        pageSize = int(result_json['data']['totalPages'])
        currentPageNo = int(result_json['data']['pageNO'])
        #print 'pageSize'+str(pageSize)
        #print currentPageNo
        datas_formated = []
        for dataItem in result_json['data']['data']:
            datas_formated.append(self.specialFormating(dataItem,orderDetailId,type_num))
        result = dict()
        result['data'] = datas_formated
        result['pageSize'] = pageSize
        result['pageNo'] = currentPageNo   
        
        return result

    def getSettlements(self,data):
        itemData = dict()
        period = data['period'].split("~")
        itemData['date_created'] = int(time.mktime(time.strptime(data['createTime'],"%Y-%m-%d %H:%M:%S")))
        itemData['s_platform_no'] = data['financeNum']
        itemData['sum'] = data['payMoney']
        itemData['date_settled'] = int(time.mktime(time.strptime(data['payTime'],"%Y-%m-%d %H:%M:%S")))
        itemData['date_started'] = int(time.mktime(time.strptime(period[0],"%Y-%m-%d")))
        itemData['date_ended'] = int(time.mktime(time.strptime(period[1],"%Y-%m-%d")))
        return itemData


    def getRebate(self,orderDetailId,type_num,pageNo=1):
        postdata = json.dumps({
            'financeNum':orderDetailId,
            'pageNo':pageNo,
            'pageSize':'20',
        })
        #print postdata
        checkcode_headers = {
            'Accept': 'application/json, */*; q=0.01',
            'Host': 'y.davdian.com',
            'Content-Type': 'application/json',
            'Referer': 'http://y.davdian.com/suppliers/client/html/details_pages',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'X-Requested-With': 'XMLHttpRequest',
        }
        detailUrl = 'http://y.davdian.com/suppliers/rebate/rebateSettlementPage'
        data_req = urllib2.Request(detailUrl,data=postdata.encode('utf-8'),headers=checkcode_headers)
        result = self.opener.open(data_req)
        data = result.read()
        result_json = json.loads(data)
        #print result_json

        pageSize = int(result_json['data']['totalPages'])
        currentPageNo = int(result_json['data']['pageNO'])
        #print 'pageSize'+str(pageSize)
        #print currentPageNo
        datas_formated = []
        for dataItem in result_json['data']['data']:
            self.orderIds.append(dataItem['deliverySn'])
            datas_formated.append(self.dataFormating(dataItem,orderDetailId,type_num))
        result = dict()
        result['data'] = datas_formated
        result['pageSize'] = pageSize
        result['pageNo'] = currentPageNo   
        
        return result



    def getDetail(self,orderDetailId,type_num,pageNo=1):
        postdata = json.dumps({
            'financeNum':orderDetailId,
            'pageNo':pageNo,
            'pageSize':'20',
            'payment':'6',
            'type':type_num
        })
        #print postdata
        checkcode_headers = {
            'Accept': 'application/json, */*; q=0.01',
            'Host': 'y.davdian.com',
            'Content-Type': 'application/json',
            'Referer': 'http://y.davdian.com/suppliers/client/html/details_pages',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'X-Requested-With': 'XMLHttpRequest',
        }
        detailUrl = 'http://y.davdian.com/suppliers/finance/settleOrder/sendOrderPage'
        data_req = urllib2.Request(detailUrl,data=postdata.encode('utf-8'),headers=checkcode_headers)
        result = self.opener.open(data_req)
        data = result.read()
        result_json = json.loads(data)
        #print result_json

        pageSize = int(result_json['data']['totalPages'])
        currentPageNo = int(result_json['data']['pageNO'])
        #print 'pageSize'+str(pageSize)
        #print currentPageNo
        datas_formated = []
        for dataItem in result_json['data']['data']:
            self.orderIds.append(dataItem['deliverySn'])
            datas_formated.append(self.dataFormating(dataItem,orderDetailId,type_num))
        result = dict()
        result['data'] = datas_formated
        result['pageSize'] = pageSize
        result['pageNo'] = currentPageNo   
        
        return result

    def removeSettlement(self,orderDetailId):
        self.connect()
        self.createCursor()
        sql = "delete from "+self.tablePre+"dav_settlement where s_platform_no = '"+orderDetailId+"'"
        self.cursor.execute(sql)
        self.closeCursor()
        self.commit()
        self.closeConnect()
        
    def removeThisFinanceNum(self,orderDetailId):
        self.connect()
        self.createCursor()
        sql = "delete from "+self.tablePre+"dav_settlement_item where s_platform_no = '"+orderDetailId+"'"
        self.cursor.execute(sql)
        self.closeCursor()
        self.commit()
        self.closeConnect()
        
    def removeSettleOrder(self,orderDetailId):
        self.connect()
        self.createCursor()
        sql = "delete from "+self.tablePre+"dav_settlement_order where s_platform_no = '"+orderDetailId+"'"
        self.cursor.execute(sql)
        self.closeCursor()
        self.commit()
        self.closeConnect()


    def specialFormating(self,data,orderDetailId,type_num):
        itemData = dict()
        itemData['date'] = int(time.mktime(time.localtime(time.time())))
        itemData['s_platform_no'] = orderDetailId
        if data['moneyType'] == 1:
            itemData['polarity'] = 2
        elif data['moneyType'] == 2:
            itemData['polarity'] = 1
        itemData['type'] = type_num
        itemData['price'] = data['money']
        itemData['name'] = data['description']
        return itemData
    
    def holdingFormating(self,data,orderDetailId,type_num):
        itemData = dict()
        itemData['name'] = data['ticketEventName']
        itemData['date'] = int(time.mktime(time.localtime(time.time())))
        itemData['s_platform_no'] = orderDetailId
        itemData['polarity'] = 2
        itemData['order_no_platform'] = data['deliverySn']
        itemData['type'] = type_num
        itemData['price'] = data['ticketAmount']
        return itemData

    def orderFormating(self,data,orderDetailId,type_num):
        itemData = dict()
        itemData['s_platform_no'] = orderDetailId
        itemData['order_no_platform'] = data['deliverySn']
        return itemData
    
    def dataFormating(self,data,orderDetailId,type_num):
        #print data
        itemData = dict()
        if type_num == 2:
            ltime=time.localtime(int(data['createTime']))
            timeStr=time.strftime("%Y.%m.%d %H:%M:%S", ltime)
            itemData['name'] = data['deliverySn']+u'于'+timeStr+u"的退货单"
        itemData['date'] = int(time.mktime(time.localtime(time.time())))
        itemData['s_platform_no'] = orderDetailId
        itemData['polarity'] = 2
        itemData['order_no_platform'] = data['deliverySn']
        itemData['type'] = type_num
        itemData['price'] = float(data['totalPrice']) + float(data['shippingFee'])
        return itemData

    #def commisionProduct(self,products):
    
    def postOrders(self,data):
        self.connect()
        self.createCursor()
        for itemIndex in range(0,len(data)):
            try:
                self.addOrder(data[itemIndex])
                
            except Exception, e:
                print 'error2'
                self.closeCursor()
                self.rollback()
                self.createCursor()
                
                
            if itemIndex % 5000 == 0:
                print '事务了'
                self.closeCursor()
                self.commit()
                self.createCursor()
                
       
        self.closeCursor()
        self.commit()
        self.closeConnect()
    
    def postSpecial(self,data):
        self.connect()
        self.createCursor()
        for itemIndex in range(0,len(data)):
            try:
                self.addSpeical(data[itemIndex])
                
            except Exception, e:
                print 'error2'
                self.closeCursor()
                self.rollback()
                self.createCursor()
                
                
            if itemIndex % 5000 == 0:
                print '事务了'
                self.closeCursor()
                self.commit()
                self.createCursor()
                
       
        self.closeCursor()
        self.commit()
        self.closeConnect()

    def postSettles(self,data):
        self.connect()
        self.createCursor()
        for itemIndex in range(0,len(data)):
            try:
                self.addSettle(data[itemIndex])
                
            except Exception, e:
                print 'error2'
                self.closeCursor()
                self.rollback()
                self.createCursor()
                
                
            if itemIndex % 5000 == 0:
                print '事务了'
                self.closeCursor()
                self.commit()
                self.createCursor()
                
       
        self.closeCursor()
        self.commit()
        self.closeConnect()
        


    def postData(self,data):
        self.connect()
        self.createCursor()
        for itemIndex in range(0,len(data)):
            try:
                self.addItem(data[itemIndex])
                
            except Exception, e:
                print 'error2'
                self.closeCursor()
                self.rollback()
                self.createCursor()
                
                
            if itemIndex % 5000 == 0:
                print u'事务了'
                self.closeCursor()
                self.commit()
                self.createCursor()
                
       
        self.closeCursor()
        self.commit()
        self.closeConnect()
        
         
   
       
    def addSpeical(self,data):
        sql = "insert into "+self.tablePre+"dav_settlement_item(s_platform_no,type,name,price,polarity,date) values (%s,%s,%s,%s,%s,%s)"
        oid = self.cursor.execute(sql,(data['s_platform_no'],data['type'],data['name'],data['price'],data['polarity'],data['date']))
    
    def addOrder(self,data):
        sql = "insert into "+self.tablePre+"dav_settlement_order(s_platform_no,order_no_platform) values (%s,%s)"
        oid = self.cursor.execute(sql,(data['s_platform_no'],data['order_no_platform']))
    

    def addItem(self,data):
        sql = "insert into "+self.tablePre+"dav_settlement_item(order_no_platform,s_platform_no,type,name,price,polarity,date) values (%s,%s,%s,%s,%s,%s,%s)"
        oid = self.cursor.execute(sql,(data['order_no_platform'],data['s_platform_no'],data['type'],data['name'],data['price'],data['polarity'],data['date']))

        
    def addSettle(self,data):
        sql = "insert into "+self.tablePre+"dav_settlement(s_platform_no,sum,date_started,date_ended,date_created,date_settled) values (%s,%s,%s,%s,%s,%s)"
        oid = self.cursor.execute(sql,(data['s_platform_no'],data['sum'],data['date_started'],data['date_ended'],data['date_created'],data['date_settled']))
    
    


    def executeRows(self):
        self.connect()
        self.createCursor()
        #mo.id in ('5','6','7') or
        self.rowsArr = []
        self.orderIdArr = dict()
        self.cursor.execute("select ps.order_no_platform,ps.id from "+self.tablePre+"platform_statement_item as ps where (ps.order_no_platform in ('"+"','".join(self.orderIds)+"') and ps.platform = '"+self.platform+"')")
        rows = self.cursor.fetchall()
        
        self.closeCursor()
        self.commit()
        self.closeConnect()
        for row in rows:
            self.rowsArr.append(row[0])
            self.orderIdArr[row[0]] = row[1]
        
    def closeConnect(self):
        self.conn.close()
    
    def closeCursor(self):
        self.cursor.close()
        
    def commit(self):
        self.conn.commit()
        
    def rollback(self):
        self.conn.rollback()
        
    def connect(self):
        self.tablePre = 'test_'
        self.conn = MySQLdb.connect(host='localhost',user="root",passwd="molimama",db="youerdian_test",charset="utf8")
        
    def createCursor(self):
        self.cursor = self.conn.cursor()
        
    def build_order_no(self):
        return time.strftime('%Y%m%d%H%M%S',time.localtime(time.time()))+str(random.randint(100000,999999))
        
    def writeSyncTime(self,timestamp):
        #print timestamp
        ltime=time.localtime(int(timestamp))
        timeStr=time.strftime("%Y.%m.%d %H:%M:%S", ltime)
        with open(syncTimeFileName, 'w') as f:
            f.write(timeStr)
        
        
        
    def __init__(self,infoCookieStr,code):
        self.filename = os.path.dirname(__file__) + '/davCookie.txt'
        self.platform = '11'
        self.orderIds = []
        self.barcodes = []
        self.settlements = []
        self.checkcode_headers = {
            'Accept': 'application/json',
            'Connection': 'keep-alive',
            'Origin': 'http://y.davdian.com',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; WOW64; rv:49.0) Gecko/20100101 Firefox/49.0',
            'Content-Type': 'application/json',
            'Referer': 'http://y.davdian.com/',
            'Accept-Language': 'zh-CN,zh;q=0.8',
            'Accept-Encoding': 'gzip, deflate',
            'X-Requested-With':'XMLHttpRequest',
        }
        self.checkcode_headers['Cookie'] = infoCookieStr+';'+'identifyCode='+code+';'
        self.checkUserCookie = infoCookieStr 
        self.postdata = {
            'userName':'793564861@qq.com',
            'password':'molimama88',
            'identifyCode': code,
            'isRemember':1,
        }
        self.login()
        
    


if __name__ == '__main__':
    resultList = []
    #formdata = '{end':'',"cookieStr":"JSESSIONID=D0A820D0564CFF2BAB564C3D6F86E36A;","captche":"9973"}'
    syncTimeFileName = os.path.dirname(__file__) + '/davStatementsTime.txt'
    fileReader = open(syncTimeFileName, 'r')
    startTime = fileReader.read()
    fileReader.close()
    cookie_str = 'JSESSIONID=0DF2D74BF603A0CA83C7A3E4B4CD37D6;'
    captche = '7286'
    dav = Dav(cookie_str,captche)
    if startTime:
        start = int(time.mktime(time.strptime(startTime,"%Y.%m.%d %H:%M:%S")))*1000 
    else:
        start = ''
    end = int(time.mktime(time.localtime(time.time())))*1000
    #end = int(time.mktime(time.strptime('2017.01.13 00:00:00',"%Y.%m.%d %H:%M:%S")))*1000
    dav.getDatas(str(start),str(end))
    
    
