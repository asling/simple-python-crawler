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
        self.endTime = end
        normalUrl = 'http://y.davdian.com/suppliers/order/list?status=&start='+start+'&end='+end+'&startPayTime=&endPayTime=&pageNo=1&pageSize=10'
        
        f1 = self.opener.open(normalUrl)
        data1 = f1.read().replace("'","\"")
        data_json1 = json.loads(data1)
        self.pageSize = data_json1['data']['pager']['totalPages'];
        
        datas_formated = []
        for page in range(1,self.pageSize+1):
            currentUrl = 'http://y.davdian.com/suppliers/order/list?status=&start='+start+'&end='+end+'&startPayTime=&endPayTime=&pageNo='+str(page)+'&pageSize=10'
            f2 = self.opener.open(currentUrl)
            data2 = f2.read().replace("'","\"")
            data_json2 = json.loads(data2)
            
            for dataItem in data_json2['data']['orderList']:
                datas_formated.append(self.getDetail(dataItem['deliveryId'],dataItem['status']))
            
        
        self.executeRows()
        self.executeOrderItem()
        
        self.postData(datas_formated)
        

    def getDetail(self,orderDetailId,status):
        detailUrl = 'http://y.davdian.com/suppliers/order/info?orderId='+str(orderDetailId)+'&status='+str(status)
        
        f = self.opener.open(detailUrl)
        data = json.loads(f.read())
        return self.dataFormating(data['data'])

    def dataFormating(self,data):
        
        itemData = dict()
        if not data.has_key("orderAddressModel") :
            buyerData = {'consignee':'','mobile':'','wholeAddress':''}
        else:
            buyerData = data['orderAddressModel']
        if not data.has_key("orderInfoParams"):
            orderData = {'orderId':'','payTimeStr':'','statusStr':''}
        else:
            orderData = data['orderInfoParams']
            orderData['payTimeStr'] = int(time.mktime(time.strptime(orderData['payTimeStr'],"%Y-%m-%d %H:%M:%S")))
        self.orderIds.append(orderData['orderId'])
        itemData['order_num'] = orderData['orderId']
        itemData['create_time'] = orderData['payTimeStr']
        itemData['statusname'] = orderData['statusStr']
        itemData['order_price'] = 0
        itemData['accept_name'] = buyerData['consignee']
        itemData['mobile'] = buyerData['mobile']
        itemData['address'] = buyerData['wholeAddress']
        itemData['platform_freight'] = 0
        itemData['supplier_freight'] = 0
        itemData['products'] = []
        if data.has_key('goodsInfoParams'):
            productsData = data['goodsInfoParams']
            
            for productIndex in range(0,len(productsData)):
                
                productItem = productsData[productIndex]
                if not productItem['barcode'] in self.barcodes:
                    self.barcodes.append(productItem['barcode'])
                itemData['products'].append({
                    "title": productItem['goodsName'],
                    "goodsCount":productItem['goodsCount'],
                    "price":productItem['supplierIncome'],
                    "number":productItem['barcode']
                }) 
                itemData['order_price'] = itemData['order_price'] + itemData['products'][productIndex]['price']*itemData['products'][productIndex]['goodsCount']
        
        return itemData

    #def commisionProduct(self,products):

    def postData(self,data):
        self.connect()
        self.createCursor()
        for itemIndex in range(0,len(data)):
            if data[itemIndex]['order_num'] in self.rowsArr:
            #update
                try:
                    self.updateItem(data[itemIndex])
                
                except Exception, e:
                    
                    self.closeCursor()
                    self.rollback()
                    self.createCursor()
                
                    
            else:
                try:
                    self.addItem(data[itemIndex])
                
                except Exception, e:
                    
                    self.closeCursor()
                    self.rollback()
                    self.createCursor()
                
            if itemIndex % 5000 == 0:
                
                self.closeCursor()
                self.commit()
                self.createCursor()
                
               
        self.closeCursor()
        self.commit()
        self.closeConnect()
        
        self.updateOrders()
        self.writeSyncTime(self.endTime)
        print 'done'
            #add
            
    def updateItem(self,data):
      
       
        sql = "update "+self.tablePre+"marketing_order set statusname=%s,accept_name=%s,address=%s,mobile=%s,payable_amount=%s,real_payable_amount=%s,create_time=%s where order_no_platform = '"+str(data['order_num'])+"' and platform = '"+str(self.platform)+"'"
        self.cursor.execute(sql,(data['statusname'],data['accept_name'],data['address'],data['mobile'],data['order_price'],data['order_price'],data['create_time']))
        existItems = []
   
        for item in data['products']:
            
            #existItems.append(item['number'])
            if self.orderItemArr.has_key(data['order_num']) and item['number'] in self.orderItemArr[data['order_num']]:
                
                sqlSub = "update "+self.tablePre+"marketing_order_item set product_name=%s,product_price=%s,product_number=%s where order_id = (select mo.id from "+self.tablePre+"marketing_order as mo where mo.order_no_platform = '"+data['order_num']+"') and product_number = '"+item['number']+"'"
                #
                
                self.cursor.execute(sqlSub,(item['title'],str(item['price']),item['number']))
            else:
                sqlSub = "insert into "+self.tablePre+"marketing_order_item(order_id,product_name,product_price,product_number) values (%s,%s,%s,%s)"
                self.cursor.execute(sqlSub,(self.orderIdArr[data['order_num']],item['title'],str(item['price']),item['number']))
            #add
            
            
            


    def addItem(self,data):
        orderNumber = "ON"+self.build_order_no()
        
        sql = "insert into "+self.tablePre+"marketing_order(order_no,platform,pay_type,statusname,pay_status,accept_name,address,mobile,payable_amount,real_payable_amount,create_time,order_no_platform,platform_freight,supplier_freight) values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        oid = self.cursor.execute(sql,(orderNumber,self.platform,1,data['statusname'],1,data['accept_name'],data['address'],data['mobile'],data['order_price'],data['order_price'],data['create_time'],data['order_num'],0,0))

        
        sql = "insert into "+self.tablePre+"marketing_order_item(order_id,product_name,product_price,product_number) values (%s,%s,%s,%s)" 
        for item in data['products']:
            for goodNum in range(0,item['goodsCount']):
                
                self.cursor.execute(sql,(str(self.cursor.lastrowid),item['title'],str(item['price']),item['number']))

    
    
    def updateOrders(self):
        self.connect()
        self.createCursor()
        
        ids = []
        idsCreateTime = dict()
        orderItemIds = []
        productNumberArr = []
        productIdNumberDict = dict()
        activityInfo = dict()
        costlogInfo = dict()
        idsInfo = dict()
        
        sql = "select id,create_time from "+self.tablePre+"marketing_order where order_no_platform in ('"+"','".join(self.orderIds)+"')"
        result = self.cursor.execute(sql)
        orderRows = self.cursor.fetchall()
        
        for orderRow in orderRows:
            ids.append(str(orderRow[0]))
            idsInfo[orderRow[0]] = {'create_time':orderRow[1],'product_ids':[],'activity':'','order_items':dict()}
        
        sql = "select id,order_id,product_number from "+self.tablePre+"marketing_order_item where order_id in ('"+"','".join(ids)+"')"
        result2 = self.cursor.execute(sql)
        orderItemRows = self.cursor.fetchall()
        
        for orderItemRow in orderItemRows:
            orderItemIds.append(orderItemRow[0])
            productNumberArr.append(orderItemRow[2])
            
        sql = "select id,number from "+self.tablePre+"market_product where number in ('"+"','".join(productNumberArr)+"')"
        result3 = self.cursor.execute(sql)
        productRows = self.cursor.fetchall()
        
        for productRow in productRows:
            productIdNumberDict[productRow[1]] = productRow[0]
            
        for orderItemRow in orderItemRows:
            productId = productIdNumberDict.has_key(orderItemRow[2]) and productIdNumberDict[orderItemRow[2]] or '' 
            idsInfo[orderItemRow[1]]['order_items'][orderItemRow[0]] = productId
            
        
        
        for orderItemRow in orderItemRows:
            if productIdNumberDict.has_key(orderItemRow[2]):
                idsInfo[orderItemRow[1]]['product_ids'].append(str(productIdNumberDict[orderItemRow[2]]))
                
            
            
        number = 0    
        for infoRowIndex in idsInfo:
            activityProductCostDict = dict()
            costlogProductCostDict = dict()
            lastProductCostDict = dict()
            activityPoductArr = []
            costlogProductArr = []
            number =+ 1
            sql = "select id from "+self.tablePre+"platform_activity where date_started <= "+str(idsInfo[infoRowIndex]['create_time'])+" and date_ended >= "+str(idsInfo[infoRowIndex]['create_time'])+" and platform_type = '"+self.platform+"'"
            self.cursor.execute(sql)
            row = self.cursor.fetchone()
            if row and row[0]>0:
                idsInfo[infoRowIndex]['activity'] = row[0]
                sql = "select product_id,cost from "+self.tablePre+"platform_activity_product where activity_id = '"+str(row[0])+"' and product_id in ('"+"','".join(idsInfo[infoRowIndex]['product_ids'])+"')"
                self.cursor.execute(sql)
                actProRows = self.cursor.fetchall()
                activityInfo[infoRowIndex] = []
                for actProRow in actProRows:
                    activityInfo[infoRowIndex].append({'product_id':actProRow[0],'cost':actProRow[1]})
                    activityPoductArr.append(actProRow[0])
                    activityProductCostDict[actProRow[0]] = actProRow[1] 
            else:
                sql = "select product_id,max(cost) from "+self.tablePre+"market_product_costlog where product_id in ('"+"','".join(idsInfo[infoRowIndex]['product_ids'])+"') GROUP BY product_id"
                self.cursor.execute(sql)
                costProRows = self.cursor.fetchall()
                costlogInfo[infoRowIndex] = []
                for costProRow in costProRows:
                    costlogInfo[infoRowIndex].append({'product_id':costProRow[0],"cost":costProRow[1]})
                    costlogProductArr.append(costProRow[0])
                    costlogProductCostDict[costProRow[0]] = costProRow[1]
            orderProductArr = list(set(costlogProductArr).union(set(activityPoductArr)))
            
            costSum = 0
            for productItem in orderProductArr:
                if activityProductCostDict.has_key(productItem):
                    costSum = costSum + activityProductCostDict[productItem]
                    lastProductCostDict[productItem] = activityProductCostDict[productItem]
                else:
                    costSum = costSum + costlogProductCostDict[productItem]
                    lastProductCostDict[productItem] = costlogProductCostDict[productItem]
                    
            
                    
            sql = "update "+self.tablePre+"marketing_order set cost_amount=%s,real_cost_amount=%s where id='"+str(infoRowIndex)+"'"
            self.cursor.execute(sql,(costSum,costSum))
            for orderItem in idsInfo[infoRowIndex]['order_items']:
                if idsInfo[infoRowIndex]['order_items'][orderItem] and idsInfo[infoRowIndex]['order_items'][orderItem] in lastProductCostDict:
                    orderItemIndex = idsInfo[infoRowIndex]['order_items'][orderItem]
                    sql = "update "+self.tablePre+"marketing_order_item set product_id=%s,cost=%s where id = '"+str(orderItem)+"'"
                    self.cursor.execute(sql,(orderItemIndex,lastProductCostDict[orderItemIndex]))
                    
            if number % 5000 == 0 :
                self.closeCursor()
                self.commit()
                self.createCursor()
            
                
        self.closeCursor()
        self.commit()
        self.closeConnect()
        
        
           
     
    def executeOrderItem(self):
        self.connect()
        self.createCursor()
        self.orderItemArr = dict()
        
        
        
        self.cursor.execute("select moi.product_number,mo.order_no_platform from "+self.tablePre+"marketing_order_item as moi left join "+self.tablePre+"marketing_order as mo on mo.id = moi.order_id where mo.order_no_platform in ('"+"','".join(self.orderIds)+"') and mo.platform = '"+str(self.platform)+"' ")
        
        
        
        rows = self.cursor.fetchall()
        self.closeCursor()
        self.commit()
        self.closeConnect()
        for row in rows:
            self.orderItemArr[row[1]] = []
        for row in rows:
            self.orderItemArr[row[1]].append(row[0])
    
    def executeRows(self):
        self.connect()
        self.createCursor()
        #mo.id in ('5','6','7') or
        self.rowsArr = []
        self.orderIdArr = dict()
        self.cursor.execute("select mo.order_no_platform,mo.id from "+self.tablePre+"marketing_order as mo where (mo.order_no_platform in ('"+"','".join(self.orderIds)+"') and mo.platform = '11')")
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
        
        ltime=time.localtime(int(timestamp))
        timeStr=time.strftime("%Y.%m.%d %H:%M:%S", ltime)
        with open(syncTimeFileName, 'w') as f:
            f.write(timeStr)
        
        
        
    def __init__(self,infoCookieStr,code):
        self.filename = os.path.dirname(__file__) + '/davCookie.txt'
        self.platform = '11'
        self.orderIds = []
        self.barcodes = []
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
    syncTimeFileName = os.path.dirname(__file__) + '/davSyncTime.txt'
    fileReader = open(syncTimeFileName, 'r')
    startTime = fileReader.read()
    fileReader.close()
    #formdata = '{end':'',"cookieStr":"JSESSIONID=D0A820D0564CFF2BAB564C3D6F86E36A;","captche":"9973"}'
    cookie_str = 'JSESSIONID=0DF2D74BF603A0CA83C7A3E4B4CD37D6;'
    captche = '7286'
    dav = Dav(cookie_str,captche)
    print startTime
    if startTime:
        start = int(time.mktime(time.strptime(startTime,"%Y.%m.%d %H:%M:%S")))
    else:
        start = ''
    
    end = int(time.mktime(time.localtime(time.time())))
    #end = int(time.mktime(time.strptime('2016.11.30 23:59:59',"%Y.%m.%d %H:%M:%S")))
    dav.getDatas(str(start),str(end))
    
    
