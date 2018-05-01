# -*- coding: utf-8 -*-
"""
Created on Wed Apr 18 18:40:43 2018

@author: Vinod
"""

import pandas as pd
import numpy as np
from pymongo import MongoClient
from datetime import datetime
from datetime import timedelta
import os

def cleanSalesTrx(path,filename):
    """
    Sales Transaction file has few junk characters at the start  of every row and empty column at the end.
    This function removes the junk characters and empty column and creates a cleaner version of the file.
    Return full file name to calling program.
    """
    messyFile = open(path+filename,encoding='latin_1')
    tidyFile = open(path+'SalesTrxCln.txt','a')
    for line in messyFile.readlines():
        tidyFile.write(line[3:-2]+'\n')
    tidyFile.close()
    print('Sales Transaction File is cleaned')
    return(tidyFile.name)

def extractSalesTrx(filename):
    """
    This function extracts the sales transaction data from cleaned Sales file to python
    It returns the sales transaction data frame
    """
    sales_cols = {'StoreNum':np.int64,
              'Register':np.int64,
              'TransNum':np.int64,
              'TransDatetime(GMT)':'datetime64[ns]',
              'TransDatetime(Local)':str,
              'BusDate':'datetime64[ns]',
              'UPC':str,
              'ItemID':str,
              'DeptNum':np.int64,
              'ItemQuantity':np.float64,
              'WeightAmt':np.float64,
              'SalesAmt':np.float64,
              'CostAmt':np.float64,
              'CashierNum':np.int64,
              'PriceType':str,
              'ServiceType':str,
              'TenderType':str,
              'LoyaltyCardNumber':np.int64}
    salesTrx = pd.read_csv(filename,
                     sep='|',
                     header=None,
                     parse_dates= [[3,4],5])
    salesTrx=pd.concat([salesTrx.iloc[:,1:4],salesTrx.iloc[:,0],salesTrx.iloc[:,0].astype(str),salesTrx.iloc[:,4:]],axis=1)
    salesTrx.columns=sales_cols.keys()
    salesTrx.fillna({'UPC':-999,
                 'LoyaltyCardNumber':-999},inplace=True)
    salesTrx=salesTrx.astype(sales_cols)
    salesTrx['BusDate'] = salesTrx['BusDate']+timedelta(hours=12)
    salesTrx.name = 'SalesTrx'
    print(salesTrx.dtypes)
    return(salesTrx)

def extractItemAttr(path,filename):
    """
    This function extracts the sales transaction data from Item Attribute file to python
    It returns the Item Atrribute data frame
    """
    attr_cols = {"UPC":str,
             "ItemPosDes":str,
             "ItemAttributeDes":str,
             "ItemAttributeValue":str,
             "AttributeStartDate":"datetime64[ns]",
             "AttributeEndDate":"datetime64[ns]"
             }
    item_att= pd.read_csv(path+filename,
            sep = '|',
            skiprows=3,
            header = None,
            names=attr_cols.keys(),
            parse_dates=[4,5])
    item_att = item_att.astype(attr_cols)
    item_att['AttributeStartDate'] = item_att['AttributeStartDate']+timedelta(hours=12)
    item_att['AttributeEndDate'] = item_att['AttributeEndDate']+timedelta(hours=12)
    return (item_att)

    
def extractCustomer(path,filename):
    """
    This function extracts the sales transaction data from Customer file to python
    It returns the Customer data frame
    """
        
    cust_cols = {"LoyaltyCardNum":np.int64,
    "HouseholdNum":np.int64,
    "MemberFavStore":np.int64,
    "City":str,
    "State":str,
    "ZipCode":str,
    "ExtraCol":str
    }
    cust_list = pd.read_csv(path + filename,
                            sep = '|',
                            header = None,
                            encoding='latin_1',
                            quoting=3,
                            names=cust_cols.keys())
    cust_list.isna().sum()
    cust_list.fillna({'LoyaltyCardNum':-999,
                       'HouseholdNum':-999,
                       'MemberFavStore':-999},inplace=True)
    cust_list = cust_list.astype(cust_cols)
    cust_list = cust_list.drop(['ExtraCol'],axis = 1)
    return (cust_list)

def extractItemList(path,filename):
    item_cols = {"UPC":str,
            "ItemID":str,
            "Status":np.int64,
            "LongDes":str,
            "ShortDes":str,
            "ClassCode":np.int64,
            "ClassDes":str,
            "CategoryCode":np.int64,
            "CategoryDes":str,
            "FamilyCode":np.int64,
            "FamilyDes":str,
            "DepartmentCode":np.int64,
            "StoreBrand":str,
            "ExtraDes":str,
            "ExtraCol":str}
    item_list = pd.read_csv(path+filename,
                            sep = '|',
                            header = None,
                            encoding='latin1',
                            names=item_cols.keys(),
                            dtype=item_cols,
                            quoting=3)
    item_list.replace({'Status':{0:'Invalid Item',
                             1:'Active',
                             2:'Suspended',
                             3:'Deleted'}},inplace=True)
    item_list['UPC'].fillna('-999',inplace=True)
    item_list = item_list.drop(['ExtraCol'],axis = 1)
    item_list.dtypes
    return(item_list)

def connectToMongo(hostname,port):
    """
    This function estalishes connection to Mongo DB and returns connection object
    """
    mongoConnect = MongoClient(host='127.0.0.1',port=27017)
    return mongoConnect

def insertIntoMongoDF(database,collection,mongo_conn,df):
    """
    This function insert the data into appropriate collection in mongo DB
    """
    db = mongo_conn.get_database(database)
    db.get_collection(collection).drop()
    db.create_collection(collection)
    db.get_collection(collection).insert_many(df.to_dict(orient='record'))
    
def insertIntoMongoDict(database,collection,mongo_conn,records):
    """
    This function insert the data into appropriate collection in mongo DB
    """
    db = mongo_conn.get_database(database)
    db.get_collection(collection).drop()
    db.create_collection(collection)
    db.get_collection(collection).insert_many(records)

def loadHierarchy(df,conn,database,collection):
    hierRecords=[{'UPC':df.loc[i,'UPC'],
         'ItemID':df.loc[i,'ItemID'],
         'DepartmentCode':int(df.loc[i,'DepartmentCode']),
         'Family':{'FamilyCode':int(df.loc[i,'FamilyCode']),
                   'FamilyDesc':df.loc[i,'FamilyDes'],
                   'Category':{'CategoryCode':int(df.loc[i,'CategoryCode']),
                               'CategoryDesc':df.loc[i,'CategoryDes'],
                               'Class':{'ClassCode':int(df.loc[i,'ClassCode']),
                                        'ClassDesc':df.loc[i,'ClassDes']}}}} for i in range(0,len(df))]
    conn.get_database(database).get_collection(collection).drop()
    conn.get_database(database).create_collection(collection)
    conn.get_database(database).get_collection(collection).insert_many(hierRecords)
    
def extractStoreLoc(path,filename):
    store_loc_cols={'StoreNum':np.int64,
                'StoreName':str,
                'ActiveFlag':str,
                'AddressLine1':str,
                'City':str,
                'StateCode':str,
                'ZipCode':str,
                'SqFoot':np.int64,
                'Region':str,
                'ClusterName':str,
                'ExtraCol':str
                }
    storeLoc= pd.read_csv(path+filename,
                   skiprows=2,
                   sep='|',
                   header=None,
                   names=store_loc_cols.keys())
    nullRecordInd = storeLoc[storeLoc.isna().sum(axis=1)>=storeLoc.shape[1]-1].index.values
    storeLoc.drop(index=nullRecordInd,axis=0,inplace=True)
    storeLoc = storeLoc.astype(store_loc_cols)
    storeLoc.drop(columns=['ExtraCol'],axis=1,inplace=True)
    return(storeLoc)

def extractScrapedStore(path):
    files = os.listdir(scrape_path)
    storeScraped=pd.DataFrame([])
    temp_cols = {'StoreName':str,
             'StoreId':np.int64,
             'LocationName':str,
             'State':str,
             'ZipCode':str,
             'ServiceName':str,
             'ServiceValue':str}
    for file in files:
        temp = pd.read_csv(scrape_path+file,
                   header=None)
        for i in range(0,len(temp.columns)):
            for j in range(0,len(temp)):
                temp.loc[j,i]=temp.loc[j,i][temp.loc[j,i].find(':')+1:]
        temp.columns = temp_cols.keys()
        temp = temp.astype(temp_cols)
        storeScraped=pd.concat([storeScraped,temp],ignore_index=True)
    
    storeScraped = storeScraped.astype(temp_cols)
    storeScraped.loc[~storeScraped['ServiceValue'].str.lower().isin(['true','false']),'ServiceValue'] = np.nan
    storeRecords = []
    for i in storeScraped.StoreId.unique():
        df_subset = storeScraped[storeScraped.StoreId==i]
        df_pivot = pd.pivot_table(data=df_subset,
                        aggfunc=lambda x: x,
                        columns='ServiceName',
                        values='ServiceValue',
                        index=['StoreId','StoreName','LocationName','State','ZipCode'])
        df_pivot.reset_index(inplace=True)
        rec_service = df_pivot.iloc[:,5:].to_dict(orient='record')[0]
        rec = df_pivot.iloc[:,:5].to_dict(orient='record')[0]
        rec['Service']=rec_service
        storeRecords.append(rec)
    return(storeRecords)
    
    
if __name__ == '__main__':
    
    PATH = 'C:/Users/Universe/Desktop/DataScience/Spring 2018/BI/Project/dataFiles/'
    scrape_path = 'C:/Users/Universe/Desktop/DataScience/Spring 2018/BI/Project/dataFiles/scraping/'
    SalesFile = 'sls_dtl.txt'
    ItemAttrFile = 'Item_Attr.txt'
    CustFile = 'customer_List.txt'
    ItemListFile = 'Item_List.txt'
    StoreLocFile = 'store_list.txt'
    
    SalesTrxClean = cleanSalesTrx(PATH,SalesFile)
    salesDF = extractSalesTrx(SalesTrxClean)
    itemAttrDF = extractItemAttr(PATH,ItemAttrFile)
    custDF = extractCustomer(PATH,CustFile)
    itemListDF = extractItemList(PATH,ItemListFile)
    storeLocDF = extractStoreLoc(PATH,StoreLocFile)
    DF = extractScrapedStore(scrape_path)
        
    conn_obj = connectToMongo(hostname='127.0.0.1',port=27017)
    insertIntoMongoDF('BIProject','SalesTrx',conn_obj,salesDF)
    insertIntoMongoDF('BIProject','ItemAttribute',conn_obj,itemAttrDF)
    insertIntoMongoDF('BIProject','Customer',conn_obj,custDF)
    insertIntoMongoDF('BIProject','ItemList',conn_obj,itemListDF)
    loadHierarchy(itemListDF,conn_obj,'BIProject','ItemHierarchy')
    insertIntoMongoDF('BIProject','StoreLocation',conn_obj,storeLocDF)
    insertIntoMongoDict('BIProject','StoreScraped',conn_obj,DF)
    
    




    
    
    





