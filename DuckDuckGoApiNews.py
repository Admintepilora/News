#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Sep 18 16:13:27 2024

@author: a
"""
from pymongo import MongoClient
from pymongo import UpdateOne
from datetime import datetime, timezone
"""apro connessione mongodb"""
import random
import subprocess


import time

from duckduckgo_search import DDGS
import pandas as pd
# Selenium imports

from flatten_json import flatten, unflatten_list

#tor imports
from configTorProxies import listOfTorProxies
from random import randrange

listOfProxy=listOfTorProxies
proxy = listOfProxy[randrange(0,len(listOfProxy))]
    

from stem.control import Controller
import random



def search_news(query, max_results=50,timelimit='d'):
    proxy = listOfProxy[randrange(0,len(listOfProxy))]
    ddgs = DDGS(proxy = proxy['https'])
    news = ddgs.news(query, max_results=max_results,timelimit=timelimit)
    return news


# Example usage
if __name__ == "__main__":
    while True:
        try:
            Mclient = MongoClient('13.39.92.143', 27017,username='Andrea', password='Sara')
            queries=['Stock Market','Bonds','Futures','Bond Market']
            queries+=['Macroeconomic','Fiscal Policy','Monetary Policy','FED','ECB','BOJ','BoE','Unemployment','Inflation','Economic Calendar']
            queries+=['Wages','Consumer Confidence']
            queries+=['Powell','Lagarde','Economy','Earnings']
            queries+=['S&P500','Nasdaq composite index','DAX','FTSE','CAC','BTP','BUND','Nikkei','TBond','BONOS','Treasury','OAT']
            queries+=['Exchange Rates','Currencies','USD','EUR','YEN','Dollar','CHF','GBP','CNY','AUD','JPY','NZD']
            queries+=['Trump','Russia','Putin','China','Xijinping','Iran','Israel']
            queries+=['OIL','WTI','Brent','Silver','Copper','Gold','Commodities']
            #queries=['Commodities']
            print(queries)
            db = Mclient['News']
            collection=db['Queries']
            queries_for_mongo=[{'Query':q} for q in queries]
            mongo_result=[collection.update_one(q, {'$set':q},upsert=True) for q in queries_for_mongo]
        
            for query in queries:
                results=[]
                print(query+' news updating...')
                result=None
                tries=0
                while result==None and tries<3:
                    try:
                        result = search_news(query, max_results=50,timelimit='d')
                        [r.update({'searchKey':query}) for r in result]
                        results+=result
                        if result!=[] and result is not None: 
                            print(query+' result OK!')
                            break
                    except:
                        pass
                        time.sleep(5)
                    tries=tries+1
                    print(tries)                
                    
                if results==[]:query+' results not found!'
                else:
                    results_df=pd.DataFrame([flatten(r,'.') for r in results])
                    results_df = results_df.sort_values(by='date', ascending=False)
                    columns_to_check = results_df.columns.difference(['searchKey','image'])
                    results_df = results_df.drop_duplicates(subset=columns_to_check)
                    results_df.head()
                    #oldcsv=pd.read_csv('news.csv')
                    #print(oldcsv.columns)
                    #oldcsv=oldcsv.drop('Unnamed: 0',axis=1)
                    #news = pd.concat([results_df, oldcsv], ignore_index=True)
                    news=results_df.copy()
                    news = news.drop_duplicates(subset=['url'], keep='first')
                    news = news.sort_values(by='date', ascending=True)
                
                    # Define the string to remove
                    text_to_remove = "Connecting decision makers to a dynamic network of information, people and ideas, Bloomberg quickly and accurately delivers business and financial information, news and insight around the world"    
                    # Remove rows where the column contains this string
                    news = news[~news['body'].str.contains(text_to_remove, na=False)]
                    try:del news['level_0']
                    except:pass
                    try:del news['index']
                    except:pass
                    news.reset_index(drop=True, inplace=True)
                    news.index = range(len(news)-1, -1, -1)
                    news["date"] = pd.to_datetime(news["date"])
            
                    news_json=news.to_dict(orient='records')
                    
                    collection=db['News']
                
                    # Create bulk operations
                    bulk_operations = [
                        UpdateOne(
                            {'url': d['url']},  # filter criteria
                            {'$set': d},        # update operation
                            upsert=True
                        ) 
                        for d in news_json
                    ]
                    
                    # Execute bulk write
                    collection = db['News']
                    if bulk_operations!=[]:
                        collection.bulk_write(bulk_operations)
                        print(query+' news updated')
                    else: print('no mongo operations to perform')
            
            result = collection.update_many(
            {"date": {"$type": "string"}},  # Match documents where 'date' is a string
            [{"$set": {"date": {"$toDate": "$date"}}}]  # Use MongoDB's $toDate operator
            )
        
            print(f"Modified {result.modified_count} documents.")
            Mclient.close()
                
                #news.to_csv('news.csv')
        except Exception as e:
            print('Error:',e)
            from pymongo import MongoClient
            from pymongo import UpdateOne
            from datetime import datetime, timezone
            """apro connessione mongodb"""
            import random
            import subprocess


            import time

            from duckduckgo_search import DDGS
            import pandas as pd
            # Selenium imports

            from flatten_json import flatten, unflatten_list

            #tor imports
            from configTorProxies import listOfTorProxies
            from random import randrange

            listOfProxy=listOfTorProxies
            proxy = listOfProxy[randrange(0,len(listOfProxy))]
                

            from stem.control import Controller
            import random
