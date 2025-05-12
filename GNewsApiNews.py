#!/usr/bin/env python3
from pymongo import MongoClient, UpdateOne
from datetime import datetime, timezone
import pandas as pd
import time
from flatten_json import flatten
from configTorProxies import listOfTorProxies
from random import randrange

def search_news(query, max_results=50, timelimit='d'):
    proxy = listOfTorProxies[randrange(0,len(listOfTorProxies))]
    print(proxy)
    #try:del proxy['http']
    #except:pass
    
    from gnews import GNews
    google_news = GNews(
        language='en',
        country='US',
        period=timelimit,
        max_results=max_results
        )
    google_news.proxy=proxy
    #print(google_news.proxy) 



    news = google_news.get_news(query)
    current_time = datetime.now(timezone.utc)
    
    for article in news:
        article.update({
            "date": current_time,
            "searchKey": query,
            "source": article.get('publisher', {}).get('href', ''),
            "url": article.get('url', '')
        })
        article['title'] = article.pop('title', '')
        article['body'] = article.pop('description', '')
        for key in ['publisher', 'published date']:
            article.pop(key, None)
    
    return news

if __name__ == "__main__":
    while True:
        time.sleep(randrange(10,20,1))

        Mclient = MongoClient('13.39.92.143', 27017, username='Andrea', password='Sara')
        queries = ['Stock Market','Bonds','Futures','Bond Market']
        queries += ['Macroeconomic','Fiscal Policy','Monetary Policy','FED','ECB','BOJ','BoE','Unemployment','Inflation','Economic Calendar']
        queries += ['Wages','Consumer Confidence']
        queries += ['Powell','Lagarde','Economy','Earnings']
        queries += ['S&P500','Nasdaq composite index','DAX','FTSE','CAC','BTP','BUND','Nikkei','TBond','BONOS','Treasury','OAT']
        queries += ['Exchange Rates','Currencies','USD','EUR','YEN','Dollar','CHF','GBP','CNY','AUD','JPY','NZD']
        queries += ['Trump','Russia','Putin','China','Xijinping','Iran','Israel']
        queries += ['OIL','WTI','Brent','Silver','Copper','Gold','Commodities']
        
        db = Mclient['News']
        collection = db['Queries']
        queries_for_mongo = [{'Query':q} for q in queries]
        [collection.update_one(q, {'$set':q}, upsert=True) for q in queries_for_mongo]

        for query in queries:
            time.sleep(randrange(5,10,1))
            results = []
            print(f"{query} news updating...")
            tries = 0
            
            while tries < 5:
                try:
                    result = search_news(query, max_results=50, timelimit='d')
                    if result and len(result) > 0:
                        [r.update({'searchKey':query}) for r in result]
                        results += result
                        print(f"{query} result OK!")
                        break
                except Exception as e:
                    print(f"Error: {e}")
                    time.sleep(1)
                tries += 1
                print(f"Attempt {tries}")

            if results:
                results_df = pd.DataFrame([flatten(r, '.') for r in results])
                results_df = results_df.sort_values(by='date', ascending=False)
                columns_to_check = results_df.columns.difference(['searchKey','image'])
                results_df = results_df.drop_duplicates(subset=columns_to_check)
                
                news = results_df.copy()
                news = news.drop_duplicates(subset=['url'], keep='first')
                news = news.sort_values(by='date', ascending=True)
                
                news = news[~news['body'].str.contains("Connecting decision makers to a dynamic network", na=False)]
                
                try: del news['level_0']
                except: pass
                try: del news['index']
                except: pass
                
                news.reset_index(drop=True, inplace=True)
                news.index = range(len(news)-1, -1, -1)
                news["date"] = pd.to_datetime(news["date"])
                
                news_json = news.to_dict(orient='records')
                collection = db['News']
                
                bulk_operations = [
                    UpdateOne(
                        {'url': d['url']},
                        {'$set': d},
                        upsert=True
                    ) for d in news_json
                ]
                
                collection.bulk_write(bulk_operations)
                print(f"{query} news updated")
            else:
                print(f"{query} results not found!")

        collection.update_many(
            {"date": {"$type": "string"}},
            [{"$set": {"date": {"$toDate": "$date"}}}]
        )
        
        Mclient.close()
        #time.sleep(3600)