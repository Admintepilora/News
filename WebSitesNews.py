from pymongo import MongoClient
from pymongo import UpdateOne
import feedparser
from datetime import datetime
from bson import ObjectId
import json
from dateutil import parser
import time
from datetime import datetime, timezone

def standardize_date(date_str):
    try:
        dt = parser.parse(date_str)
        return dt.astimezone(timezone.utc)  # Returns datetime object
    except (ValueError, OverflowError) as e:
        print(f"Error parsing date: {e}")
        return None

rss_sources = [
    {'site': 'www.ft.com', 'RSS': 'https://www.ft.com/news-feed?format=rss', 'source': 'FinancialTimes'},
    {'site': 'www.wsj.com', 'RSS': 'https://feeds.a.dj.com/rss/RSSMarketsMain.xml', 'source': 'WallStreetJournal'},
    {'site': 'www.bloomberg.com', 'RSS': 'https://feeds.bloomberg.com/markets/news.rss', 'source': 'Bloomberg'},
    {'site': 'finance.yahoo.com', 'RSS': 'https://finance.yahoo.com/news/rssindex', 'source': 'YahooFinance'},
    {'site': 'www.marketwatch.com', 'RSS': 'https://feeds.marketwatch.com/marketwatch/topstories/', 'source': 'MarketWatch'},
    {'site': 'www.zerohedge.com', 'RSS': 'https://feeds.feedburner.com/zerohedge/feed', 'source': 'ZeroHedge'},
    {'site': 'www.politico.com', 'RSS': 'https://rss.politico.com/economy.xml', 'source': 'Politico'},
    {'site': 'www.politico.com', 'RSS': 'https://rss.politico.com/politics-news.xml', 'source': 'Politico'},
    {'site': 'www.politico.eu', 'RSS': 'https://www.politico.eu/rss', 'source': 'PoliticoEurope'},
    {'site': 'www.nasdaq.com', 'RSS': 'https://nasdaqtrader.com/rss.aspx?feed=currentheadlines&categorylist=0', 'source': 'Nasdaq Latest Articles'},]

# Function to Parse RSS Feeds
def parse_rss_feed(rss_url, search_key, source):
    rss_url=site['RSS']
    feed = feedparser.parse(rss_url)
    documents = []
    print(len(feed.entries))
    for entry in feed.entries:
        #print(dir(entry))
        try:
            document = {
                "url": entry.link,  # URL of the news article
                "body": entry.summary if 'summary' in entry else "No Summary Available",  # Article summary
                "date":  standardize_date(entry.published if 'published' in entry else datetime.utcnow().isoformat()),  # Published date,
                "image": None,
                "searchKey": None,
                "source": source,
                "title": entry.title  # Article title
            }
            documents.append(document)
        except Exception as e:
            print(e)
            pass
    return documents

while True:
    try:
        # Main Scraper Loop
        news_json = []
        for site in rss_sources:
            print(f"Processing RSS feed for {site['source']} ({site['site']})")
            documents = parse_rss_feed(site['RSS'], search_key="General", source=site['source'])
            news_json.extend(documents)
        
        
        
        Mclient = MongoClient('13.39.92.143', 27017, username='Andrea', password='Sara')
        db = Mclient['News']
        collection = db['News']
        
        
        bulk_operations = [
            UpdateOne(
                {'url': d['url']},
                {'$set': d},
                upsert=True
            ) for d in news_json
        ]
        
        collection.bulk_write(bulk_operations)
        print('websites news updated')
        print('wating for 30 seconds...')
        time.sleep(30)
    except Exception as e:
        print(datetime.now(),e)
"""
result = collection.update_many(
{"date": {"$type": "string"}},  # Match documents where 'date' is a string
[{"$set": {"date": {"$toDate": "$date"}}}]  # Use MongoDB's $toDate operator
)

print(f"Modified {result.modified_count} documents.")
"""