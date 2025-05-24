#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Common database utilities for news scrapers
MongoDB-only version (ClickHouse removed)
"""
import os
from pymongo import MongoClient, UpdateOne
from datetime import datetime
import pandas as pd

def get_mongo_client(host='localhost', port=27017, username='newsadmin', password='newspassword'):
    """Create and return MongoDB client"""
    return MongoClient(host, port, username=username, password=password)

def save_articles_to_db(articles, collection_name='News', logger=None):
    """Save news articles to MongoDB with deduplication by URL"""
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    # Check if articles is empty list, None, or empty DataFrame
    if articles is None or (isinstance(articles, list) and len(articles) == 0) or \
       (isinstance(articles, pd.DataFrame) and articles.empty):
        log("No articles to save")
        return 0
    
    # Convert to DataFrame for easier processing if not already
    if not isinstance(articles, pd.DataFrame):
        try:
            df = pd.DataFrame([article for article in articles])
        except Exception as e:
            log_error(f"Error converting articles to DataFrame: {e}")
            return 0
    else:
        df = articles.copy()  # Create a copy to avoid modifying the original
    
    # Make sure required fields exist
    for field in ['url', 'title', 'body', 'date']:
        if field not in df.columns:
            log_error(f"Required field '{field}' missing from articles")
            return 0
    
    # Make sure date is datetime format
    df['date'] = pd.to_datetime(df['date'])
    
    # Convert to dict for MongoDB
    articles_dict = df.to_dict(orient='records')
    
    # Create bulk operations
    bulk_operations = [
        UpdateOne(
            {'url': article['url']},  # filter criteria
            {'$set': article},        # update operation
            upsert=True
        ) 
        for article in articles_dict
    ]
    
    # Save to MongoDB
    client = get_mongo_client()
    db = client['News']
    collection = db[collection_name]
    
    # Avoid "if bulk_operations" which triggers DataFrame truth value error
    if len(bulk_operations) > 0:
        result = collection.bulk_write(bulk_operations)
        client.close()
        return result.upserted_count + result.modified_count
    else:
        client.close()
        return 0

def ensure_date_format():
    """Fix any string dates in the collection by converting to datetime"""
    client = get_mongo_client()
    db = client['News']
    collection = db['News']
    
    result = collection.update_many(
        {"date": {"$type": "string"}},  # Match documents where 'date' is a string
        [{"$set": {"date": {"$toDate": "$date"}}}]  # Use MongoDB's $toDate operator
    )
    
    client.close()
    return result.modified_count

def check_urls_exist(urls_to_check, mongodb_collection='News', logger=None):
    """
    Check if specific URLs already exist in MongoDB
    
    Parameters:
    - urls_to_check: list of URLs to check
    - mongodb_collection: MongoDB collection name
    - logger: optional logger object
    
    Returns:
    - set of URLs that already exist
    """
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    if not urls_to_check:
        return set()
    
    existing_urls = set()
    
    # Check URLs in MongoDB
    try:
        client = get_mongo_client()
        db = client['News']
        collection = db[mongodb_collection]
        
        # Use $in to check only specific URLs
        mongo_existing = collection.find(
            {"url": {"$in": urls_to_check}}, 
            {"url": 1, "_id": 0}
        )
        
        mongo_urls = set([doc["url"] for doc in mongo_existing if "url" in doc])
        existing_urls.update(mongo_urls)
        client.close()
        
        log(f"Found {len(mongo_urls)} existing URLs in MongoDB")
    except Exception as e:
        log_error(f"Error checking URLs in MongoDB: {e}")
    
    log(f"Total: {len(existing_urls)} URLs already exist out of {len(urls_to_check)} checked")
    return existing_urls

# Simplified function for saving articles (MongoDB only)
def save_articles_to_all_dbs(articles, mongodb_collection='News', logger=None, check_duplicates=True):
    """
    Save articles to MongoDB with deduplication
    
    Parameters:
    - articles: articles to save (DataFrame or list of dicts)
    - mongodb_collection: MongoDB collection name
    - logger: optional logger object
    - check_duplicates: if True, check for existing URLs before saving
    """
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    # Convert to DataFrame if not already
    if not isinstance(articles, pd.DataFrame):
        try:
            df = pd.DataFrame([article for article in articles])
        except Exception as e:
            log_error(f"Error converting articles to DataFrame: {e}")
            return {'mongodb': 0, 'total': 0}
    else:
        df = articles.copy()
    
    # Return if no articles
    if df.empty:
        log("No articles to save")
        return {'mongodb': 0, 'total': 0}
    
    # Check for duplicates if requested
    if check_duplicates and 'url' in df.columns:
        urls_to_check = df['url'].tolist()
        existing_urls = check_urls_exist(
            urls_to_check=urls_to_check,
            mongodb_collection=mongodb_collection,
            logger=logger
        )
        
        # Filter out existing articles
        if existing_urls:
            original_count = len(df)
            df = df[~df['url'].isin(existing_urls)]
            skipped = original_count - len(df)
            if skipped > 0:
                log(f"Skipped {skipped} articles that already exist")
            
            # Exit if no new articles
            if df.empty:
                log("No new articles to save")
                return {'mongodb': 0, 'total': 0}
    
    # Save to MongoDB
    mongo_count = 0
    try:
        mongo_articles = df.to_dict(orient='records') if not df.empty else []
        if mongo_articles:
            mongo_count = save_articles_to_db(mongo_articles, collection_name=mongodb_collection, logger=logger)
            log(f"Saved {mongo_count} articles to MongoDB")
    except Exception as e:
        log_error(f"Error saving to MongoDB: {e}")
    
    # Return summary
    return {
        'mongodb': mongo_count,
        'total': mongo_count
    }

def get_articles_count(collection_name='News', logger=None):
    """Get total count of articles in MongoDB collection"""
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    try:
        client = get_mongo_client()
        db = client['News']
        collection = db[collection_name]
        
        count = collection.count_documents({})
        client.close()
        
        log(f"MongoDB collection '{collection_name}' contains {count:,} articles")
        return count
    except Exception as e:
        log_error(f"Error counting articles in MongoDB: {e}")
        return 0

def get_unique_urls_count(collection_name='News', logger=None):
    """Get count of unique URLs in MongoDB collection"""
    log = logger.info if logger else print
    log_error = logger.error if logger else print
    
    try:
        client = get_mongo_client()
        db = client['News']
        collection = db[collection_name]
        
        # Use aggregation to count unique URLs
        pipeline = [
            {"$group": {"_id": "$url"}},
            {"$count": "unique_urls"}
        ]
        
        result = list(collection.aggregate(pipeline))
        unique_count = result[0]['unique_urls'] if result else 0
        client.close()
        
        log(f"MongoDB collection '{collection_name}' contains {unique_count:,} unique URLs")
        return unique_count
    except Exception as e:
        log_error(f"Error counting unique URLs in MongoDB: {e}")
        return 0

if __name__ == "__main__":
    # Test functionality
    import argparse
    
    parser = argparse.ArgumentParser(description='Database utilities for news')
    parser.add_argument('--fix-dates', action='store_true', help='Fix date format in MongoDB')
    parser.add_argument('--count', action='store_true', help='Count articles in MongoDB')
    parser.add_argument('--stats', action='store_true', help='Show database statistics')
    
    args = parser.parse_args()
    
    if args.fix_dates:
        # Test the date format conversion
        fixed_count = ensure_date_format()
        print(f"Fixed {fixed_count} date fields in MongoDB News collection")
    
    if args.count:
        # Count articles
        count = get_articles_count()
        unique_count = get_unique_urls_count()
        print(f"Total articles: {count:,}")
        print(f"Unique URLs: {unique_count:,}")
        duplicates = count - unique_count
        print(f"Duplicates: {duplicates:,} ({duplicates/count*100:.1f}%)" if count > 0 else "Duplicates: 0")
    
    if args.stats:
        # Show comprehensive stats
        print("📊 MongoDB Database Statistics")
        print("=" * 40)
        
        total = get_articles_count()
        unique = get_unique_urls_count()
        duplicates = total - unique
        
        print(f"Total articles: {total:,}")
        print(f"Unique URLs: {unique:,}")
        print(f"Duplicates: {duplicates:,}")
        
        if total > 0:
            print(f"Duplication rate: {duplicates/total*100:.1f}%")
            print(f"Unique rate: {unique/total*100:.1f}%")
        
        print("\n✅ MongoDB-only architecture active")