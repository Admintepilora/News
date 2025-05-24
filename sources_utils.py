#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
News Sources Utilities
Manages news sources for the RSS Feed scraper
"""
import sys
from pymongo import MongoClient, UpdateOne
from datetime import datetime

from db_utils import get_mongo_client
from logger import get_logger

# Set up logger
logger = get_logger("SourcesUtils")

# Collection name for sources
SOURCES_COLLECTION = 'NewsSources'

def initialize_sources_collection():
    """Initialize the news sources collection if it doesn't exist"""
    client = get_mongo_client()
    db = client['News']
    
    # Check if collection exists
    if SOURCES_COLLECTION not in db.list_collection_names():
        logger.info(f"Creating {SOURCES_COLLECTION} collection")
        db.create_collection(SOURCES_COLLECTION)
        
        # Set up indexes
        collection = db[SOURCES_COLLECTION]
        collection.create_index([('site', 1), ('RSS', 1)], unique=True)
        
        # Add default sources
        default_sources = [
            {'site': 'www.ft.com', 'RSS': 'https://www.ft.com/news-feed?format=rss', 'source': 'FinancialTimes', 'active': True, 'category': 'finance', 'added_date': datetime.now()},
            {'site': 'www.wsj.com', 'RSS': 'https://feeds.a.dj.com/rss/RSSMarketsMain.xml', 'source': 'WallStreetJournal', 'active': True, 'category': 'finance', 'added_date': datetime.now()},
            {'site': 'www.bloomberg.com', 'RSS': 'https://feeds.bloomberg.com/markets/news.rss', 'source': 'Bloomberg', 'active': True, 'category': 'finance', 'added_date': datetime.now()},
            {'site': 'finance.yahoo.com', 'RSS': 'https://finance.yahoo.com/news/rssindex', 'source': 'YahooFinance', 'active': True, 'category': 'finance', 'added_date': datetime.now()},
            {'site': 'www.marketwatch.com', 'RSS': 'https://feeds.marketwatch.com/marketwatch/topstories/', 'source': 'MarketWatch', 'active': True, 'category': 'finance', 'added_date': datetime.now()},
            {'site': 'www.zerohedge.com', 'RSS': 'https://feeds.feedburner.com/zerohedge/feed', 'source': 'ZeroHedge', 'active': True, 'category': 'finance', 'added_date': datetime.now()},
            {'site': 'www.politico.com', 'RSS': 'https://rss.politico.com/economy.xml', 'source': 'Politico', 'active': True, 'category': 'politics', 'added_date': datetime.now()},
            {'site': 'www.politico.com', 'RSS': 'https://rss.politico.com/politics-news.xml', 'source': 'Politico', 'active': True, 'category': 'politics', 'added_date': datetime.now()},
            {'site': 'www.politico.eu', 'RSS': 'https://www.politico.eu/rss', 'source': 'PoliticoEurope', 'active': True, 'category': 'politics', 'added_date': datetime.now()},
            {'site': 'www.nasdaq.com', 'RSS': 'https://nasdaqtrader.com/rss.aspx?feed=currentheadlines&categorylist=0', 'source': 'Nasdaq Latest Articles', 'active': True, 'category': 'finance', 'added_date': datetime.now()},
        ]
        
        result = collection.insert_many(default_sources)
        logger.info(f"Added {len(result.inserted_ids)} default sources")
        
    else:
        logger.info(f"{SOURCES_COLLECTION} collection already exists")
    
    client.close()
    return True

def get_active_sources(category=None):
    """Get all active sources, optionally filtered by category"""
    client = get_mongo_client()
    db = client['News']
    collection = db[SOURCES_COLLECTION]
    
    # Build query
    query = {'active': True}
    if category:
        query['category'] = category
    
    # Get sources
    sources = list(collection.find(query))
    client.close()
    
    logger.info(f"Retrieved {len(sources)} active sources" + (f" in category '{category}'" if category else ""))
    return sources

def add_source(site, rss_url, source_name, category='general', active=True):
    """Add a new news source"""
    client = get_mongo_client()
    db = client['News']
    collection = db[SOURCES_COLLECTION]
    
    # Check if source already exists
    existing = collection.find_one({'site': site, 'RSS': rss_url})
    if existing:
        logger.warning(f"Source already exists: {site} - {rss_url}")
        client.close()
        return False
    
    # Create new source document
    source = {
        'site': site,
        'RSS': rss_url,
        'source': source_name,
        'active': active,
        'category': category,
        'added_date': datetime.now(),
        'last_updated': datetime.now()
    }
    
    # Insert new source
    result = collection.insert_one(source)
    client.close()
    
    if result.inserted_id:
        logger.info(f"Added new source: {source_name} ({site})")
        return True
    else:
        logger.error(f"Failed to add source: {source_name} ({site})")
        return False

def update_source(site, rss_url, updates):
    """Update an existing news source"""
    client = get_mongo_client()
    db = client['News']
    collection = db[SOURCES_COLLECTION]
    
    # Set last_updated timestamp
    updates['last_updated'] = datetime.now()
    
    # Update source
    result = collection.update_one(
        {'site': site, 'RSS': rss_url},
        {'$set': updates}
    )
    
    client.close()
    
    if result.modified_count > 0:
        logger.info(f"Updated source: {site} - {rss_url}")
        return True
    else:
        logger.warning(f"Source not found or not modified: {site} - {rss_url}")
        return False

def toggle_source(site, rss_url, active=None):
    """Toggle source active status, or set to specific state"""
    client = get_mongo_client()
    db = client['News']
    collection = db[SOURCES_COLLECTION]
    
    # Get current state if no specific state provided
    if active is None:
        source = collection.find_one({'site': site, 'RSS': rss_url})
        if not source:
            logger.warning(f"Source not found: {site} - {rss_url}")
            client.close()
            return False
        
        # Toggle state
        active = not source.get('active', True)
    
    # Update source
    result = collection.update_one(
        {'site': site, 'RSS': rss_url},
        {'$set': {'active': active, 'last_updated': datetime.now()}}
    )
    
    client.close()
    
    if result.modified_count > 0:
        status = "activated" if active else "deactivated"
        logger.info(f"Source {status}: {site} - {rss_url}")
        return True
    else:
        logger.warning(f"Source not found or not modified: {site} - {rss_url}")
        return False

def remove_source(site, rss_url):
    """Remove a news source"""
    client = get_mongo_client()
    db = client['News']
    collection = db[SOURCES_COLLECTION]
    
    # Delete source
    result = collection.delete_one({'site': site, 'RSS': rss_url})
    
    client.close()
    
    if result.deleted_count > 0:
        logger.info(f"Removed source: {site} - {rss_url}")
        return True
    else:
        logger.warning(f"Source not found: {site} - {rss_url}")
        return False

def list_sources(active_only=False, category=None):
    """List all sources with optional filtering"""
    client = get_mongo_client()
    db = client['News']
    collection = db[SOURCES_COLLECTION]
    
    # Build query
    query = {}
    if active_only:
        query['active'] = True
    if category:
        query['category'] = category
    
    # Get sources
    sources = list(collection.find(query).sort('category', 1))
    client.close()
    
    return sources

def print_sources(sources=None):
    """Print sources in a readable format"""
    if sources is None:
        sources = list_sources()
    
    if not sources:
        print("No sources found")
        return
    
    print(f"Found {len(sources)} sources:")
    print("-" * 80)
    
    current_category = None
    for source in sources:
        # Print category header if changed
        if source.get('category') != current_category:
            current_category = source.get('category')
            print(f"\nCategory: {current_category.upper()}")
            print("-" * 80)
        
        # Print source info
        status = "ACTIVE" if source.get('active', True) else "INACTIVE"
        print(f"{source.get('source', 'Unknown')} ({status})")
        print(f"  Site: {source.get('site', 'Unknown')}")
        print(f"  RSS: {source.get('RSS', 'Unknown')}")
        if 'added_date' in source:
            added_date = source['added_date']
            if isinstance(added_date, datetime):
                added_date = added_date.strftime('%Y-%m-%d %H:%M:%S')
            print(f"  Added: {added_date}")
        print("-" * 80)

if __name__ == "__main__":
    # Parse command line arguments
    import argparse
    parser = argparse.ArgumentParser(description='News Sources Manager')
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Initialize command
    subparsers.add_parser('init', help='Initialize sources collection')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List sources')
    list_parser.add_argument('--all', action='store_true', help='List all sources including inactive')
    list_parser.add_argument('--category', help='Filter by category')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add a new source')
    add_parser.add_argument('--site', required=True, help='Website domain (e.g., example.com)')
    add_parser.add_argument('--rss', required=True, help='RSS feed URL')
    add_parser.add_argument('--name', required=True, help='Source name (e.g., Example News)')
    add_parser.add_argument('--category', default='general', help='Source category')
    add_parser.add_argument('--inactive', action='store_true', help='Add as inactive')
    
    # Toggle command
    toggle_parser = subparsers.add_parser('toggle', help='Toggle source active status')
    toggle_parser.add_argument('--site', required=True, help='Website domain')
    toggle_parser.add_argument('--rss', required=True, help='RSS feed URL')
    toggle_parser.add_argument('--activate', action='store_true', help='Explicitly activate')
    toggle_parser.add_argument('--deactivate', action='store_true', help='Explicitly deactivate')
    
    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a source')
    remove_parser.add_argument('--site', required=True, help='Website domain')
    remove_parser.add_argument('--rss', required=True, help='RSS feed URL')
    
    args = parser.parse_args()
    
    # Process commands
    if args.command == 'init':
        initialize_sources_collection()
        print("Sources collection initialized")
    
    elif args.command == 'list':
        sources = list_sources(active_only=not args.all, category=args.category)
        print_sources(sources)
    
    elif args.command == 'add':
        active = not args.inactive
        if add_source(args.site, args.rss, args.name, args.category, active):
            print(f"Added new source: {args.name} ({args.site})")
        else:
            print(f"Failed to add source: {args.name} ({args.site})")
    
    elif args.command == 'toggle':
        active = None
        if args.activate:
            active = True
        elif args.deactivate:
            active = False
        
        if toggle_source(args.site, args.rss, active):
            status = "activated" if active else "deactivated" if active is not None else "toggled"
            print(f"Source {status}: {args.site} - {args.rss}")
        else:
            print(f"Failed to toggle source: {args.site} - {args.rss}")
    
    elif args.command == 'remove':
        if remove_source(args.site, args.rss):
            print(f"Removed source: {args.site} - {args.rss}")
        else:
            print(f"Failed to remove source: {args.site} - {args.rss}")
    
    else:
        parser.print_help()