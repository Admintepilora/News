#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
RSS Feed News Scraper
Collects news from financial/news website RSS feeds
Runs on a continuous basis to monitor RSS feeds
Sources are dynamically loaded from MongoDB
"""
import os
import sys
import time
import argparse
import feedparser
from datetime import datetime, timezone
from dateutil import parser as date_parser

# Import shared modules
from db_utils import save_articles_to_db, save_articles_to_all_dbs, ensure_date_format
from sources_utils import get_active_sources, initialize_sources_collection
from logger import get_logger, log_start, log_end

# Set up logger
logger = get_logger("WebSitesNews")

def standardize_date(date_str):
    """Convert various date formats to UTC datetime object"""
    try:
        dt = date_parser.parse(date_str)
        return dt.astimezone(timezone.utc)  # Returns datetime object
    except (ValueError, OverflowError) as e:
        logger.error(f"Error parsing date: {e}")
        return None

def parse_rss_feed(site):
    """Parse RSS feed and return standardized article data"""
    rss_url = site['RSS']
    source = site['source']
    
    try:
        feed = feedparser.parse(rss_url)
        documents = []
        logger.info(f"Found {len(feed.entries)} entries in feed")
        
        for entry in feed.entries:
            try:
                # Create standardized document
                published_date = entry.published if 'published' in entry else datetime.utcnow().isoformat()
                document = {
                    "url": entry.link,  # URL of the news article
                    "body": entry.summary if 'summary' in entry else "No Summary Available",  # Article summary
                    "date": standardize_date(published_date),  # Published date
                    "image": None,
                    "searchKey": None,
                    "source": source,
                    "title": entry.title  # Article title
                }
                documents.append(document)
            except Exception as e:
                logger.error(f"Error processing entry: {e}")
        
        return documents
    except Exception as e:
        logger.error(f"Error parsing RSS feed: {e}")
        return []

def main(test_mode=False, category=None):
    """Main function to process RSS feeds and save articles"""
    try:
        # Ensure sources collection is initialized
        initialize_sources_collection()
        
        # Get active sources from MongoDB
        all_sources = get_active_sources(category=category)
        
        # Use only first 2 sources in test mode
        sources = all_sources[:2] if test_mode else all_sources
        logger.info(f"Processing {len(sources)} RSS feeds" + (f" in category '{category}'" if category else ""))
        
        all_articles = []
        
        # Process each RSS feed
        for site in sources:
            logger.info(f"Processing RSS feed for {site['source']} ({site['site']})")
            articles = parse_rss_feed(site)
            if articles:
                all_articles.extend(articles)
                logger.info(f"Added {len(articles)} articles from {site['source']}")
            else:
                logger.warning(f"No articles found for {site['source']}")
        
        # Save all articles to database
        if all_articles and len(all_articles) > 0:
            # Salvataggio parallelo su MongoDB e ClickHouse
            result = save_articles_to_all_dbs(
                all_articles, 
                use_mongodb=not args.no_mongodb,
                use_clickhouse=not args.no_clickhouse,
                logger=logger,
                check_across_dbs=True
            )
            logger.info(f"Saved articles - MongoDB: {result['mongodb']} articles, ClickHouse: {result['clickhouse']} articles")
        else:
            logger.warning("No articles to save")
        
        # Ensure dates are in proper format
        fixed_dates = ensure_date_format()
        if fixed_dates and isinstance(fixed_dates, int) and fixed_dates > 0:
            logger.info(f"Fixed {fixed_dates} date fields")
        
        return True
            
    except Exception as e:
        logger.error(f"Error in main RSS process: {e}")
        return False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='RSS Feed Scraper')
    parser.add_argument('--test', action='store_true', help='Run in test mode with limited sources')
    parser.add_argument('--no-wait', action='store_true', help='Exit after one run even in normal mode')
    parser.add_argument('--category', help='Process only sources in this category')
    parser.add_argument('--no-mongodb', action='store_true', help='Disable MongoDB storage')
    parser.add_argument('--no-clickhouse', action='store_true', help='Disable ClickHouse storage')
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    # Check for TEST_MODE environment variable (set by main.py)
    if os.environ.get('TEST_MODE') == '1':
        args.test = True
    
    # Check for NO_WAIT environment variable
    if os.environ.get('NO_WAIT') == '1':
        args.no_wait = True
    
    # Check for CATEGORY environment variable
    if not args.category and 'CATEGORY' in os.environ:
        args.category = os.environ['CATEGORY']
    
    # Start logging
    log_start(logger, "RSS Feed Scraper", test_mode=args.test)
    
    # Run the scraper
    success = main(test_mode=args.test, category=args.category)
    
    # Log results
    if success:
        logger.info("RSS Feed Scraper completed successfully")
    else:
        logger.error("RSS Feed Scraper encountered errors")
    
    # Exit if test mode or no-wait
    if args.test or args.no_wait:
        logger.info("One-time run completed")
        log_end(logger, "RSS Feed Scraper", success=success)
        sys.exit(0 if success else 1)
    
    # Continuous operation
    logger.info("Starting continuous operation with 30-second intervals. Press Ctrl+C to stop.")
    try:
        while True:
            # Wait before next run (30 seconds in normal mode)
            time.sleep(30)
            logger.info(f"Starting new run at {datetime.now()}")
            success = main(test_mode=False, category=args.category)
            if not success:
                logger.warning("Run completed with errors")
    except KeyboardInterrupt:
        logger.info("Stopping RSS Feed Scraper due to user interrupt")
    except Exception as e:
        logger.error(f"Unhandled exception in main loop: {e}")
        success = False
    
    # End logging
    log_end(logger, "RSS Feed Scraper", success=success)
    sys.exit(0 if success else 1)