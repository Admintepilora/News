#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
DuckDuckGo News Scraper
Collects news from DuckDuckGo Search API based on predefined queries
Supports on-the-fly searches via command line arguments
"""
import os
import sys
import time
import argparse
import pandas as pd
from datetime import datetime
from duckduckgo_search import DDGS
from flatten_json import flatten

# Import shared modules
from topics import get_all_topics
from proxy_utils import get_random_proxy
from db_utils import save_articles_to_db, save_articles_to_all_dbs, ensure_date_format
from logger import get_logger, log_start, log_end

def search_news(query, max_results=50, timelimit='d'):
    """Search DuckDuckGo for news articles on a query"""
    proxy = get_random_proxy()
    ddgs = DDGS(proxy=proxy['https'])
    try:
        news = list(ddgs.news(query, max_results=max_results, timelimit=timelimit))
        # Add search query to each result
        for item in news:
            item['searchKey'] = query
        return news
    except Exception as e:
        print(f"Error searching DuckDuckGo for '{query}': {e}")
        return []

def process_articles(articles):
    """Process and deduplicate news articles"""
    if not articles:
        return None
    
    # Convert to DataFrame
    results_df = pd.DataFrame([flatten(r, '.') for r in articles])
    
    if results_df.empty:
        return None
    
    # Sort by date
    results_df = results_df.sort_values(by='date', ascending=False)
    
    # Remove duplicates
    columns_to_check = results_df.columns.difference(['searchKey', 'image'])
    results_df = results_df.drop_duplicates(subset=columns_to_check)
    
    # More filtering
    news = results_df.copy()
    news = news.drop_duplicates(subset=['url'], keep='first')
    news = news.sort_values(by='date', ascending=True)
    
    # Remove Bloomberg connector text
    text_to_remove = "Connecting decision makers to a dynamic network of information"
    news = news[~news['body'].str.contains(text_to_remove, na=False)]
    
    # Clean up DataFrame
    try: del news['level_0']
    except: pass
    try: del news['index']
    except: pass
    
    # Reset index
    news.reset_index(drop=True, inplace=True)
    news.index = range(len(news)-1, -1, -1)
    
    # Ensure date is datetime
    news["date"] = pd.to_datetime(news["date"])
    
    return news

# Set up logger
logger = get_logger("DuckDuckGoApi")

def main(test_mode=False, on_the_fly_query=None):
    """Main function to search and save news articles"""
    try:
        # Determine search approach
        if on_the_fly_query:
            # Single query mode for on-the-fly search
            topics = [on_the_fly_query]
            logger.info(f"On-the-fly search for: {on_the_fly_query}")
        elif test_mode:
            # Test mode with limited topics
            topics = get_all_topics()[:2]  # Just first 2 for testing
            logger.info(f"Test mode: using {len(topics)} topics")
        else:
            # Normal operation with all active topics
            topics = get_all_topics()
            logger.info(f"Using {len(topics)} topics")
        
        for query in topics:
            logger.info(f"{query} news updating...")
            articles = []
            
            # Try up to 3 times to get results
            for attempt in range(3):
                try:
                    results = search_news(query, max_results=50, timelimit='d')
                    if results is not None and isinstance(results, list) and len(results) > 0:  # Check explicitly for non-empty
                        articles.extend(results)
                        logger.info(f"{query} result OK! ({len(results)} articles)")
                        break
                except Exception as e:
                    logger.error(f"Error on attempt {attempt+1}: {e}")
                
                # Wait before retry
                if attempt < 2:
                    time.sleep(5)
            
            # Process and save articles
            if articles:
                news_df = process_articles(articles)
                # Use explicit checks for None and emptiness
                if news_df is not None and isinstance(news_df, pd.DataFrame) and not news_df.empty:
                    # Salvataggio parallelo su MongoDB e ClickHouse con verifica incrociata
                    result = save_articles_to_all_dbs(
                        news_df, 
                        use_mongodb=not args.no_mongodb, 
                        use_clickhouse=not args.no_clickhouse,
                        logger=logger,
                        check_across_dbs=True
                    )
                    logger.info(f"{query} news updated - MongoDB: {result['mongodb']} articles, ClickHouse: {result['clickhouse']} articles")
                else:
                    logger.warning(f"{query} - No valid articles after processing")
            else:
                logger.warning(f"{query} - No results found")
        
        # Ensure dates are in proper format
        fixed_dates = ensure_date_format()
        if fixed_dates is not None and isinstance(fixed_dates, int) and fixed_dates > 0:
            logger.info(f"Fixed {fixed_dates} date fields")
        
        return True
            
    except Exception as e:
        logger.error(f"Error in main DuckDuckGo process: {e}")
        return False

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='DuckDuckGo News Scraper')
    parser.add_argument('--test', action='store_true', help='Run in test mode with limited topics')
    parser.add_argument('--query', help='Run a single on-the-fly search for this query')
    parser.add_argument('--no-wait', action='store_true', help='Exit after one run even in normal mode')
    parser.add_argument('--no-mongodb', action='store_true', help='Disable MongoDB storage')
    parser.add_argument('--no-clickhouse', action='store_true', help='Disable ClickHouse storage')
    return parser.parse_args()

if __name__ == "__main__":
    # Parse command line arguments
    args = parse_args()
    
    # Check for TEST_MODE environment variable (set by main.py)
    if os.environ.get('TEST_MODE') == '1':
        args.test = True
    
    # Check for QUERY environment variable (set by main.py)
    if not args.query and 'QUERY' in os.environ:
        args.query = os.environ['QUERY']
    
    # Check for NO_WAIT environment variable
    if os.environ.get('NO_WAIT') == '1':
        args.no_wait = True
    
    # Start logging
    log_start(logger, "DuckDuckGo News Scraper", test_mode=args.test)
    
    # Run the scraper
    success = main(test_mode=args.test, on_the_fly_query=args.query)
    
    # Log results
    if success:
        logger.info("DuckDuckGo News Scraper completed successfully")
    else:
        logger.error("DuckDuckGo News Scraper encountered errors")
    
    # Exit if test mode, on-the-fly query, or no-wait
    if args.test or args.query or args.no_wait:
        logger.info("One-time run completed")
        log_end(logger, "DuckDuckGo News Scraper", success=success)
        sys.exit(0 if success else 1)
    
    # Continuous operation
    logger.info("Starting continuous operation with 30-second intervals. Press Ctrl+C to stop.")
    try:
        while True:
            # Wait before next run (30 seconds)
            time.sleep(30)
            logger.info(f"Starting new run at {datetime.now()}")
            success = main(test_mode=False)
            if not success:
                logger.warning("Run completed with errors")
    except KeyboardInterrupt:
        logger.info("Stopping DuckDuckGo News Scraper due to user interrupt")
    except Exception as e:
        logger.error(f"Unhandled exception in main loop: {e}")
        success = False
    
    # End logging
    log_end(logger, "DuckDuckGo News Scraper", success=success)
    sys.exit(0 if success else 1)