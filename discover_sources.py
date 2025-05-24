#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
News Sources Discovery
Discovers potential new RSS sources by:
1. Analyzing existing news articles for referenced sources
2. Checking discovered sources for RSS feeds
3. Suggesting new sources to add to the system
"""
import sys
import re
import time
import argparse
import requests
from urllib.parse import urlparse, urljoin
from bs4 import BeautifulSoup
import feedparser
from datetime import datetime
import random
import pandas as pd

# Import shared modules
from db_utils import get_mongo_client
from sources_utils import add_source, list_sources
from proxy_utils import get_random_proxy
from logger import get_logger

# Set up logger
logger = get_logger("DiscoverSources")

# Headers for HTTP requests (to avoid being blocked)
USER_AGENTS = [
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36',
    'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/92.0.4515.107 Safari/537.36',
    'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11.5; rv:90.0) Gecko/20100101 Firefox/90.0',
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 11_5) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/14.1.2 Safari/605.1.15',
]

def get_headers():
    """Get random user agent headers"""
    return {
        'User-Agent': random.choice(USER_AGENTS),
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
        'Accept-Language': 'en-US,en;q=0.5',
        'Connection': 'keep-alive',
        'Upgrade-Insecure-Requests': '1',
        'Pragma': 'no-cache',
        'Cache-Control': 'no-cache',
    }

def extract_domains_from_articles(days=30, min_occurrence=3):
    """Extract potential source domains from existing articles"""
    client = get_mongo_client()
    db = client['News']
    collection = db['News']
    
    # Calculate date cutoff
    from datetime import datetime, timedelta
    date_cutoff = datetime.now() - timedelta(days=days)
    
    # Get recent articles
    articles = list(collection.find({
        'date': {'$gte': date_cutoff}
    }, {
        'title': 1, 
        'body': 1, 
        'source': 1, 
        'url': 1
    }))
    
    logger.info(f"Found {len(articles)} articles from the last {days} days")
    
    # Extract domains from article URLs
    domains = {}
    for article in articles:
        try:
            # Extract domain from article URL
            article_url = article.get('url', '')
            if article_url:
                domain = urlparse(article_url).netloc
                if domain and domain.startswith('www.'):
                    domain = domain[4:]  # Remove www. prefix
                
                if domain:
                    domains[domain] = domains.get(domain, 0) + 1
            
            # Extract domains from article text
            text = f"{article.get('title', '')} {article.get('body', '')}"
            # Look for URLs in text
            urls = re.findall(r'https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+', text)
            for url in urls:
                domain = urlparse(url).netloc
                if domain and domain.startswith('www.'):
                    domain = domain[4:]  # Remove www. prefix
                
                if domain:
                    domains[domain] = domains.get(domain, 0) + 1
                    
        except Exception as e:
            logger.error(f"Error processing article: {e}")
    
    client.close()
    
    # Filter domains by occurrence
    filtered_domains = {domain: count for domain, count in domains.items() 
                       if count >= min_occurrence}
    
    # Sort by occurrence count (descending)
    sorted_domains = sorted(filtered_domains.items(), key=lambda x: x[1], reverse=True)
    
    return sorted_domains

def get_existing_source_domains():
    """Get domains of existing sources"""
    sources = list_sources()
    domains = set()
    
    for source in sources:
        site = source.get('site', '')
        if site:
            # Remove www. prefix if present
            if site.startswith('www.'):
                site = site[4:]
            domains.add(site)
    
    return domains

def check_rss_feeds(domain, proxy=None, timeout=10):
    """Check if a domain has RSS feeds"""
    if not domain:
        return []
    
    # Add www. prefix if needed
    if not domain.startswith('www.'):
        domain = 'www.' + domain
    
    # Common RSS feed paths
    feed_paths = [
        '/rss',
        '/feed',
        '/feeds',
        '/rss.xml',
        '/atom.xml',
        '/feed.xml',
        '/index.xml',
        '/feeds/posts/default',
        '/sitemap.xml',
        '/news/feed',
        '/news/rss',
        '/blog/feed',
        '/blog/rss',
        '/rss/all.xml',
        '/feed/podcast',
        '/news.rss',
        '/feed/atom',
        '/feeds/news',
        '/feeds/latest',
        '/home/feed',
        '/home/rss',
    ]
    
    rss_feeds = []
    base_url = f"https://{domain}"
    
    # Set up session without proxy for testing
    session = requests.Session()
    
    try:
        # First try to fetch the homepage to look for RSS links
        logger.info(f"Checking {base_url} for RSS feeds")
        response = session.get(base_url, headers=get_headers(), timeout=timeout)
        if response.status_code == 200:
            soup = BeautifulSoup(response.content, 'html.parser')
            
            # Look for RSS links in HTML
            feed_links = soup.find_all('link', type=lambda t: t and ('rss' in t or 'atom' in t))
            
            for link in feed_links:
                href = link.get('href', '')
                if href:
                    # Make relative URLs absolute
                    if not href.startswith(('http://', 'https://')):
                        href = urljoin(base_url, href)
                    
                    rss_feeds.append({
                        'url': href,
                        'title': link.get('title', 'Untitled Feed')
                    })
            
            # Also look for 'a' tags with RSS in the text or href
            a_links = soup.find_all('a', href=True, text=lambda t: t and ('rss' in t.lower() or 'feed' in t.lower() or 'atom' in t.lower()))
            for link in a_links:
                href = link.get('href', '')
                if href:
                    # Make relative URLs absolute
                    if not href.startswith(('http://', 'https://')):
                        href = urljoin(base_url, href)
                    
                    rss_feeds.append({
                        'url': href,
                        'title': link.get_text(strip=True) or 'Untitled Feed'
                    })
        
        # Try common feed paths
        for path in feed_paths:
            try:
                feed_url = f"{base_url}{path}"
                logger.debug(f"Checking potential feed URL: {feed_url}")
                
                # Simple head request to check if URL exists
                head_response = session.head(feed_url, headers=get_headers(), timeout=timeout)
                if head_response.status_code == 200:
                    # If URL exists, check if it's a valid feed
                    feed = feedparser.parse(feed_url)
                    
                    if feed.entries and len(feed.entries) > 0:
                        title = feed.feed.get('title', 'Untitled Feed')
                        rss_feeds.append({
                            'url': feed_url,
                            'title': title
                        })
                        logger.info(f"Found valid RSS feed: {feed_url} - {title}")
            except Exception as e:
                logger.debug(f"Error checking feed path {path}: {e}")
                continue
                
    except Exception as e:
        logger.error(f"Error checking RSS feeds for {domain}: {e}")
    
    # Remove duplicates
    unique_feeds = []
    seen_urls = set()
    for feed in rss_feeds:
        if feed['url'] not in seen_urls:
            seen_urls.add(feed['url'])
            unique_feeds.append(feed)
    
    return unique_feeds

def suggest_new_sources(days=30, min_occurrence=3, check_limit=20):
    """Suggest new sources based on article analysis"""
    # Get domains from existing articles
    domains = extract_domains_from_articles(days, min_occurrence)
    logger.info(f"Found {len(domains)} potential source domains")
    
    # Get existing source domains
    existing_domains = get_existing_source_domains()
    logger.info(f"Found {len(existing_domains)} existing source domains")
    
    # Filter out existing domains
    new_domains = [(domain, count) for domain, count in domains 
                   if domain not in existing_domains]
    logger.info(f"Found {len(new_domains)} new potential source domains")
    
    # Limit the number of domains to check
    domains_to_check = new_domains[:check_limit]
    
    # Check for RSS feeds
    suggested_sources = []
    for domain, count in domains_to_check:
        logger.info(f"Checking domain: {domain} (referenced {count} times)")
        
        # Use a proxy to avoid rate limiting
        proxy = get_random_proxy()
        
        # Check for RSS feeds
        rss_feeds = check_rss_feeds(domain, proxy=proxy)
        
        # Add to suggested sources if feeds found
        if rss_feeds:
            logger.info(f"Found {len(rss_feeds)} RSS feeds for {domain}")
            for feed in rss_feeds:
                suggested_sources.append({
                    'domain': domain,
                    'feed_url': feed['url'],
                    'feed_title': feed['title'],
                    'references': count
                })
        else:
            logger.info(f"No RSS feeds found for {domain}")
        
        # Wait to avoid rate limiting
        time.sleep(2)
    
    return suggested_sources

def print_suggested_sources(sources):
    """Print suggested sources in a readable format"""
    if not sources:
        print("No new sources to suggest")
        return
    
    print(f"Found {len(sources)} potential new sources:")
    print("-" * 80)
    
    # Convert to DataFrame for easier display
    df = pd.DataFrame(sources)
    
    # Group by domain
    grouped = df.groupby('domain')
    
    for domain, feeds in grouped:
        references = feeds.iloc[0]['references']
        print(f"\nDomain: {domain} (referenced {references} times)")
        print("-" * 80)
        
        for _, feed in feeds.iterrows():
            print(f"  Feed: {feed['feed_title']}")
            print(f"  URL: {feed['feed_url']}")
            print(f"  Add command: python3 sources_utils.py add --site www.{domain} --rss \"{feed['feed_url']}\" --name \"{feed['feed_title']}\"")
            print("-" * 80)

def add_suggested_source(domain, feed_url, feed_title, category='discovered'):
    """Add a suggested source to the database"""
    if not domain.startswith('www.'):
        domain = 'www.' + domain
    
    return add_source(
        site=domain,
        rss_url=feed_url,
        source_name=feed_title,
        category=category,
        active=True
    )

def parse_args():
    """Parse command line arguments"""
    parser = argparse.ArgumentParser(description='News Sources Discovery')
    parser.add_argument('--days', type=int, default=30, 
                       help='Number of days of articles to analyze (default: 30)')
    parser.add_argument('--min-occurrence', type=int, default=3, 
                       help='Minimum number of occurrences to consider a domain (default: 3)')
    parser.add_argument('--limit', type=int, default=20, 
                       help='Maximum number of domains to check for RSS feeds (default: 20)')
    parser.add_argument('--add', action='store_true',
                       help='Automatically add discovered sources')
    parser.add_argument('--category', default='discovered',
                       help='Category for added sources (default: discovered)')
    return parser.parse_args()

if __name__ == "__main__":
    args = parse_args()
    
    logger.info(f"Starting news source discovery (analyzing {args.days} days, min occurrence: {args.min_occurrence})")
    
    # Discover sources
    suggested_sources = suggest_new_sources(
        days=args.days,
        min_occurrence=args.min_occurrence,
        check_limit=args.limit
    )
    
    # Print suggested sources
    print_suggested_sources(suggested_sources)
    
    # Optionally add sources automatically
    if args.add and suggested_sources:
        print("\nAutomatically adding discovered sources:")
        added_count = 0
        
        for source in suggested_sources:
            success = add_suggested_source(
                domain=source['domain'],
                feed_url=source['feed_url'],
                feed_title=source['feed_title'],
                category=args.category
            )
            
            if success:
                print(f"Added: {source['feed_title']} ({source['domain']})")
                added_count += 1
            
            # Wait a bit between additions
            time.sleep(0.5)
        
        print(f"\nAdded {added_count} new sources to the database")
    
    logger.info(f"Discovered {len(suggested_sources)} potential new sources")