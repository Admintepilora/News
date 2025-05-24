# News Aggregation System - Simple Approach

## God's Laws
- Keep it simple, less code is better
- No Classes
- Few functions, only when needed
- Less code is better
- Less code is better
- Less code is better
- Less code is better
- Less code is better

## Current Architecture Analysis

The system consists of three main news scrapers, each running as a separate process:

1. **DuckDuckGoApiNews.py**: Uses DuckDuckGo search API to collect news based on predefined queries
2. **GNewsApiNews.py**: Uses Google News API to collect news based on the same predefined queries
3. **WebSitesNews.py**: Collects news from specific financial/news websites via RSS feeds

We'll keep this approach of three separate processes, but improve common components.

All scripts store data in MongoDB, using Tor proxies for anonymized requests. The system currently runs with:
- Fixed predefined topic lists
- No on-demand search capability
- No user interface for management
- No integration with social platforms
- No AI-powered content generation

## Coding Principles

All implementations will follow these core principles:

1. **Simplicity First**
   - No classes - use functions and basic data structures only
   - Functions only when strictly needed - prefer direct linear code
   - Order functions in the order they are called (fundamental functions first)
   - Less code is better - aim for readability and maintainability

2. **Maintainability**
   - Minimal and focused logging - log only essential information
   - Each script must include a working `if __name__ == "__main__"` section for standalone testing
   - Use descriptive variable names and minimal comments
   - Keep functions small and focused on a single responsibility

3. **Resource Efficiency**
   - Optimize for memory usage and processing efficiency
   - Clean up resources when no longer needed
   - Use generators and iterators for processing large datasets
   - Add circuit breakers to prevent overwhelming external resources

## Enhancement Plan (Backend Focus)

### 1. Process Optimization

#### 1.1 Centralized Query Management
- Create a simple `topics.py` module as a single source of truth for queries
- Keep using MongoDB for topic storage (no changes to database)
- Add topic priority levels as integer values in MongoDB
- Export simple functions to add/remove/update topics

```python
# Example structure for topics.py
import pymongo

# Connection setup at module level
client = pymongo.MongoClient('connection_string')
db = client['News']
topics_collection = db['Topics']

def get_all_topics():
    """Get all active topics sorted by priority"""
    return list(topics_collection.find({'active': True}).sort('priority', 1))

def add_topic(query, priority=5, active=True):
    """Add a new topic for tracking"""
    topics_collection.update_one(
        {'query': query}, 
        {'$set': {'query': query, 'priority': priority, 'active': active}},
        upsert=True
    )

if __name__ == "__main__":
    # Test code for direct execution
    print("Current topics:")
    for topic in get_all_topics():
        print(f"{topic['query']} (Priority: {topic['priority']})")
```

#### 1.2 Efficient Processing
- Use Python's built-in `concurrent.futures` for parallel processing
- Create a simple master scheduler using APScheduler
- Implement staggered execution to avoid resource spikes

```python
# Example structure for scheduler.py
import time
import logging
from apscheduler.schedulers.background import BackgroundScheduler
import concurrent.futures

# Simple setup - no classes
scheduler = BackgroundScheduler()
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def run_scraper(scraper_func, max_workers=3):
    """Run a scraper function with topics in parallel"""
    from topics import get_all_topics
    
    topics = get_all_topics()
    logging.info(f"Running {scraper_func.__name__} with {len(topics)} topics")
    
    start_time = time.time()
    with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(scraper_func, topic['query']): topic['query'] for topic in topics}
        for future in concurrent.futures.as_completed(futures):
            query = futures[future]
            try:
                result = future.result()
                logging.info(f"Completed: {query} with {result} results")
            except Exception as e:
                logging.error(f"Error with {query}: {e}")
    
    logging.info(f"Completed {scraper_func.__name__} in {time.time() - start_time:.2f} seconds")

if __name__ == "__main__":
    # Test code for direct execution
    from duck_search import search_ddg_news
    
    run_scraper(search_ddg_news, max_workers=2)
```

#### 1.3 Error Handling and Resilience
- Use focused and minimal logging - only record errors and key events
- Add simple retry logic with exponential backoff
- Implement basic circuit breakers for external APIs

```python
# Example structure for resilience.py
import time
import random
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')

def with_retry(func, max_retries=3, initial_delay=1):
    """Simple retry decorator with exponential backoff"""
    def wrapper(*args, **kwargs):
        retries = 0
        delay = initial_delay
        
        while retries <= max_retries:
            try:
                return func(*args, **kwargs)
            except Exception as e:
                retries += 1
                if retries > max_retries:
                    logging.error(f"Failed after {max_retries} retries: {e}")
                    raise
                
                # Exponential backoff with jitter
                sleep_time = delay * (2 ** retries) + random.uniform(0, 1)
                logging.info(f"Retry {retries} after {sleep_time:.2f} seconds")
                time.sleep(sleep_time)
        
    return wrapper

if __name__ == "__main__":
    # Test the retry logic
    @with_retry
    def flaky_function():
        if random.random() < 0.7:  # 70% chance of failure
            raise ValueError("Random failure")
        return "Success!"
    
    try:
        result = flaky_function()
        print(f"Result: {result}")
    except Exception as e:
        print(f"Final error: {e}")
```

#### 1.4 Caching
- Consider Redis for caching if lighter than current approach
- If Redis not viable, use Python's built-in `functools.lru_cache` for function-level caching
- Add simple time-based cache expiration

```python
# Example structure for caching.py
import time
import functools

# Simple time-based cache for function results
cache = {}

def timed_cache(seconds=300):
    """Simple time-based cache decorator"""
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            key = str(args) + str(sorted(kwargs.items()))
            
            # Check if cached and not expired
            current_time = time.time()
            if key in cache and current_time - cache[key]['timestamp'] < seconds:
                return cache[key]['result']
            
            # Call function and cache result
            result = func(*args, **kwargs)
            cache[key] = {
                'result': result,
                'timestamp': current_time
            }
            
            # Cleanup expired items occasionally
            if random.random() < 0.1:  # 10% chance to clean on each call
                for k in list(cache.keys()):
                    if current_time - cache[k]['timestamp'] >= seconds:
                        del cache[k]
            
            return result
        return wrapper
    return decorator

if __name__ == "__main__":
    # Test the cache
    @timed_cache(seconds=5)
    def expensive_operation(x):
        print(f"Computing for {x}...")
        time.sleep(1)  # Simulate expensive operation
        return x * 2
    
    for _ in range(3):
        print(expensive_operation(10))  # Should only compute once
    
    time.sleep(6)  # Wait for cache to expire
    print(expensive_operation(10))  # Should recompute
```

### 2. On-Demand Search Functionality

#### 2.1 Lightweight API Endpoints
- Create simple API endpoints using Flask in a separate server
- Implement basic authentication
- Keep backend processing separate from API handling

```python
# Example structure for search_api.py (would run on API server)
from flask import Flask, request, jsonify
from flask_httpauth import HTTPBasicAuth
import requests

app = Flask(__name__)
auth = HTTPBasicAuth()
BACKEND_URL = "http://backend-server:5001/internal_api"

# Simple user DB - would be moved to a proper store in production
users = {
    "admin": "password123",
}

@auth.verify_password
def verify_password(username, password):
    if username in users and users[username] == password:
        return username

@app.route('/api/search', methods=['GET'])
@auth.login_required
def search_news():
    query = request.args.get('query', '')
    days = request.args.get('days', 7)
    sources = request.args.get('sources', '').split(',')
    
    if not query:
        return jsonify({"error": "Query parameter is required"}), 400
        
    # Forward to backend (will be via internal API)
    response = requests.get(
        f"{BACKEND_URL}/search",
        params={"query": query, "days": days, "sources": ','.join(sources)}
    )
    
    return jsonify(response.json())

if __name__ == "__main__":
    # Test code for direct execution
    app.run(debug=True, port=5000)
```

#### 2.2 Search Engine
- Implement a unified search function for MongoDB
- Keep direct searches to MongoDB for simplicity
- Add basic relevance ranking by recency and keyword matches

```python
# Example structure for search_engine.py
import pymongo
import re
from datetime import datetime, timedelta

# Connection setup
client = pymongo.MongoClient('connection_string')
db = client['News']
news_collection = db['News']

def search_news_db(query, days=7, sources=None):
    """Search news in MongoDB with basic relevance ranking"""
    date_cutoff = datetime.now() - timedelta(days=days)
    
    # Build query
    mongo_query = {
        'date': {'$gte': date_cutoff},
        '$or': [
            {'title': {'$regex': re.escape(query), '$options': 'i'}},
            {'body': {'$regex': re.escape(query), '$options': 'i'}}
        ]
    }
    
    if sources and sources[0]:  # Non-empty list
        mongo_query['source'] = {'$in': sources}
    
    # Get articles and sort by a basic relevance score
    articles = list(news_collection.find(mongo_query))
    
    # Simple ranking: recent + keyword count in title is better
    for article in articles:
        # Days from now (0-7 range)
        days_old = (datetime.now() - article['date']).days
        recency_score = 1.0 - min(days_old / 7.0, 1.0)
        
        # Keyword matches
        title_matches = len(re.findall(re.escape(query), article['title'], re.I))
        body_matches = len(re.findall(re.escape(query), article['body'], re.I)) 
        
        # Combined score
        article['_score'] = (recency_score * 5) + (title_matches * 3) + (body_matches * 0.2)
    
    return sorted(articles, key=lambda x: x['_score'], reverse=True)

if __name__ == "__main__":
    # Test code for direct execution
    results = search_news_db("inflation", days=30)
    print(f"Found {len(results)} results")
    for i, article in enumerate(results[:5]):
        print(f"{i+1}. {article['title']} ({article['date'].strftime('%Y-%m-%d')}) - Score: {article['_score']:.2f}")
```

#### 2.3 Results Processing
- Standardize news article format
- Add simple deduplication by URL and title similarity
- Extract key entities using basic NLP techniques

```python
# Example structure for results_processor.py
import re
from difflib import SequenceMatcher

def standardize_article(article, source=None):
    """Convert articles from various sources to standard format"""
    return {
        'title': article.get('title', ''),
        'body': article.get('body', article.get('description', '')),
        'url': article.get('url', ''),
        'date': article.get('date'),
        'source': article.get('source', source),
        'image_url': article.get('image', None),
        'keywords': extract_keywords(article.get('title', '') + ' ' + article.get('body', ''))
    }

def extract_keywords(text, max_keywords=5):
    """Extract basic keywords from text"""
    # Simple keyword extraction - would be enhanced in production
    common_words = {'the', 'and', 'is', 'of', 'to', 'in', 'that', 'it', 'with', 'for', 'as', 'was', 'on'}
    words = re.findall(r'\b[a-zA-Z]{4,15}\b', text.lower())
    word_counts = {}
    
    for word in words:
        if word not in common_words:
            word_counts[word] = word_counts.get(word, 0) + 1
    
    # Get top keywords
    keywords = sorted(word_counts.items(), key=lambda x: x[1], reverse=True)
    return [k[0] for k in keywords[:max_keywords]]

def deduplicate_articles(articles, similarity_threshold=0.85):
    """Remove duplicate articles based on URL and title similarity"""
    unique_articles = []
    urls_seen = set()
    
    for article in articles:
        url = article.get('url', '')
        
        # Skip if URL already seen
        if url in urls_seen and url:
            continue
        
        # Check title similarity with existing articles
        title = article.get('title', '')
        duplicate = False
        
        for existing in unique_articles:
            existing_title = existing.get('title', '')
            if not existing_title or not title:
                continue
                
            similarity = SequenceMatcher(None, title, existing_title).ratio()
            if similarity > similarity_threshold:
                duplicate = True
                break
        
        if not duplicate:
            unique_articles.append(article)
            if url:
                urls_seen.add(url)
    
    return unique_articles

if __name__ == "__main__":
    # Test code for direct execution
    test_articles = [
        {'title': 'Stock Market Soars on Fed News', 'url': 'https://example.com/1'},
        {'title': 'Markets Jump on Federal Reserve Announcement', 'url': 'https://other.com/2'},
        {'title': 'Technology Sector Leads Market Rally', 'url': 'https://example.com/3'},
    ]
    
    unique = deduplicate_articles(test_articles)
    print(f"Reduced from {len(test_articles)} to {len(unique)} articles")
    for article in unique:
        print(f"- {article['title']}")
```

### 3. Dynamic Topic Management

#### 3.1 Topic Storage and Management
- Continue using MongoDB for topic storage
- Add simple command-line scripts for topic management
- Store additional metadata for sources and update frequency

```python
# Example structure for topic_manager.py
import argparse
import pymongo
import json

# MongoDB setup
client = pymongo.MongoClient('connection_string')
db = client['News']
topics_collection = db['Topics']

def list_topics():
    """List all topics in the database"""
    topics = list(topics_collection.find().sort('priority', 1))
    for topic in topics:
        active = "ACTIVE" if topic.get('active', True) else "DISABLED"
        print(f"{topic['query']} - Priority: {topic['priority']} - {active}")
        
def add_topic(query, priority=5, sources=None, update_hours=6):
    """Add or update a topic"""
    topic_data = {
        'query': query,
        'priority': priority,
        'active': True,
        'sources': sources or ["duckduckgo", "gnews", "websites"],
        'update_frequency_hours': update_hours
    }
    
    result = topics_collection.update_one(
        {'query': query},
        {'$set': topic_data},
        upsert=True
    )
    
    if result.upserted_id:
        print(f"Added new topic: {query}")
    else:
        print(f"Updated topic: {query}")

def remove_topic(query):
    """Remove a topic from the database"""
    result = topics_collection.delete_one({'query': query})
    if result.deleted_count:
        print(f"Removed topic: {query}")
    else:
        print(f"Topic not found: {query}")

def toggle_topic(query, active=None):
    """Enable or disable a topic"""
    topic = topics_collection.find_one({'query': query})
    if not topic:
        print(f"Topic not found: {query}")
        return
        
    # Toggle if active not specified
    new_state = not topic.get('active', True) if active is None else active
    
    topics_collection.update_one(
        {'query': query},
        {'$set': {'active': new_state}}
    )
    
    state_str = "enabled" if new_state else "disabled"
    print(f"Topic {query} {state_str}")

if __name__ == "__main__":
    # Command-line interface for topic management
    parser = argparse.ArgumentParser(description='Manage news topics')
    subparsers = parser.add_subparsers(dest='command', help='Command to run')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all topics')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add or update a topic')
    add_parser.add_argument('query', help='Search query for the topic')
    add_parser.add_argument('--priority', type=int, default=5, help='Priority (1-10, lower is higher priority)')
    add_parser.add_argument('--sources', nargs='+', help='Data sources to use')
    add_parser.add_argument('--hours', type=int, default=6, help='Update frequency in hours')
    
    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a topic')
    remove_parser.add_argument('query', help='Topic to remove')
    
    # Toggle command
    toggle_parser = subparsers.add_parser('toggle', help='Enable or disable a topic')
    toggle_parser.add_argument('query', help='Topic to toggle')
    toggle_parser.add_argument('--enable', action='store_true', help='Explicitly enable')
    toggle_parser.add_argument('--disable', action='store_true', help='Explicitly disable')
    
    # Parse and dispatch
    args = parser.parse_args()
    
    if args.command == 'list':
        list_topics()
    elif args.command == 'add':
        add_topic(args.query, args.priority, args.sources, args.hours)
    elif args.command == 'remove':
        remove_topic(args.query)
    elif args.command == 'toggle':
        active = True if args.enable else (False if args.disable else None)
        toggle_topic(args.query, active)
    else:
        parser.print_help()
```

#### 3.2 Topic Scheduler
- Use APScheduler for lightweight scheduling
- Implement staggered execution based on topic priority
- Create simple dispatcher for all scrapers

```python
# Example structure for topic_scheduler.py
from apscheduler.schedulers.background import BackgroundScheduler
from datetime import datetime, timedelta
import random
import time
import logging

# Setup
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(message)s')
scheduler = BackgroundScheduler()

def schedule_topics():
    """Schedule all active topics based on their update frequency"""
    from topics import get_all_topics
    import duck_search
    import gnews_search
    import website_scraper
    
    # Clear existing jobs
    scheduler.remove_all_jobs()
    
    # Get all active topics
    topics = get_all_topics()
    logging.info(f"Scheduling {len(topics)} active topics")
    
    for i, topic in enumerate(topics):
        query = topic['query']
        hours = topic.get('update_frequency_hours', 6)
        
        # Stagger start times to avoid spikes
        start_delay = random.randint(1, 60) * (i + 1) % 60  # 1-60 minutes staggered
        start_time = datetime.now() + timedelta(minutes=start_delay)
        
        # Add jobs for each source
        sources = topic.get('sources', ["duckduckgo", "gnews", "websites"])
        
        if "duckduckgo" in sources:
            scheduler.add_job(
                duck_search.search_and_save,
                'interval', 
                hours=hours,
                start_date=start_time,
                args=[query],
                id=f"duck_{query}",
                replace_existing=True
            )
            
        if "gnews" in sources:
            # Add 20 minutes to stagger further
            gnews_time = start_time + timedelta(minutes=20)
            scheduler.add_job(
                gnews_search.search_and_save,
                'interval', 
                hours=hours,
                start_date=gnews_time,
                args=[query],
                id=f"gnews_{query}",
                replace_existing=True
            )
            
        if "websites" in sources and topic.get('priority', 5) <= 3:
            # Only high priority topics (1-3) use website scraping
            # Add 40 minutes to stagger further
            web_time = start_time + timedelta(minutes=40)
            scheduler.add_job(
                website_scraper.search_websites,
                'interval', 
                hours=hours,
                start_date=web_time,
                args=[query],
                id=f"web_{query}",
                replace_existing=True
            )
    
    # Add a job to refresh the schedule daily
    scheduler.add_job(
        schedule_topics,
        'interval',
        days=1,
        id='refresh_schedule',
        replace_existing=True
    )
    
    logging.info(f"Scheduled {len(scheduler.get_jobs())} jobs")

def start_scheduler():
    """Start the scheduler with initial schedule"""
    schedule_topics()
    scheduler.start()
    logging.info("Scheduler started")
    
def stop_scheduler():
    """Stop the scheduler"""
    scheduler.shutdown()
    logging.info("Scheduler stopped")

if __name__ == "__main__":
    # Test code for direct execution
    logging.info("Starting scheduler for testing...")
    start_scheduler()
    
    try:
        # Keep running for test
        while True:
            time.sleep(60)
            logging.info(f"Scheduler running with {len(scheduler.get_jobs())} jobs")
    except KeyboardInterrupt:
        logging.info("Stopping scheduler...")
        stop_scheduler()
```

### 4. Social Media Integration and Other Features

The remaining features would follow the same pattern - simple functional code with no classes, direct calls, and minimal dependencies. The backend services would all run on the current server, while APIs, dashboards, and other user-facing components would be deployed on a separate server.

## Simple Implementation Plan

### Common Components
- `topics.py`: Stores all search queries in one location
- `db_utils.py`: Common database functions for MongoDB
- `proxy_utils.py`: Simplified Tor proxy management

### Scraper Updates
- Each scraper (DuckDuckGoApiNews.py, GNewsApiNews.py, WebSitesNews.py) remains a separate process
- Update each to use the common components
- Standardize error handling and logging
- Add test mode for easy verification

### Execution
- Each scraper can still be run independently
- Run scripts will start the scrapers in separate processes
- Keep separate logs for each scraper for easy debugging

## Technical Architecture (Two Server Model)

```
┌──────────────────────────────────────────────────────┐
│                    BACKEND SERVER                     │
│                                                       │
│  ┌─────────────┐     ┌─────────────┐     ┌─────────┐  │
│  │             │     │             │     │         │  │
│  │ Scrapers    │     │ Schedulers  │     │ Topics  │  │
│  │             │     │             │     │         │  │
│  └──────┬──────┘     └──────┬──────┘     └────┬────┘  │
│         │                   │                  │       │
│         │                   │                  │       │
│  ┌──────▼───────────────────▼──────────────────▼────┐  │
│  │                                                   │  │
│  │               MongoDB Database                    │  │
│  │                                                   │  │
│  └──────────────────────────┬────────────────────────┘  │
│                             │                           │
│  ┌──────────────────────────▼────────────────────────┐  │
│  │                                                   │  │
│  │          Internal API for Frontend Access         │  │
│  │                                                   │  │
│  └──────────────────────────┬────────────────────────┘  │
└──────────────────────────────┼───────────────────────────┘
                               │
                               │
┌──────────────────────────────▼───────────────────────────┐
│                    FRONTEND SERVER                        │
│                                                           │
│  ┌─────────────────┐     ┌────────────────┐               │
│  │                 │     │                │               │
│  │ Public API      │     │ Dashboard UI   │               │
│  │                 │     │                │               │
│  └─────────┬───────┘     └────────┬───────┘               │
│            │                      │                       │
│            │                      │                       │
│  ┌─────────▼──────────────────────▼───────────┐           │
│  │                                            │           │
│  │          Social Media Connectors           │           │
│  │                                            │           │
│  └─────────────────────┬────────────────────┬─┘           │
│                        │                    │             │
│  ┌────────────────────▼─┐  ┌───────────────▼────────────┐ │
│  │                      │  │                            │ │
│  │    AI Connector      │  │    Analytics & Reporting   │ │
│  │                      │  │                            │ │
│  └──────────────────────┘  └────────────────────────────┘ │
└───────────────────────────────────────────────────────────┘
```

## Lightweight Technology Stack

### Backend Server
- **Language**: Python 3.9+
- **Database**: MongoDB (existing)
- **Caching**: Consider Redis if lighter than current approach
- **Scheduling**: APScheduler (simple scheduler)
- **Process Management**: Supervisor

### Frontend Server (Separate)
- **API Framework**: Flask 
- **Frontend**: Simple Flask with Jinja2 templates
- **Authentication**: Basic HTTP Authentication
- **CSS**: Minimal framework like Pure.css

## Resource Conservation Strategies

1. **Functional Approach**:
   - No classes - pure functions with clear inputs/outputs
   - Minimal function nesting - keep call stacks shallow
   - Use named parameters for clarity

2. **Processing Optimization**:
   - Stagger scheduled tasks to prevent resource spikes
   - Use generators for data processing pipelines
   - Implement circuit breakers for external APIs

3. **Memory Management**:
   - Clean up resources explicitly after use
   - Use connection pooling for database access
   - Implement pagination for large queries

4. **Development Practices**:
   - Each script must be independently testable
   - Functions ordered from most fundamental to derived
   - Only log essential information

This approach will keep the backend server running efficiently while providing all the requested functionality through a well-defined internal API that the frontend server can access.
