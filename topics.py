#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Topic management for news scrapers
Provides functions to manage search topics/queries
"""
from pymongo import MongoClient
from datetime import datetime
import sys

# Default queries organized by category
DEFAULT_TOPICS = {
    'markets': [
        'Stock Market', 'Bonds', 'Futures', 'Bond Market',
        'S&P500', 'Nasdaq composite index', 'DAX', 'FTSE', 'CAC', 
        'BTP', 'BUND', 'Nikkei', 'TBond', 'BONOS', 'Treasury', 'OAT'
    ],
    'economy': [
        'Macroeconomic', 'Fiscal Policy', 'Monetary Policy',
        'FED', 'ECB', 'BOJ', 'BoE', 'Unemployment', 'Inflation', 
        'Economic Calendar', 'Wages', 'Consumer Confidence',
        'Powell', 'Lagarde', 'Economy', 'Earnings'
    ],
    'geopolitics': [
        'Trump', 'Russia', 'Putin', 'China', 'Xijinping', 'Iran', 'Israel'
    ],
    'commodities': [
        'OIL', 'WTI', 'Brent', 'Silver', 'Copper', 'Gold', 'Commodities'
    ],
    'currencies': [
        'Exchange Rates', 'Currencies', 'USD', 'EUR', 'YEN', 'Dollar',
        'CHF', 'GBP', 'CNY', 'AUD', 'JPY', 'NZD'
    ]
}

# MongoDB collection names
TOPICS_COLLECTION = 'Topics'
QUERIES_COLLECTION = 'Queries'  # Legacy collection

def get_mongo_client(host='localhost', port=27017, username='newsadmin', password='newspassword'):
    """Create and return MongoDB client"""
    return MongoClient(host, port, username=username, password=password)

def initialize_topics():
    """Initialize topics collection with default topics if empty"""
    client = get_mongo_client()
    db = client['News']
    collection = db[TOPICS_COLLECTION]
    
    # Check if we need to initialize
    if collection.count_documents({}) == 0:
        print("Initializing topics collection with defaults")
        for category, topics in DEFAULT_TOPICS.items():
            for topic in topics:
                add_topic(topic, category=category)
    
    client.close()

def get_topics_from_db(active_only=True, category=None):
    """Get topics from database, optionally filtered by category"""
    client = get_mongo_client()
    db = client['News']
    collection = db[TOPICS_COLLECTION]
    
    # Build query
    query = {}
    if active_only:
        query['active'] = True
    if category:
        query['category'] = category
    
    # Execute query and sort by priority
    topics = list(collection.find(query).sort('priority', 1))
    client.close()
    
    return topics

def get_all_topics():
    """Get all active topics from database"""
    topics = get_topics_from_db(active_only=True)
    return [topic['query'] for topic in topics]

def get_topics_by_category(category):
    """Get active topics for a specific category"""
    topics = get_topics_from_db(active_only=True, category=category)
    return [topic['query'] for topic in topics]

def add_topic(query, category='general', priority=5, active=True):
    """Add a new topic or update existing one"""
    if not query or not query.strip():
        print("Error: Topic query cannot be empty")
        return False
    
    client = get_mongo_client()
    db = client['News']
    collection = db[TOPICS_COLLECTION]
    
    # Prepare topic document
    topic = {
        'query': query.strip(),
        'category': category,
        'priority': priority,
        'active': active,
        'created': datetime.now(),
        'last_updated': datetime.now()
    }
    
    # Check if topic exists
    existing = collection.find_one({'query': query})
    
    if existing:
        # Update existing topic
        result = collection.update_one(
            {'query': query},
            {'$set': {
                'category': category,
                'priority': priority,
                'active': active,
                'last_updated': datetime.now()
            }}
        )
        success = result.modified_count > 0
    else:
        # Insert new topic
        result = collection.insert_one(topic)
        success = result.inserted_id is not None
    
    # Also update legacy Queries collection for compatibility
    queries_collection = db[QUERIES_COLLECTION]
    queries_collection.update_one(
        {'Query': query},
        {'$set': {'Query': query}},
        upsert=True
    )
    
    client.close()
    return success

def remove_topic(query):
    """Remove a topic from the database"""
    if not query or not query.strip():
        print("Error: Topic query cannot be empty")
        return False
    
    client = get_mongo_client()
    db = client['News']
    collection = db[TOPICS_COLLECTION]
    
    # Remove from Topics collection
    result = collection.delete_one({'query': query})
    success = result.deleted_count > 0
    
    # Also remove from legacy Queries collection
    queries_collection = db[QUERIES_COLLECTION]
    queries_collection.delete_one({'Query': query})
    
    client.close()
    return success

def toggle_topic(query, active=None):
    """Toggle active status of a topic (or set to specific value)"""
    if not query or not query.strip():
        print("Error: Topic query cannot be empty")
        return False
    
    client = get_mongo_client()
    db = client['News']
    collection = db[TOPICS_COLLECTION]
    
    # Get current topic
    topic = collection.find_one({'query': query})
    if not topic:
        print(f"Topic not found: {query}")
        client.close()
        return False
    
    # Determine new state
    new_state = not topic.get('active', True) if active is None else active
    
    # Update topic
    result = collection.update_one(
        {'query': query},
        {'$set': {'active': new_state, 'last_updated': datetime.now()}}
    )
    
    client.close()
    return result.modified_count > 0

def update_topic_priority(query, priority):
    """Update the priority of a topic"""
    if not query or not query.strip():
        print("Error: Topic query cannot be empty")
        return False
    
    client = get_mongo_client()
    db = client['News']
    collection = db[TOPICS_COLLECTION]
    
    # Update priority
    result = collection.update_one(
        {'query': query},
        {'$set': {'priority': priority, 'last_updated': datetime.now()}}
    )
    
    client.close()
    return result.modified_count > 0

def update_topic_category(query, category):
    """Update the category of a topic"""
    if not query or not query.strip():
        print("Error: Topic query cannot be empty")
        return False
    
    client = get_mongo_client()
    db = client['News']
    collection = db[TOPICS_COLLECTION]
    
    # Update category
    result = collection.update_one(
        {'query': query},
        {'$set': {'category': category, 'last_updated': datetime.now()}}
    )
    
    client.close()
    return result.modified_count > 0

def list_topics(category=None, show_inactive=False):
    """List topics from database with details"""
    topics = get_topics_from_db(active_only=not show_inactive, category=category)
    
    if not topics:
        print("No topics found" + (f" in category '{category}'" if category else ""))
        return
    
    print(f"{'QUERY':<30} {'CATEGORY':<15} {'PRIORITY':<10} {'STATUS':<10}")
    print("-" * 65)
    
    for topic in topics:
        status = "ACTIVE" if topic.get('active', True) else "INACTIVE"
        print(f"{topic['query']:<30} {topic.get('category', 'general'):<15} {topic.get('priority', 5):<10} {status:<10}")

def save_topics_to_db():
    """Save all default topics to MongoDB - for backward compatibility"""
    client = get_mongo_client()
    db = client['News']
    legacy_collection = db[QUERIES_COLLECTION]
    
    all_topics = [topic for category in DEFAULT_TOPICS.values() for topic in category]
    queries_for_mongo = [{'Query': q} for q in all_topics]
    
    # Insert topics with upsert to avoid duplicates
    for query in queries_for_mongo:
        legacy_collection.update_one(query, {'$set': query}, upsert=True)
    
    client.close()
    return len(all_topics)

def run_cli():
    """Command-line interface for topic management"""
    import argparse
    
    parser = argparse.ArgumentParser(description='Manage news topics')
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # List command
    list_parser = subparsers.add_parser('list', help='List all topics')
    list_parser.add_argument('--category', help='Filter by category')
    list_parser.add_argument('--all', action='store_true', help='Show inactive topics too')
    
    # Add command
    add_parser = subparsers.add_parser('add', help='Add a new topic')
    add_parser.add_argument('query', help='Search query')
    add_parser.add_argument('--category', default='general', help='Topic category')
    add_parser.add_argument('--priority', type=int, default=5, help='Priority (1-10, lower = higher priority)')
    add_parser.add_argument('--inactive', action='store_true', help='Add as inactive')
    
    # Remove command
    remove_parser = subparsers.add_parser('remove', help='Remove a topic')
    remove_parser.add_argument('query', help='Topic to remove')
    
    # Toggle command
    toggle_parser = subparsers.add_parser('toggle', help='Toggle topic status')
    toggle_parser.add_argument('query', help='Topic to toggle')
    toggle_parser.add_argument('--enable', action='store_true', help='Explicitly enable')
    toggle_parser.add_argument('--disable', action='store_true', help='Explicitly disable')
    
    # Update priority command
    priority_parser = subparsers.add_parser('priority', help='Update topic priority')
    priority_parser.add_argument('query', help='Topic to update')
    priority_parser.add_argument('priority', type=int, help='New priority (1-10)')
    
    # Update category command
    category_parser = subparsers.add_parser('category', help='Update topic category')
    category_parser.add_argument('query', help='Topic to update')
    category_parser.add_argument('category', help='New category')
    
    # Initialize command
    subparsers.add_parser('init', help='Initialize with default topics')
    
    # Parse args
    args = parser.parse_args()
    
    # Initialize topics collection if needed
    if args.command == 'init':
        initialize_topics()
        print("Topics initialized")
        return
    
    # Process other commands
    if args.command == 'list':
        list_topics(category=args.category, show_inactive=args.all)
    elif args.command == 'add':
        if add_topic(args.query, args.category, args.priority, not args.inactive):
            print(f"Topic added: {args.query}")
        else:
            print(f"Failed to add topic: {args.query}")
    elif args.command == 'remove':
        if remove_topic(args.query):
            print(f"Topic removed: {args.query}")
        else:
            print(f"Topic not found: {args.query}")
    elif args.command == 'toggle':
        active = True if args.enable else (False if args.disable else None)
        if toggle_topic(args.query, active):
            state = "enabled" if active is True else ("disabled" if active is False else "toggled")
            print(f"Topic {args.query} {state}")
        else:
            print(f"Failed to update topic: {args.query}")
    elif args.command == 'priority':
        if update_topic_priority(args.query, args.priority):
            print(f"Updated priority for {args.query} to {args.priority}")
        else:
            print(f"Failed to update topic: {args.query}")
    elif args.command == 'category':
        if update_topic_category(args.query, args.category):
            print(f"Updated category for {args.query} to {args.category}")
        else:
            print(f"Failed to update topic: {args.query}")
    else:
        parser.print_help()

if __name__ == "__main__":
    # Run the CLI when executed directly
    if len(sys.argv) > 1:
        run_cli()
    else:
        # Initialize topics collection if needed
        initialize_topics()
        
        # Print all topics
        print("Current topics:")
        list_topics()