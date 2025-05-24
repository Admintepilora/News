#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configuration for database access
MongoDB-only version (ClickHouse removed)
"""

# MongoDB Configuration
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'newsadmin',
    'password': 'newspassword'
}

# Global Configuration
USE_MONGODB = True

# Database collection name
DEFAULT_COLLECTION = 'News'

# Database name
DEFAULT_DATABASE = 'News'