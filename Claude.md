# News Scraper System - Technical Documentation

## Architecture Overview

This is a **MongoDB-only news aggregation system** that has been simplified from a dual-database architecture. ClickHouse was removed due to memory limitations and complexity issues.

## System Status

- **Database**: MongoDB only (ClickHouse removed)
- **Articles**: 1.1M+ unique articles
- **Duplicates**: 0% (100% unique URLs)
- **Performance**: Fast and reliable

## Key Components

### Core Scripts
- `main.py` - Main orchestrator and CLI interface
- `DuckDuckGoApiNews.py` - DuckDuckGo news scraper
- `GNewsApiNews.py` - Google News scraper  
- `WebSitesNews.py` - RSS feeds scraper

### Database Layer
- `db_utils.py` - MongoDB-only utilities (ClickHouse removed)
- `config.py` - Simplified MongoDB-only configuration

### Utilities
- `topics.py` - Topic/query management
- `proxy_utils.py` - Proxy rotation
- `logger.py` - Logging framework

## Removed Components (Archived)

The following ClickHouse-related files have been moved to `archive/clickhouse_removed/`:
- `clickhouse_utils.py`
- `db_sync.py` 
- `sync_utils.py`
- All deduplication scripts (`*dedup*.py`)
- ClickHouse migration scripts
- ClickHouse configuration files

## Database Operations

### Basic Commands
```bash
# Check database stats
python3 db_utils.py --stats

# Count articles
python3 db_utils.py --count

# Start scrapers
python3 main.py start

# Check status
python3 main.py status

# Search articles
python3 main.py search --query "your topic"
```

### MongoDB Schema
```javascript
{
  "_id": ObjectId,
  "url": String (unique),
  "title": String,
  "body": String,
  "date": Date,
  "source": String,
  "searchKey": String (optional),
  "image": String (optional)
}
```

## Development Notes

### Why ClickHouse Was Removed
1. **Memory Limitations**: 3.35GB limit insufficient for 2.6M articles
2. **High Duplication**: 93% duplicate rate required complex workarounds
3. **Performance Issues**: Simple queries like `SELECT uniq(url)` failed
4. **Maintenance Overhead**: Required specialized knowledge and monitoring

### Current MongoDB Benefits
1. **Zero Duplicates**: Native upsert handling by URL
2. **Consistent Performance**: No memory limit issues
3. **Simple Operations**: Standard CRUD without workarounds
4. **Easy Maintenance**: Standard MongoDB operations

### Key Design Decisions
- **Deduplication**: Handled at application level via upsert operations
- **Storage**: MongoDB with URL as natural unique key
- **Architecture**: Single database reduces complexity
- **Performance**: 1.1M+ articles with 0% duplicates

## Testing Commands

```bash
# Test MongoDB connection and stats
python3 db_utils.py --stats

# Test scrapers individually
python3 DuckDuckGoApiNews.py --query "test" --no-wait
python3 GNewsApiNews.py --query "test" --no-wait

# Test full system
python3 main.py fly "test query"
```

## Monitoring

### Log Files
- `logs/main.log` - Main orchestrator
- `logs/DuckDuckGoApi.log` - DuckDuckGo scraper
- `logs/GNewsApi.log` - Google News scraper
- `logs/WebSitesNews.log` - Website scraper

### Status Files
- `processes.json` - Running processes
- `status.json` - System status

## Configuration

### MongoDB Settings (`config.py`)
```python
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'newsadmin',
    'password': 'newspassword'
}
```

## Troubleshooting

### Common Issues
1. **MongoDB Connection**: Check MongoDB service status
2. **Duplicate Articles**: Should be 0% - check upsert logic if issues
3. **Scraper Errors**: Check proxy configuration and API limits
4. **Performance**: MongoDB handles large datasets efficiently

### Debug Commands
```bash
# Check MongoDB collection stats
python3 db_utils.py --stats

# Verify no duplicates
python3 db_utils.py --count

# Check scraper logs
tail -f logs/DuckDuckGoApi.log
```

## Migration History

- **Previous**: Dual database (MongoDB + ClickHouse) with complex sync
- **Current**: MongoDB-only with natural deduplication
- **Reason**: Simplified architecture, better performance, easier maintenance
- **Result**: 1.1M+ clean articles, 0% duplicates, reliable operations

## God's Laws
- Keep it simple, less code is better
- No Classes
- Few functions, only when needed
- Less code is better
- Less code is better
- Less code is better
- Less code is better
- Less code is better

## Coding Principles

All implementations follow these core principles:

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