# News Scraper System

A streamlined news aggregation system that collects articles from multiple sources and stores them in MongoDB with automatic deduplication and comprehensive search capabilities.

## Features

- **Multiple Data Sources**:
  - DuckDuckGo News API scraper
  - Google News API scraper
  - Web RSS feeds scraper (Financial Times, WSJ, Bloomberg, etc.)

- **Flexible Search**:
  - Database search of collected articles
  - On-the-fly searches for new content
  - Topic management system

- **Robust Architecture**:
  - Proxy rotation for API access
  - MongoDB-only storage with automatic deduplication
  - Process monitoring and management
  - Comprehensive logging
  - Simplified single-database design

- **Command-line Interface**:
  - Process control (start/stop/restart)
  - Status monitoring
  - Search interface
  - Topic management

## Setup

### Prerequisites

- Python 3.7+
- MongoDB server
- Required Python packages (see below)

### Installation

1. Clone the repository:
   ```bash
   git clone https://github.com/yourusername/news-scraper.git
   cd news-scraper
   ```

2. Install required packages:
   ```bash
   pip install pymongo pandas psutil duckduckgo_search gnews flatten_json
   ```

3. Configure MongoDB:
   - Install MongoDB if not already installed
   - Create a database named "News"
   - Create a user with read/write access to the database

4. Initialize the system:
   ```bash
   python topics.py init  # Initialize topics collection
   ```

## Usage

### Starting the Scrapers

Start all scrapers in continuous mode:
```bash
./run_scrapers.sh start
```

Or use the main orchestrator:
```bash
python main.py start
```

### Checking Status

Check the status of all scrapers:
```bash
python main.py status
```

For continuous monitoring:
```bash
python main.py status --monitor
```

### Searching for News

Search the database for articles:
```bash
python main.py search --query "your search terms" --days 7
```

Run an on-the-fly search to fetch new articles:
```bash
python main.py fly "your search terms"
```

### Managing Topics

List all topics:
```bash
python topics.py list
```

Add a new topic:
```bash
python topics.py add "New Topic" --category markets --priority 3
```

Remove a topic:
```bash
python topics.py remove "Unwanted Topic"
```

Toggle a topic's active status:
```bash
python topics.py toggle "Some Topic" --disable
```

### Database Management

Check database statistics:
```bash
python db_utils.py --stats
```

Count articles and duplicates:
```bash
python db_utils.py --count
```

Fix date formats in database:
```bash
python db_utils.py --fix-dates
```

## System Components

### Main Scrapers

1. **DuckDuckGoApiNews.py**
   - Fetches news from DuckDuckGo Search API
   - Supports on-the-fly searches

2. **GNewsApiNews.py**
   - Fetches news from Google News API
   - Provides wide coverage of sources

3. **WebSitesNews.py**
   - Scrapes RSS feeds from major financial/news websites
   - Direct source integration

### Utility Modules

1. **topics.py**
   - Manages search topics/queries
   - Provides CLI for topic operations

2. **db_utils.py**
   - MongoDB-only integration
   - Efficient article storage and retrieval
   - Automatic URL-based deduplication
   - Database statistics and monitoring

3. **proxy_utils.py**
   - Manages proxy rotation
   - Prevents rate limiting

4. **logger.py**
   - Standardized logging across components
   - Log file management

5. **main.py**
   - Orchestrates all components
   - Provides unified CLI

### Management Scripts

**run_scrapers.sh** - Bash script for managing scraper processes:
- `./run_scrapers.sh start` - Start all scrapers
- `./run_scrapers.sh stop` - Stop all scrapers  
- `./run_scrapers.sh restart` - Restart all scrapers
- `./run_scrapers.sh status` - Check status
- `./run_scrapers.sh test` - Run in test mode

## Architecture

**Simplified MongoDB-Only Design:**

```
┌─────────────────┐     ┌─────────────────┐     ┌─────────────────┐
│ DuckDuckGoAPI   │     │ Google News API │     │ Website RSS     │
└────────┬────────┘     └────────┬────────┘     └────────┬────────┘
         │                       │                       │
         ▼                       ▼                       ▼
┌─────────────────────────────────────────────────────────────────┐
│               MongoDB Database (Auto-Deduplication)             │
│                        1.1M+ Articles                           │
│                      100% Unique URLs                           │
└─────────────────────────────────────────────────────────────────┘
         ▲                       ▲                       ▲
         │                       │                       │
┌────────┴────────┐     ┌────────┴────────┐     ┌────────┴────────┐
│ Search Interface│     │ Topic Management│     │ Process Monitor │
└─────────────────┘     └─────────────────┘     └─────────────────┘
```

**Key Benefits:**
- ✅ **Simplified**: Single database, easier maintenance
- ✅ **Reliable**: No memory limits, consistent performance  
- ✅ **Fast**: Native MongoDB deduplication
- ✅ **Clean**: 0% duplicates, 100% unique URLs

## Log Files

Log files are stored in the `logs/` directory:
- `DuckDuckGoApi.log` - DuckDuckGo scraper logs
- `GNewsApi.log` - Google News scraper logs
- `WebSitesNews.log` - Website scraper logs
- `main.log` - Orchestrator logs

## Troubleshooting

### Common Issues

1. **MongoDB Connection Issues**
   - Check that MongoDB is running
   - Verify credentials in code match your MongoDB setup

2. **API Rate Limiting**
   - Check proxy configuration
   - Decrease scraping frequency

3. **Process Hangs**
   - Use `./run_scrapers.sh restart` to reset all processes
   - Check log files for specific errors

### Process Monitoring

The system creates status files that can help diagnose issues:
- `processes.json` - Tracks running processes
- `status.json` - System status information

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Acknowledgments

- Uses [duckduckgo_search](https://pypi.org/project/duckduckgo-search/) for DuckDuckGo API access
- Uses [gnews](https://pypi.org/project/gnews/) for Google News API access
- Uses PyMongo for MongoDB integration