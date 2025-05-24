#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Main orchestrator for news scrapers
Provides search functionality and manages script execution
"""
import os
import sys
import time
import argparse
import subprocess
import signal
import json
import psutil
from datetime import datetime, timedelta
import pymongo
import re

# Import utilities
from topics import (
    get_all_topics, add_topic, remove_topic, toggle_topic, 
    list_topics, initialize_topics, TOPICS_COLLECTION
)
from logger import get_logger, log_start, log_end
from db_utils import get_mongo_client
# Sync utilities removed - using MongoDB only

# Get logger
logger = get_logger("main")

# Script paths
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DUCK_SCRIPT = os.path.join(SCRIPT_DIR, "DuckDuckGoApiNews.py")
GNEWS_SCRIPT = os.path.join(SCRIPT_DIR, "GNewsApiNews.py")
WEBSITES_SCRIPT = os.path.join(SCRIPT_DIR, "WebSitesNews.py")

# Process tracking
PROCESS_FILE = os.path.join(SCRIPT_DIR, "processes.json")

# Status file for monitoring
STATUS_FILE = os.path.join(SCRIPT_DIR, "status.json")

def save_process_info(process_dict):
    """Save process info to file"""
    with open(PROCESS_FILE, 'w') as f:
        json.dump(process_dict, f)

def load_process_info():
    """Load process info from file"""
    if not os.path.exists(PROCESS_FILE):
        return {}
    
    try:
        with open(PROCESS_FILE, 'r') as f:
            return json.load(f)
    except (json.JSONDecodeError, FileNotFoundError):
        return {}

def update_status():
    """Update status file with current information"""
    status = {
        "last_update": datetime.now().isoformat(),
        "system": {
            "cpu_percent": psutil.cpu_percent(),
            "memory_percent": psutil.virtual_memory().percent,
            "disk_percent": psutil.disk_usage('/').percent
        },
        "processes": {}
    }
    
    processes = load_process_info()
    for name, info in processes.items():
        pid = info.get('pid')
        if pid:
            # Check if process is running
            try:
                # Use psutil to get more detailed information
                process = psutil.Process(pid)
                running = process.is_running()
                
                if running:
                    # Get process stats
                    with process.oneshot():
                        try:
                            cpu_percent = process.cpu_percent(interval=0.1)
                            memory_percent = process.memory_percent()
                            status["processes"][name] = {
                                "pid": pid,
                                "running": True,
                                "start_time": info.get('start_time'),
                                "last_active": info.get('last_active'),
                                "uptime": get_uptime(info.get('start_time')),
                                "cpu_percent": cpu_percent,
                                "memory_percent": memory_percent,
                                "command": process.cmdline()
                            }
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            running = False
                
                if not running:
                    # Process not running
                    status["processes"][name] = {
                        "pid": pid,
                        "running": False,
                        "start_time": info.get('start_time'),
                        "last_active": info.get('last_active'),
                        "status": "stopped"
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                # Process doesn't exist or we can't access it
                status["processes"][name] = {
                    "pid": pid,
                    "running": False,
                    "start_time": info.get('start_time'),
                    "last_active": info.get('last_active'),
                    "status": "not_found"
                }
    
    # Check log files
    status["logs"] = check_log_files()
    
    # Write status file
    with open(STATUS_FILE, 'w') as f:
        json.dump(status, f, indent=2)
    
    return status

def check_log_files():
    """Check log files for each script"""
    log_dir = os.path.join(SCRIPT_DIR, 'logs')
    log_info = {}
    
    if not os.path.exists(log_dir):
        return log_info
    
    for log_file in os.listdir(log_dir):
        if log_file.endswith('.log'):
            script_name = log_file[:-4]  # Remove .log extension
            log_path = os.path.join(log_dir, log_file)
            
            # Get file stats
            stats = os.stat(log_path)
            
            # Get last few lines for error detection
            last_lines = []
            try:
                with open(log_path, 'r') as f:
                    # Read last 10 lines
                    lines = f.readlines()
                    last_lines = lines[-10:] if len(lines) >= 10 else lines
            except Exception as e:
                last_lines = [f"Error reading log: {e}"]
            
            # Check for errors in last lines
            has_errors = any('ERROR' in line for line in last_lines)
            
            log_info[script_name] = {
                "size": stats.st_size,
                "last_modified": datetime.fromtimestamp(stats.st_mtime).isoformat(),
                "has_errors": has_errors,
                "last_lines": last_lines
            }
    
    return log_info

def start_script(script_path, test_mode=False):
    """Start a script as a background process"""
    script_name = os.path.basename(script_path)
    logger.info(f"Starting {script_name}")
    
    # Check if script is already running
    processes = load_process_info()
    if script_name in processes:
        pid = processes[script_name].get('pid')
        if pid:
            try:
                os.kill(pid, 0)  # Check if process is running
                logger.warning(f"{script_name} is already running with PID {pid}")
                return False
            except OSError:
                # Process not running, remove from tracking
                logger.info(f"Process {pid} for {script_name} is not running, will restart")
    
    # Set python executable (use same python as running this script)
    python_exe = sys.executable
    
    # Set environment variable for test mode
    env = os.environ.copy()
    if test_mode:
        env['TEST_MODE'] = '1'
    
    # Start the process
    try:
        process = subprocess.Popen(
            [python_exe, script_path],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        
        # Store process info
        processes[script_name] = {
            'pid': process.pid,
            'script': script_path,
            'start_time': datetime.now().isoformat(),
            'last_active': datetime.now().isoformat()
        }
        save_process_info(processes)
        
        logger.info(f"Started {script_name} with PID {process.pid}")
        return True
    except Exception as e:
        logger.error(f"Error starting {script_name}: {e}")
        return False

def stop_script(script_path):
    """Stop a running script"""
    script_name = os.path.basename(script_path)
    logger.info(f"Stopping {script_name}")
    
    # Get process info
    processes = load_process_info()
    if script_name not in processes:
        logger.warning(f"{script_name} is not being tracked")
        return False
    
    pid = processes[script_name].get('pid')
    if not pid:
        logger.warning(f"No PID found for {script_name}")
        return False
    
    # Try to stop the process
    try:
        os.kill(pid, signal.SIGTERM)
        time.sleep(2)  # Give it time to terminate
        
        # Check if it's still running
        try:
            os.kill(pid, 0)
            # Still running, try SIGKILL
            logger.warning(f"{script_name} didn't terminate with SIGTERM, trying SIGKILL")
            os.kill(pid, signal.SIGKILL)
        except OSError:
            # Process terminated
            pass
        
        # Remove from tracking
        del processes[script_name]
        save_process_info(processes)
        
        logger.info(f"Stopped {script_name} (PID {pid})")
        return True
    except Exception as e:
        logger.error(f"Error stopping {script_name}: {e}")
        return False

def check_scripts_status():
    """Check status of all tracked scripts"""
    processes = load_process_info()
    status = {}
    
    for name, info in processes.items():
        pid = info.get('pid')
        if pid:
            try:
                # Use psutil for more detailed info
                process = psutil.Process(pid)
                running = process.is_running()
                
                if running:
                    # Get memory and CPU usage
                    with process.oneshot():
                        try:
                            cpu_percent = process.cpu_percent(interval=0.1)
                            memory_info = process.memory_info()
                            memory_mb = memory_info.rss / (1024 * 1024)  # Convert to MB
                            
                            status[name] = {
                                'running': True,
                                'pid': pid,
                                'start_time': info.get('start_time'),
                                'uptime': get_uptime(info.get('start_time')),
                                'cpu': f"{cpu_percent:.1f}%",
                                'memory': f"{memory_mb:.1f} MB"
                            }
                        except (psutil.NoSuchProcess, psutil.AccessDenied):
                            running = False
                
                if not running:
                    status[name] = {
                        'running': False,
                        'pid': pid,
                        'start_time': info.get('start_time'),
                        'last_active': info.get('last_active'),
                        'status': 'stopped'
                    }
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                status[name] = {
                    'running': False,
                    'pid': pid,
                    'start_time': info.get('start_time'),
                    'last_active': info.get('last_active'),
                    'status': 'not_found'
                }
    
    return status

def get_uptime(start_time_str):
    """Calculate uptime from start time string"""
    if not start_time_str:
        return "unknown"
    
    try:
        start_time = datetime.fromisoformat(start_time_str)
        uptime = datetime.now() - start_time
        
        days = uptime.days
        hours, remainder = divmod(uptime.seconds, 3600)
        minutes, seconds = divmod(remainder, 60)
        
        parts = []
        if days:
            parts.append(f"{days}d")
        if hours:
            parts.append(f"{hours}h")
        if minutes:
            parts.append(f"{minutes}m")
        if seconds or not parts:
            parts.append(f"{seconds}s")
            
        return " ".join(parts)
    except (ValueError, TypeError):
        return "unknown"

def print_status():
    """Print status of all tracked scripts"""
    status = check_scripts_status()
    
    if not status:
        print("No scripts are being tracked")
        return
    
    # Update status to get latest information
    update_status()
    
    # Check system resources
    system_stats = psutil.virtual_memory()
    print(f"System Status:")
    print(f"  CPU: {psutil.cpu_percent()}%")
    print(f"  Memory: {system_stats.percent}% ({system_stats.used // (1024*1024)} MB used of {system_stats.total // (1024*1024)} MB)")
    print(f"  Disk: {psutil.disk_usage('/').percent}% used\n")
    
    # Print process table
    print(f"{'SCRIPT':<25} {'STATUS':<10} {'PID':<8} {'CPU':<8} {'MEMORY':<12} {'UPTIME':<15}")
    print("-" * 80)
    
    for name, info in status.items():
        status_str = "RUNNING" if info['running'] else "STOPPED"
        pid = info.get('pid', 'N/A')
        uptime = info.get('uptime', 'N/A')
        cpu = info.get('cpu', 'N/A')
        memory = info.get('memory', 'N/A')
        print(f"{name:<25} {status_str:<10} {pid:<8} {cpu:<8} {memory:<12} {uptime:<15}")
    
    # Check log files
    print("\nLog Status:")
    log_dir = os.path.join(SCRIPT_DIR, 'logs')
    if os.path.exists(log_dir):
        for log_file in os.listdir(log_dir):
            if log_file.endswith('.log'):
                script_name = log_file[:-4]  # Remove .log extension
                log_path = os.path.join(log_dir, log_file)
                stats = os.stat(log_path)
                size_kb = stats.st_size / 1024
                mod_time = datetime.fromtimestamp(stats.st_mtime)
                age = datetime.now() - mod_time
                
                # Check for errors
                has_errors = False
                try:
                    with open(log_path, 'r') as f:
                        lines = f.readlines()
                        last_lines = lines[-20:] if len(lines) >= 20 else lines
                        has_errors = any('ERROR' in line for line in last_lines)
                except Exception:
                    pass
                
                status_str = "OK" if not has_errors else "ERRORS"
                print(f"  {script_name:<23} {status_str:<8} {size_kb:.1f} KB  Last update: {mod_time.strftime('%Y-%m-%d %H:%M:%S')} ({int(age.total_seconds()/60)} min ago)")
    else:
        print("  No logs directory found")

def search_news(query, days=7, sources=None, limit=20):
    """Search news database for articles matching query"""
    if not query:
        logger.error("Empty search query")
        return []
    
    logger.info(f"Searching for '{query}' in the last {days} days")
    
    client = get_mongo_client()
    db = client['News']
    collection = db['News']
    
    # Calculate date cutoff
    date_cutoff = datetime.now() - timedelta(days=days)
    
    # Build query
    mongo_query = {
        'date': {'$gte': date_cutoff},
        '$or': [
            {'title': {'$regex': re.escape(query), '$options': 'i'}},
            {'body': {'$regex': re.escape(query), '$options': 'i'}}
        ]
    }
    
    if sources:
        mongo_query['source'] = {'$in': sources}
    
    # Execute query and sort by date (newest first)
    articles = list(collection.find(mongo_query).sort('date', -1).limit(limit))
    
    client.close()
    return articles

def print_search_results(articles):
    """Print search results in a readable format"""
    if not articles:
        print("No results found")
        return
    
    print(f"Found {len(articles)} articles:")
    print("-" * 80)
    
    for i, article in enumerate(articles, 1):
        date_str = article.get('date', 'Unknown date')
        if isinstance(date_str, datetime):
            date_str = date_str.strftime('%Y-%m-%d %H:%M')
        
        print(f"{i}. {article.get('title', 'No title')}")
        print(f"   Source: {article.get('source', 'Unknown source')} | Date: {date_str}")
        print(f"   URL: {article.get('url', 'No URL')}")
        print("-" * 80)

def run_search(args):
    """Run search based on command line arguments"""
    # If no query provided via args, ask for it
    query = args.query
    while not query:
        query = input("Enter search query: ")
        if not query:
            print("Query cannot be empty")
    
    # Run search
    articles = search_news(
        query, 
        days=args.days, 
        sources=args.sources.split(',') if args.sources else None,
        limit=args.limit
    )
    
    # Print results
    print_search_results(articles)
    
    # Log search
    logger.info(f"Search for '{query}' returned {len(articles)} results")
    
    # Return query for potential topic addition
    return query

def on_the_fly_search(query):
    """Run on-the-fly search for a single query"""
    logger.info(f"Running on-the-fly search for '{query}'")
    
    # Add topic temporarily if it doesn't exist
    add_topic(query, category='ondemand', priority=1, active=True)
    
    # Try with each scraper in parallel
    processes = []
    
    # Start DuckDuckGo search
    duck_process = subprocess.Popen(
        [sys.executable, DUCK_SCRIPT, '--query', query, '--no-wait'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ.copy(), 'QUERY': query, 'NO_WAIT': '1'}
    )
    processes.append(('DuckDuckGo', duck_process))
    
    # Start GNews search
    gnews_process = subprocess.Popen(
        [sys.executable, GNEWS_SCRIPT, '--query', query, '--no-wait'],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        env={**os.environ.copy(), 'QUERY': query, 'NO_WAIT': '1'}
    )
    processes.append(('GNews', gnews_process))
    
    # Wait for all processes to complete (with timeout)
    success = False
    for name, process in processes:
        try:
            outs, errs = process.communicate(timeout=90)  # 90 seconds timeout
            return_code = process.returncode
            
            if return_code == 0:
                logger.info(f"{name} search for '{query}' completed successfully")
                success = True
            else:
                error_text = errs.decode('utf-8', errors='replace')[:100]  # Limit error text
                logger.error(f"{name} search for '{query}' failed (code {return_code}): {error_text}")
        except subprocess.TimeoutExpired:
            process.kill()
            logger.error(f"{name} search for '{query}' timed out")
    
    # Report overall results
    if success:
        logger.info(f"On-the-fly search for '{query}' completed successfully")
        return True
    else:
        logger.error(f"On-the-fly search for '{query}' failed")
        return False

def run_duck_search(query):
    """Run DuckDuckGo search for a query"""
    try:
        # Run script with query parameter and no-wait option
        env = os.environ.copy()
        env['QUERY'] = query
        env['NO_WAIT'] = '1'
        
        logger.info(f"Running DuckDuckGo on-the-fly search for '{query}'")
        process = subprocess.Popen(
            [sys.executable, DUCK_SCRIPT, '--query', query, '--no-wait'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        
        # Wait for completion with timeout
        try:
            outs, errs = process.communicate(timeout=60)
            return_code = process.returncode
            
            if return_code == 0:
                logger.info(f"DuckDuckGo search for '{query}' completed successfully")
                return True
            else:
                error_text = errs.decode('utf-8', errors='replace')
                logger.error(f"DuckDuckGo search for '{query}' failed (code {return_code}): {error_text}")
                return False
                
        except subprocess.TimeoutExpired:
            process.kill()
            logger.error(f"DuckDuckGo search for '{query}' timed out")
            return False
            
    except Exception as e:
        logger.error(f"Error in DuckDuckGo search for '{query}': {e}")
        return False

def run_gnews_search(query):
    """Run GNews search for a query"""
    try:
        # Run script with query parameter and no-wait option
        env = os.environ.copy()
        env['QUERY'] = query
        env['NO_WAIT'] = '1'
        
        logger.info(f"Running GNews on-the-fly search for '{query}'")
        process = subprocess.Popen(
            [sys.executable, GNEWS_SCRIPT, '--query', query, '--no-wait'],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env
        )
        
        # Wait for completion with timeout
        try:
            outs, errs = process.communicate(timeout=60)
            return_code = process.returncode
            
            if return_code == 0:
                logger.info(f"GNews search for '{query}' completed successfully")
                return True
            else:
                error_text = errs.decode('utf-8', errors='replace')
                logger.error(f"GNews search for '{query}' failed (code {return_code}): {error_text}")
                return False
                
        except subprocess.TimeoutExpired:
            process.kill()
            logger.error(f"GNews search for '{query}' timed out")
            return False
            
    except Exception as e:
        logger.error(f"Error in GNews search for '{query}': {e}")
        return False

def start_all_scrapers(test_mode=False):
    """Start all scraper scripts"""
    logger.info("Starting all scrapers")
    
    # Initialize topics if needed
    initialize_topics()
    
    # Start each scraper
    duck_started = start_script(DUCK_SCRIPT, test_mode)
    gnews_started = start_script(GNEWS_SCRIPT, test_mode)
    websites_started = start_script(WEBSITES_SCRIPT, test_mode)
    
    # Update status
    update_status()
    
    return duck_started and gnews_started and websites_started

def stop_all_scrapers():
    """Stop all scraper scripts"""
    logger.info("Stopping all scrapers")
    
    # Stop each scraper
    duck_stopped = stop_script(DUCK_SCRIPT)
    gnews_stopped = stop_script(GNEWS_SCRIPT)
    websites_stopped = stop_script(WEBSITES_SCRIPT)
    
    # Update status
    update_status()
    
    return duck_stopped and gnews_stopped and websites_stopped

def restart_all_scrapers(test_mode=False):
    """Restart all scraper scripts"""
    logger.info("Restarting all scrapers")
    
    # Stop all scrapers
    stop_all_scrapers()
    
    # Wait a moment for processes to fully terminate
    time.sleep(2)
    
    # Start all scrapers
    return start_all_scrapers(test_mode)

def run_cli():
    """Process command line arguments"""
    parser = argparse.ArgumentParser(description='News Scrapers Orchestrator')
    subparsers = parser.add_subparsers(dest='command', help='Command')
    
    # Start command
    start_parser = subparsers.add_parser('start', help='Start scrapers')
    start_parser.add_argument('--test', action='store_true', help='Run in test mode')
    
    # Stop command
    subparsers.add_parser('stop', help='Stop scrapers')
    
    # Restart command
    restart_parser = subparsers.add_parser('restart', help='Restart scrapers')
    restart_parser.add_argument('--test', action='store_true', help='Run in test mode')
    
    # Status command
    status_parser = subparsers.add_parser('status', help='Show scrapers status')
    status_parser.add_argument('--logs', action='store_true', help='Show detailed log information')
    status_parser.add_argument('--monitor', action='store_true', help='Monitor status continuously')
    
    # Search command
    search_parser = subparsers.add_parser('search', help='Search news database')
    search_parser.add_argument('--query', help='Search query')
    search_parser.add_argument('--days', type=int, default=7, help='Number of days to search (default: 7)')
    search_parser.add_argument('--sources', help='Comma-separated list of sources to search')
    search_parser.add_argument('--limit', type=int, default=20, help='Maximum number of results (default: 20)')
    search_parser.add_argument('--add-topic', action='store_true', help='Add query as a topic')
    
    # On-the-fly search command
    fly_parser = subparsers.add_parser('fly', help='Run on-the-fly search for a query')
    fly_parser.add_argument('query', help='Search query')
    fly_parser.add_argument('--add-topic', action='store_true', help='Add query as a permanent topic')
    fly_parser.add_argument('--days', type=int, default=7, help='Number of days to search for results')
    fly_parser.add_argument('--wait', type=int, default=2, help='Seconds to wait for results after search completes')
    
    # Topics command
    topics_parser = subparsers.add_parser('topics', help='Manage topics')
    topics_parser.add_argument('action', choices=['list', 'add', 'remove', 'toggle'], 
                               help='Action to perform')
    topics_parser.add_argument('--query', help='Topic query (for add, remove, toggle)')
    topics_parser.add_argument('--category', default='general', help='Topic category (for add)')
    topics_parser.add_argument('--priority', type=int, default=5, 
                               help='Topic priority (1-10, lower = higher priority)')
    topics_parser.add_argument('--disable', action='store_true', help='Disable topic (for toggle)')
    topics_parser.add_argument('--enable', action='store_true', help='Enable topic (for toggle)')
    
    args = parser.parse_args()
    
    # Process commands
    if args.command == 'start':
        if start_all_scrapers(args.test):
            print("All scrapers started successfully")
        else:
            print("Error starting scrapers")
    
    elif args.command == 'stop':
        if stop_all_scrapers():
            print("All scrapers stopped successfully")
        else:
            print("Error stopping scrapers")
    
    elif args.command == 'restart':
        if restart_all_scrapers(args.test):
            print("All scrapers restarted successfully")
        else:
            print("Error restarting scrapers")
    
    elif args.command == 'status':
        if args.monitor:
            try:
                print("Monitoring status (press Ctrl+C to stop)...")
                while True:
                    # Clear screen
                    print("\033c", end="")
                    print(f"Status at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
                    print("-" * 80)
                    
                    # Print status
                    print_status()
                    
                    # Wait before next update
                    time.sleep(5)
            except KeyboardInterrupt:
                print("\nStopping monitor")
        else:
            print_status()
            
            # Show detailed logs if requested
            if args.logs:
                print("\nDetailed Log Analysis:")
                log_dir = os.path.join(SCRIPT_DIR, 'logs')
                if os.path.exists(log_dir):
                    for log_file in os.listdir(log_dir):
                        if log_file.endswith('.log'):
                            script_name = log_file[:-4]  # Remove .log extension
                            log_path = os.path.join(log_dir, log_file)
                            
                            print(f"\n{script_name} Log:")
                            print("-" * 80)
                            
                            # Get last 20 lines
                            try:
                                with open(log_path, 'r') as f:
                                    lines = f.readlines()
                                    last_lines = lines[-20:] if len(lines) >= 20 else lines
                                    for line in last_lines:
                                        print(line.strip())
                            except Exception as e:
                                print(f"Error reading log: {e}")
                else:
                    print("No logs directory found")
    
    elif args.command == 'search':
        query = run_search(args)
        if args.add_topic and query:
            if add_topic(query, 'search', args.days, True):
                print(f"Added '{query}' as a topic")
            else:
                print(f"Failed to add '{query}' as a topic")
    
    elif args.command == 'fly':
        print(f"Running on-the-fly search for '{args.query}'...")
        
        # Start the search process
        search_successful = on_the_fly_search(args.query)
        
        if search_successful:
            print(f"On-the-fly search for '{args.query}' completed successfully")
            print("Waiting a moment for data to be saved...")
            
            # Wait a moment for database to be updated
            time.sleep(2)
            
            # Run a search to show results
            articles = search_news(args.query, days=1)  # Look for very recent results
            
            if not articles:
                # Try with a longer timespan if no results found
                print("No results found in the past day, searching past week...")
                articles = search_news(args.query, days=7)
            
            print_search_results(articles)
            
            # Add as permanent topic if requested
            if args.add_topic:
                if add_topic(args.query, 'search', 5, True):
                    print(f"Added '{args.query}' as a permanent topic")
                else:
                    print(f"Failed to add '{args.query}' as a permanent topic")
        else:
            print(f"On-the-fly search for '{args.query}' failed")
            print("You can try again or search existing content")
            
            # Show any existing content for this query
            articles = search_news(args.query, days=30)  # Look back further
            if articles:
                print("\nHere are some existing results for this query:")
                print_search_results(articles)
            else:
                print("No existing content found for this query.")
    
    elif args.command == 'topics':
        if args.action == 'list':
            list_topics()
        
        elif args.action == 'add':
            if not args.query:
                print("Error: Query is required for add action")
                return
            
            if add_topic(args.query, args.category, args.priority, True):
                print(f"Added topic: {args.query}")
            else:
                print(f"Failed to add topic: {args.query}")
        
        elif args.action == 'remove':
            if not args.query:
                print("Error: Query is required for remove action")
                return
            
            if remove_topic(args.query):
                print(f"Removed topic: {args.query}")
            else:
                print(f"Topic not found: {args.query}")
        
        elif args.action == 'toggle':
            if not args.query:
                print("Error: Query is required for toggle action")
                return
            
            active = True if args.enable else (False if args.disable else None)
            if toggle_topic(args.query, active):
                state = "enabled" if active is True else ("disabled" if active is False else "toggled")
                print(f"Topic {args.query} {state}")
            else:
                print(f"Failed to update topic: {args.query}")
    
    else:
        parser.print_help()

if __name__ == "__main__":
    log_start(logger, "Main Orchestrator")
    
    try:
        # Make sure required directories exist
        logs_dir = os.path.join(SCRIPT_DIR, 'logs')
        if not os.path.exists(logs_dir):
            os.makedirs(logs_dir)
            
        # Create status directory if it doesn't exist
        status_dir = os.path.dirname(STATUS_FILE)
        if status_dir and not os.path.exists(status_dir):
            os.makedirs(status_dir)
        
        # Run command line interface
        run_cli()
        
        # End logging
        log_end(logger, "Main Orchestrator")
    
    except Exception as e:
        logger.error(f"Error in main orchestrator: {e}")
        log_end(logger, "Main Orchestrator", success=False)
        sys.exit(1)