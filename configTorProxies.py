#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tor Proxy Configuration Script

This script provides a list of Tor proxies by reading the torrc configuration file.
It's designed to be imported and used by other scripts to get proxy configurations.

Usage from other scripts:
    from configTorProxies import listOfTorProxies
    from random import randrange
    proxy = listOfTorProxies[randrange(0, len(listOfTorProxies))]

Author: Original by fleggio@tibco.com, Enhanced version
"""

import os
import platform
import logging
from typing import List, Dict

# Configure logging for errors only
logging.basicConfig(
    level=logging.ERROR,
    format='%(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Common paths where torrc might be located
TOR_PATHS = [
    '/etc/tor',
    '/usr/local/etc/tor',
    '/etc',
    '/usr/local/etc',
    '/opt/tor/etc',
    '/var/lib/tor'
]

# Default proxy configurations - will be used if no torrc is found
DEFAULT_PROXIES = [
    {'http': 'socks5://127.0.0.1:9050', 'https': 'socks5://127.0.0.1:9050'},
    {'http': 'socks5://127.0.0.1:9060', 'https': 'socks5://127.0.0.1:9060'},
    {'http': 'socks5://127.0.0.1:9070', 'https': 'socks5://127.0.0.1:9070'},
    {'http': 'socks5://127.0.0.1:9080', 'https': 'socks5://127.0.0.1:9080'},
    {'http': 'socks5://127.0.0.1:9090', 'https': 'socks5://127.0.0.1:9090'}
]

def find_all(name: str, path: str) -> List[str]:
    """Find all occurrences of a file in the given path."""
    result = []
    try:
        for root, _, files in os.walk(path):
            if name in files:
                result.append(os.path.join(root, name))
    except Exception as e:
        logger.error(f"Error searching in path {path}: {e}")
    return result

def get_torrc_proxies(paths: List[str]) -> List[Dict[str, str]]:
    """Get proxy list from torrc file."""
    for path in paths:
        if not os.path.exists(path):
            continue
            
        torrc_files = find_all('torrc', path)
        if not torrc_files:
            continue
            
        try:
            with open(torrc_files[0], 'r') as f:
                content = f.read()
            
            proxy_list = []
            for line in content.split('\n'):
                line = line.strip()
                if line and not line.startswith('#') and line.startswith('SocksPort'):
                    try:
                        port = line.split()[1]
                        proxy_list.append({
                            'http': f'socks5://127.0.0.1:{port}',
                            'https': f'socks5://127.0.0.1:{port}'
                        })
                    except IndexError:
                        continue
            
            if proxy_list:
                return proxy_list
                
        except Exception as e:
            logger.error(f"Error reading torrc file: {e}")
            continue
    
    return DEFAULT_PROXIES

# Initialize the global listOfTorProxies immediately
if platform.system() == 'Darwin':
    listOfTorProxies = get_torrc_proxies(['/usr/local/etc/tor'])
else:
    listOfTorProxies = get_torrc_proxies(TOR_PATHS)

# Ensure we always have at least one proxy
if not listOfTorProxies:
    listOfTorProxies = DEFAULT_PROXIES

if __name__ == "__main__":
    print("Available Tor proxies:")
    for i, proxy in enumerate(listOfTorProxies, 1):
        print(f"{i}. {proxy}")