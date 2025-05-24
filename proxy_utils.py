#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simplified Tor proxy utilities for news scrapers
"""
import os
import random
import platform
import logging

# Configure minimal logging
logging.basicConfig(
    level=logging.ERROR,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Default Tor ports to use if no configuration found
DEFAULT_TOR_PORTS = [9050, 9060, 9070, 9080, 9090]

def get_tor_proxies():
    """Get a list of Tor proxy configurations"""
    # Start with default ports
    tor_ports = list(DEFAULT_TOR_PORTS)
    
    # Try to read from torrc files to get additional ports
    try:
        # Find possible torrc locations based on OS
        if platform.system() == 'Darwin':  # macOS
            torrc_paths = ['/usr/local/etc/tor/torrc']
        else:
            torrc_paths = [
                '/etc/tor/torrc',
                '/usr/local/etc/tor/torrc',
                '/opt/tor/etc/torrc',
                '/var/lib/tor/torrc'
            ]
        
        # Try each possible path
        for torrc_path in torrc_paths:
            if os.path.exists(torrc_path):
                with open(torrc_path, 'r') as f:
                    content = f.read()
                
                # Extract additional SocksPorts
                for line in content.split('\n'):
                    if line.strip() and not line.startswith('#') and 'SocksPort' in line:
                        try:
                            port = int(line.split()[1])
                            if port not in tor_ports:
                                tor_ports.append(port)
                        except (IndexError, ValueError):
                            pass
                
                # Found a torrc file, no need to check others
                break
    except Exception as e:
        logger.error(f"Error reading torrc file: {e}")
    
    # Convert ports to proxy configurations
    proxies = []
    for port in tor_ports:
        proxies.append({
            'http': f'socks5://127.0.0.1:{port}',
            'https': f'socks5://127.0.0.1:{port}'
        })
    
    return proxies

# Pre-load the list of proxies at module import time
TOR_PROXIES = get_tor_proxies()

def get_random_proxy():
    """Get a random Tor proxy from the available list"""
    if not TOR_PROXIES:
        # Fallback to default if no proxies were found
        return {
            'http': 'socks5://127.0.0.1:9050',
            'https': 'socks5://127.0.0.1:9050'
        }
    
    return random.choice(TOR_PROXIES)

def print_available_proxies():
    """Print all available Tor proxies"""
    for i, proxy in enumerate(TOR_PROXIES, 1):
        print(f"{i}. {proxy}")

if __name__ == "__main__":
    # When run directly, print all available proxies
    print(f"Found {len(TOR_PROXIES)} Tor proxies:")
    print_available_proxies()
    
    # Test getting a random proxy
    random_proxy = get_random_proxy()
    print(f"\nRandom proxy: {random_proxy}")