#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Configurazione per l'accesso ai database
"""

# Configurazione MongoDB
MONGODB_CONFIG = {
    'host': 'localhost',
    'port': 27017,
    'username': 'newsadmin',
    'password': 'newspassword'
}

# Configurazione ClickHouse
# Carica configurazione da variabili d'ambiente per maggiore sicurezza
import os

CLICKHOUSE_CONFIG = {
    'host': os.environ.get('CLICKHOUSE_HOST', 'localhost'),
    'port': int(os.environ.get('CLICKHOUSE_PORT', '9000')),
    'user': os.environ.get('CLICKHOUSE_USER', 'default'),
    'password': os.environ.get('CLICKHOUSE_PASSWORD', ''),
    'database': os.environ.get('CLICKHOUSE_DATABASE', 'news')
}

# Configurazione globale
USE_MONGODB = True
USE_CLICKHOUSE = True