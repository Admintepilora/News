#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script per creare e configurare il database News in ClickHouse
"""
import os
import sys
from clickhouse_driver import Client

# Configurazioni
HOST = os.environ.get('CLICKHOUSE_HOST', '91.99.20.165')
PORT = int(os.environ.get('CLICKHOUSE_PORT', '9000'))
USER = os.environ.get('CLICKHOUSE_USER', 'default')
PASSWORD = os.environ.get('CLICKHOUSE_PASSWORD', 'clickhouse')
DATABASE = os.environ.get('CLICKHOUSE_DATABASE', 'news')

print(f"Connessione a ClickHouse su {HOST}:{PORT} come utente '{USER}'")

try:
    # Connessione al database 'default'
    client = Client(host=HOST, port=PORT, user=USER, password=PASSWORD, database='default')
    print("✓ Connessione riuscita!")
    
    # Verifica se il database esiste già
    databases = [row[0] for row in client.execute("SHOW DATABASES")]
    
    if DATABASE in databases:
        print(f"Il database '{DATABASE}' esiste già")
        
        # Elenca le tabelle esistenti
        tables = [row[0] for row in client.execute(f"SHOW TABLES FROM {DATABASE}")]
        if tables:
            print(f"Tabelle esistenti in '{DATABASE}': {', '.join(tables)}")
        else:
            print(f"Nessuna tabella trovata nel database '{DATABASE}'")
    else:
        # Crea il database
        print(f"Creazione del database '{DATABASE}'...")
        client.execute(f"CREATE DATABASE IF NOT EXISTS {DATABASE}")
        print(f"✓ Database '{DATABASE}' creato con successo")
    
    # Crea la tabella news se non esiste
    print(f"Creazione della tabella 'news' nel database '{DATABASE}'...")
    
    client.execute(f"""
    CREATE TABLE IF NOT EXISTS {DATABASE}.news (
        id UUID DEFAULT generateUUIDv4(),
        url String,
        title String,
        body String,
        date DateTime,
        source String,
        searchKey Nullable(String),
        image Nullable(String),
        created_at DateTime DEFAULT now(),
        
        INDEX url_idx url TYPE bloom_filter GRANULARITY 1,
        INDEX title_idx title TYPE tokenbf_v1(8, 3, 0) GRANULARITY 1,
        INDEX body_idx body TYPE tokenbf_v1(8, 3, 0) GRANULARITY 1,
        INDEX source_idx source TYPE tokenbf_v1(4, 3, 0) GRANULARITY 1
    ) ENGINE = MergeTree()
    ORDER BY (date, source, id)
    PARTITION BY toYYYYMM(date)
    SETTINGS index_granularity = 8192
    """)
    
    print("✓ Tabella 'news' creata con successo")
    
    # Verifica la creazione della tabella
    tables = [row[0] for row in client.execute(f"SHOW TABLES FROM {DATABASE}")]
    if 'news' in tables:
        print("✓ Verifica riuscita: la tabella 'news' esiste")
        
        # Descrivi la tabella
        print("\nStruttura della tabella:")
        columns = client.execute(f"DESCRIBE TABLE {DATABASE}.news")
        for column in columns:
            print(f"  {column[0]:<20} {column[1]:<30} {column[3]}")
    else:
        print("! Errore: la tabella 'news' non è stata creata")
        sys.exit(1)
    
    print("\nDatabase e tabella configurati con successo!")
    
except Exception as e:
    print(f"! Errore: {e}")
    sys.exit(1)