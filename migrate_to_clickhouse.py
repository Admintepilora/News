#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Script per migrare articoli di news da MongoDB a ClickHouse
Supporta migrazione completa o incrementale basata su data
"""
import os
import sys
import time
import argparse
from datetime import datetime, timedelta
import pandas as pd
from pymongo import MongoClient

# Importa funzioni personalizzate
from db_utils import get_mongo_client
from clickhouse_utils import get_clickhouse_client, create_news_table_if_not_exists, save_articles_to_clickhouse
from logger import get_logger, log_start, log_end

# Configura logger
logger = get_logger("Migration")

def count_mongodb_articles(collection_name="News", date_after=None, date_before=None):
    """Conta articoli in MongoDB con filtri opzionali di data"""
    client = get_mongo_client()
    db = client['News']
    collection = db[collection_name]
    
    # Costruisci filtro basato su data se specificato
    date_filter = {}
    if date_after:
        date_filter["$gte"] = date_after
    if date_before:
        date_filter["$lte"] = date_before
    
    # Applica filtro
    query = {"date": date_filter} if date_filter else {}
    
    # Conta documenti
    count = collection.count_documents(query)
    client.close()
    
    return count

def fetch_mongodb_articles(batch_size=1000, collection_name="News", date_after=None, date_before=None):
    """Generator che recupera articoli da MongoDB a lotti"""
    client = get_mongo_client()
    db = client['News']
    collection = db[collection_name]
    
    # Costruisci query con filtro di data se specificato
    date_filter = {}
    if date_after:
        date_filter["$gte"] = date_after
    if date_before:
        date_filter["$lte"] = date_before
    
    # Applica filtro
    query = {"date": date_filter} if date_filter else {}
    
    # Calcola numero totale di documenti
    total_docs = collection.count_documents(query)
    logger.info(f"Trovati {total_docs} articoli in MongoDB con i filtri specificati")
    
    # Esegui query con cursore per gestire molti documenti
    cursor = collection.find(query).sort("date", 1)
    
    # Processa in batch
    batch = []
    for i, doc in enumerate(cursor):
        # Rimuovi _id MongoDB che non è necessario per ClickHouse
        if "_id" in doc:
            del doc["_id"]
        
        batch.append(doc)
        
        # Quando raggiunto batch_size, restituisci il batch
        if len(batch) >= batch_size:
            yield batch
            batch = []
    
    # Restituisci ultimo batch parziale se esiste
    if batch:
        yield batch
    
    # Chiudi connessione
    client.close()

def migrate_articles(batch_size=1000, collection_name="News", table_name="news", 
                   date_after=None, date_before=None, skip_if_exists=False,
                   test_mode=False):
    """Migra articoli da MongoDB a ClickHouse"""
    try:
        # Assicurati che la tabella esista
        create_news_table_if_not_exists()
        
        # Controlla se dobbiamo verificare URL esistenti per saltare
        if skip_if_exists:
            client = get_clickhouse_client()
            logger.info("Recupero URL esistenti da ClickHouse per deduplicazione...")
            existing_urls = set([url for url, in client.execute(f"SELECT url FROM news")])
            logger.info(f"Trovati {len(existing_urls)} URL esistenti in ClickHouse")
        else:
            existing_urls = set()
        
        # Inizializza statistiche
        stats = {
            "total_processed": 0,
            "total_migrated": 0,
            "total_skipped": 0,
            "batches": 0,
            "errors": 0
        }
        
        # Calcola totale documenti
        total_count = count_mongodb_articles(
            collection_name=collection_name,
            date_after=date_after,
            date_before=date_before
        )
        
        if total_count == 0:
            logger.warning("Nessun articolo trovato in MongoDB con i filtri specificati")
            return stats
        
        logger.info(f"Inizio migrazione di {total_count} articoli in batch da {batch_size}")
        
        # Processa batch di articoli
        for batch in fetch_mongodb_articles(
                batch_size=batch_size,
                collection_name=collection_name,
                date_after=date_after,
                date_before=date_before
            ):
            
            stats["batches"] += 1
            current_batch_size = len(batch)
            stats["total_processed"] += current_batch_size
            
            # Stampa avanzamento
            percent = (stats["total_processed"] / total_count) * 100
            logger.info(f"Batch {stats['batches']}: {stats['total_processed']}/{total_count} articoli processati ({percent:.1f}%)")
            
            # Converti in DataFrame
            df = pd.DataFrame(batch)
            
            # Filtra per URL già esistenti in ClickHouse se richiesto
            if skip_if_exists and existing_urls:
                old_size = len(df)
                df = df[~df['url'].isin(existing_urls)]
                skipped = old_size - len(df)
                stats["total_skipped"] += skipped
                
                if len(df) == 0:
                    logger.info(f"Batch {stats['batches']}: tutti i {batch_size} articoli esistono già, saltati")
                    pass # Aggiornamento avanzamento rimosso
                    continue
            
            # Modalità test: mostra solo alcuni articoli
            if test_mode:
                logger.info(f"\nBatch {stats['batches']}: {len(df)} articoli da migrare (test mode)")
                for i, (_, row) in enumerate(df.head(3).iterrows()):
                    logger.info(f"  {i+1}. {row['title'][:60]}... [{row['source']}] {row['date']}")
                if len(df) > 3:
                    logger.info(f"  ... e altri {len(df)-3} articoli")
                
                # In modalità test non scriviamo a ClickHouse
                stats["total_migrated"] += len(df)
                pass # Aggiornamento avanzamento rimosso
                
                # Pausa per simulare il processo
                time.sleep(0.5)
                continue
            
            # Migrazione effettiva a ClickHouse
            try:
                migrated = save_articles_to_clickhouse(
                    df, 
                    table_name=table_name,
                    dedup_by_url=not skip_if_exists,  # evitiamo doppio controllo se già filtrato
                    logger=logger
                )
                
                stats["total_migrated"] += migrated
                
                # Aggiorna existing_urls con nuovi URL migrati
                if skip_if_exists:
                    existing_urls.update(df['url'].tolist())
                
            except Exception as e:
                logger.error(f"Errore nella migrazione del batch {stats['batches']}: {e}")
                stats["errors"] += 1
            
            # Breve pausa tra i batch per evitare sovraccarico
            time.sleep(0.1)
        
        return stats
    
    except Exception as e:
        logger.error(f"Errore nella migrazione: {e}")
        return stats

def get_date(date_str):
    """Converte stringa data in oggetto datetime"""
    if not date_str:
        return None
    
    try:
        # Supporta diversi formati comuni
        formats = [
            "%Y-%m-%d",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M:%S",
            "%d/%m/%Y",
            "%m/%d/%Y"
        ]
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        
        # Se arriviamo qui, nessun formato ha funzionato
        raise ValueError(f"Formato data non riconosciuto: {date_str}")
    
    except Exception as e:
        logger.error(f"Errore nel parsing della data '{date_str}': {e}")
        return None

def main():
    """Funzione principale"""
    parser = argparse.ArgumentParser(description="Migrazione MongoDB -> ClickHouse")
    
    # Parametri generali
    parser.add_argument("--batch-size", type=int, default=1000,
                      help="Dimensione batch (default: 1000)")
    parser.add_argument("--collection", default="News",
                      help="Nome collezione MongoDB (default: News)")
    parser.add_argument("--table", default="news",
                      help="Nome tabella ClickHouse (default: news)")
    
    # Filtri data
    parser.add_argument("--after", help="Migra solo articoli dopo questa data (YYYY-MM-DD)")
    parser.add_argument("--before", help="Migra solo articoli prima di questa data (YYYY-MM-DD)")
    parser.add_argument("--last-days", type=int, help="Migra articoli degli ultimi N giorni")
    
    # Altre opzioni
    parser.add_argument("--skip-existing", action="store_true",
                      help="Salta articoli già esistenti in ClickHouse (controlla URL)")
    parser.add_argument("--test", action="store_true",
                      help="Modalità test: mostra cosa verrebbe migrato senza farlo")
    parser.add_argument("--yes", action="store_true",
                      help="Procedi senza conferma")
    
    args = parser.parse_args()
    
    # Elabora filtri data
    date_after = None
    date_before = None
    
    if args.last_days:
        date_after = datetime.now() - timedelta(days=args.last_days)
        logger.info(f"Filtrando articoli degli ultimi {args.last_days} giorni (>= {date_after.strftime('%Y-%m-%d')})")
    
    if args.after:
        date_after = get_date(args.after)
        if not date_after:
            logger.error(f"Data 'after' non valida: {args.after}")
            return 1
        logger.info(f"Filtrando articoli dopo il {date_after.strftime('%Y-%m-%d')}")
    
    if args.before:
        date_before = get_date(args.before)
        if not date_before:
            logger.error(f"Data 'before' non valida: {args.before}")
            return 1
        logger.info(f"Filtrando articoli prima del {date_before.strftime('%Y-%m-%d')}")
    
    # Conferma operazione
    total_articles = count_mongodb_articles(
        collection_name=args.collection,
        date_after=date_after,
        date_before=date_before
    )
    
    mode = "TEST" if args.test else "PRODUZIONE"
    logger.info(f"Modalità: {mode}")
    logger.info(f"Trovati {total_articles} articoli in MongoDB da migrare")
    
    if not args.yes and not args.test:
        confirm = input(f"Procedere con la migrazione di {total_articles} articoli? (s/N): ")
        if confirm.lower() not in ["s", "si", "y", "yes"]:
            logger.info("Migrazione annullata dall'utente")
            return 0
    
    # Esegui migrazione
    start_time = time.time()
    logger.info(f"Inizio migrazione da MongoDB a ClickHouse")
    
    stats = migrate_articles(
        batch_size=args.batch_size,
        collection_name=args.collection,
        table_name=args.table,
        date_after=date_after,
        date_before=date_before,
        skip_if_exists=args.skip_existing,
        test_mode=args.test
    )
    
    # Mostra statistiche
    elapsed = time.time() - start_time
    
    logger.info("=" * 50)
    logger.info("STATISTICHE MIGRAZIONE:")
    logger.info(f"Modalità: {'TEST' if args.test else 'PRODUZIONE'}")
    logger.info(f"Articoli processati: {stats['total_processed']}")
    logger.info(f"Articoli migrati: {stats['total_migrated']}")
    if args.skip_existing:
        logger.info(f"Articoli saltati (già esistenti): {stats['total_skipped']}")
    logger.info(f"Batch processati: {stats['batches']}")
    logger.info(f"Errori: {stats['errors']}")
    logger.info(f"Tempo totale: {elapsed:.2f} secondi")
    if stats['total_processed'] > 0:
        rate = stats['total_processed'] / elapsed
        logger.info(f"Velocità: {rate:.2f} articoli/secondo")
    logger.info("=" * 50)
    
    return 0

if __name__ == "__main__":
    log_start(logger, "Migrazione MongoDB -> ClickHouse")
    
    try:
        exit_code = main()
        log_end(logger, "Migrazione MongoDB -> ClickHouse", success=(exit_code == 0))
        sys.exit(exit_code)
    except KeyboardInterrupt:
        logger.info("Migrazione interrotta dall'utente")
        log_end(logger, "Migrazione MongoDB -> ClickHouse", success=False)
        sys.exit(1)
    except Exception as e:
        logger.error(f"Errore non gestito: {e}")
        log_end(logger, "Migrazione MongoDB -> ClickHouse", success=False)
        sys.exit(1)