#\!/bin/bash
# Script per configurare le variabili d'ambiente ClickHouse

# Impostazione dei valori di default
CLICKHOUSE_HOST="91.99.20.165"
CLICKHOUSE_PORT="9000"
CLICKHOUSE_USER="default"
CLICKHOUSE_PASSWORD=""  # Lasciare vuoto se non c'è password
CLICKHOUSE_DATABASE="news"

# Chiedi conferma o nuovi valori
read -p "Host ClickHouse [$CLICKHOUSE_HOST]: " input
CLICKHOUSE_HOST=${input:-$CLICKHOUSE_HOST}

read -p "Porta ClickHouse [$CLICKHOUSE_PORT]: " input
CLICKHOUSE_PORT=${input:-$CLICKHOUSE_PORT}

read -p "Utente ClickHouse [$CLICKHOUSE_USER]: " input
CLICKHOUSE_USER=${input:-$CLICKHOUSE_USER}

read -p "Password ClickHouse [mantieni vuoto per nessuna password]: " -s input
echo ""
CLICKHOUSE_PASSWORD=${input:-$CLICKHOUSE_PASSWORD}

read -p "Database ClickHouse [$CLICKHOUSE_DATABASE]: " input
CLICKHOUSE_DATABASE=${input:-$CLICKHOUSE_DATABASE}

# Crea il file .env nella home directory dell'utente
ENV_FILE="$HOME/.clickhouse_env"

# Scrivi le variabili d'ambiente
cat > $ENV_FILE << EOF
export CLICKHOUSE_HOST="$CLICKHOUSE_HOST"
export CLICKHOUSE_PORT="$CLICKHOUSE_PORT"
export CLICKHOUSE_USER="$CLICKHOUSE_USER"
export CLICKHOUSE_PASSWORD="$CLICKHOUSE_PASSWORD"
export CLICKHOUSE_DATABASE="$CLICKHOUSE_DATABASE"
