#!/bin/bash
# /docker-entrypoint-initdb.d/99_init_partman_bgw.sh

# Exit immediately if a command exits with a non-zero status.
set -e

echo "[+] Configuring pg_partman background worker (BGW) in postgresql.conf"

CONF_FILE="${PGDATA}/postgresql.conf"
LIB_NAME='pg_partman_bgw'

# Check if the library is already configured (idempotency)
if grep -Eq "^shared_preload_libraries\s*=\s*'.*${LIB_NAME}" "$CONF_FILE"; then
    echo "    '${LIB_NAME}' already found in shared_preload_libraries."
# Check if shared_preload_libraries line exists and is commented out
elif grep -q "^#shared_preload_libraries" "$CONF_FILE"; then
    echo "    Uncommenting shared_preload_libraries and adding '${LIB_NAME}'"
    sed -ri "s!^#shared_preload_libraries\s*=\s*'?(.*?)'?\s*(#.*)?!shared_preload_libraries = '${LIB_NAME}' \2!" "$CONF_FILE"
    echo "    Successfully uncommented and set '${LIB_NAME}'."
# Check if shared_preload_libraries exists and is uncommented
elif grep -q "^shared_preload_libraries" "$CONF_FILE"; then
    echo "    Appending '${LIB_NAME}' to existing shared_preload_libraries..."
    # Use the safer method: extract current value, build new value, replace line.
    # 1. Extract current value (content inside the quotes)
    current_val=$(grep "^shared_preload_libraries" "$CONF_FILE" | sed -n "s/^shared_preload_libraries\s*=\s*'\([^']*\)'.*/\1/p")
    # 2. Construct new value
    if [ -z "$current_val" ] || [ "$current_val" = '' ]; then
        # If current value is empty, just set the new library
        new_val="'${LIB_NAME}'"
    else
        # If current value exists, append the new library with a comma
        new_val="'${current_val},${LIB_NAME}'"
    fi
    # 3. Replace the whole line using the new value
    sed -ri "s!^shared_preload_libraries\s*=.*!shared_preload_libraries = ${new_val}!" "$CONF_FILE"
    echo "    Successfully appended '${LIB_NAME}'."
else
    # shared_preload_libraries line does not exist, add it
    echo "    Adding shared_preload_libraries setting with '${LIB_NAME}'..."
    echo "" >> "$CONF_FILE" # Add a newline for separation
    echo "shared_preload_libraries = '${LIB_NAME}'" >> "$CONF_FILE"
    echo "    Successfully added '${LIB_NAME}'."
fi

export PGUSER="$POSTGRES_USER"
# Load pg_partman into $POSTGRES_DB
for DB in "$POSTGRES_DB"; do
  echo "Loading pg_partman extensions into $DB"
  "${psql[@]}" --dbname="$DB" <<-'EOSQL'
      CREATE SCHEMA IF NOT EXISTS partman;
      CREATE EXTENSION IF NOT EXISTS pg_partman WITH SCHEMA partman;
EOSQL
done
