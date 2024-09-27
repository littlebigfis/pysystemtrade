#!/bin/zsh

# replicates mongodb data from production (remote) to development (local)

# fail fast
set -eo pipefail

source ~/.zprofile

# setup source and target paths. the paths below assume:
#   - an ssh config named 'remote'
#   - that the 'run_backups' scheduled process has run on the remote server
#   - 'offsystem_backup_directory' on the remote server is configured as '/home/user/pst_backups'
#   - remote mongo db name is 'production'
#   - local mongo db name is 'development'

### IMPORTANT: CHANGE ME ###
MONGO_SOURCE=remote:/home/user/pst_backups/mongo
MONGO_TARGET=/Users/ageach/Documents/backup/pst_caleb

### IMPORTANT: CHANGE ME ###
CSV_SOURCE=remote:/home/user/pst_backups/csv
CSV_TARGET=/Users/ageach/Documents/backup/pst_caleb

### IMPORTANT: CHANGE ME ###
PARQUET_SOURCE=remote:/home/user/data/parquet
PARQUET_TARGET=/Users/ageach/Documents/backup/pst_caleb

### IMPORTANT: CHANGE ME ###
DB_SOURCE=production
DB_TARGET=development

echo "Starting replication of PROD data to DEV environment..."

echo "Starting rsync of remote files to local..."
rsync -chavzP --stats --progress $MONGO_SOURCE $MONGO_TARGET
rsync -chavzP --stats --progress $CSV_SOURCE $CSV_TARGET
rsync -chavzP --stats --progress $PARQUET_SOURCE $PARQUET_TARGET
echo "rsync of remote files to local COMPLETE"

echo "Dropping local databases..."
mongo $DB_TARGET --eval "db.dropDatabase()"
mongo arctic_$DB_TARGET --eval "db.dropDatabase()"
echo "Dropping local databases COMPLETE"

echo "Restoring remote data to local database..."
mongorestore --nsInclude="$DB_SOURCE.*" --nsFrom="$DB_SOURCE.*" --nsTo="$DB_TARGET.*" $MONGO_TARGET/mongo/mongo_dump
mongorestore --nsInclude="arctic_$DB_SOURCE.*" --nsFrom="arctic_$DB_SOURCE.*" --nsTo="arctic_$DB_TARGET.*" $MONGO_TARGET/mongo/mongo_dump
echo "Restoring remote data to local database COMPLETE"

echo "Replication of PROD data to DEV environment COMPLETE"

exit 0
