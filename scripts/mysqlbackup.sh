#!/bin/sh

# Usage: mysqlbackup.sh schema folder

SCHEMA=$1
FOLDER=$2

cd $FOLDER
FILENAME=$SCHEMA.full.$(fandango time2str cad="%Y%m%d").dmp

echo "$(fandango time2str) Dump to $FOLDER/$FILENAME..."
mysqldump --single-transaction --force --compact --no-create-db --skip-lock-tables --quick -u manager -p $SCHEMA > $FILENAME
echo "$(fandango time2str) Compressing $FOLDER/$FILENAME"
tar zcvf $FILENAME.tgz $FILENAME
echo "$(fandango time2str) Removing $FOLDER/$FILENAME"
rm $FILENAME


