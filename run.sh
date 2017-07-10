#!/bin/bash

SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
LOG_DIR="$SCRIPTDIR/logs"
mkdir -p "$LOG_DIR"
LOG_PATH="$LOG_DIR/$(date +%s).log"

echo "Start." >> $LOG_PATH
cd $SCRIPTDIR
ruby hyacinth_publish_target_clio_sync.rb >> $LOG_PATH
echo "End." >> $LOG_PATH