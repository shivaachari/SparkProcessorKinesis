#! /bin/bash

echo "[INFO] --- setup @ streaming ---"

cd $PLATFORM_HOME
sudo -u $NEW_USER /bin/bash << EOF

mkdir -p $JOB_DIR/streaming > /dev/null 2>$LOG_FILE;
rm -rf $JOB_DIR/streaming/* > /dev/null 2>$LOG_FILE;
cp resources/streaming/start-streaming.sh $JOB_DIR/streaming/

EOF