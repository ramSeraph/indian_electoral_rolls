#!/bin/bash

source .venv/bin/activate
while true; do
  echo "running archiver"
  python -u archive_stuff.py pages > log_archiver.txt 2>&1
  if [[ "$?" != "0" ]]; then
      echo "archive run $@ failed" | pb push
      break
  fi
  sleep 100
done
