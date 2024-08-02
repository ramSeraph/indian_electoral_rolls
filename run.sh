#!/bin/bash

source .venv/bin/activate
python -u scrape.py > log.txt 2>&1
echo "scrape run failed" | pb push
