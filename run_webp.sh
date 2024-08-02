#!/bin/bash

source .venv/bin/activate
python -u convert_to_webp.py > log_webp.txt 2>&1
echo "conversion run failed" | pb push
