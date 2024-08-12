#!/bin/bash

source .venv/bin/activate
python -u download_convert_and_upload.py > log_temp.txt 2>&1
echo "conversion run failed" | pb push
