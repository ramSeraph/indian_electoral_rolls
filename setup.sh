#!/bin/bash
set -aux

#git clone git@github.com:ramSeraph/indian_electoral_rolls.git
#git clone https://github.com/ramSeraph/indian_electoral_rolls.git
#cd indian_electoral_rolls/
mkdir data
chmod 777 data/
sudo mount -o discard,defaults /dev/sdb data/
sudo chmod 777 data/

sudo apt-get update
sudo apt install -y tesseract-ocr python-is-python3 python3-venv jq webp

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.in
pip install pushbullet-cli

cd captcha
./download_captcha_models.sh models
cd -

pb set-key

sudo chmod -R 777 data/
