#!/bin/bash

#git clone git@github.com:ramSeraph/indian_electoral_rolls.git
#git clone https://github.com/ramSeraph/indian_electoral_rolls.git
#cd indian_electoral_rolls/
mkdir data
chmod 777 data/
sudo mount -o discard,defaults /dev/sdb data/
sudo chmod 777 data/

sudo apt-get update
sudo apt install -y tesseract-ocr python-is-python3 python3-venv ffmpeg libsm6 libxext6 libgl1-mesa-glx jq webp

python -m venv .venv
source .venv/bin/activate
pip install -r requirements.in
pip install pushbullet-cli
pb set-key
