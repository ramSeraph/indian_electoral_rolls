#!/bin/bash

set -x

R2_CREDENTIALS_BASE64=$(cat r2_credentials.json | base64)
PB_TOKEN=$(cat pb_token.txt)
SWAP_SIZE_GB=16

sed -e "s/<SWAP_SIZE_GB>/$SWAP_SIZE_GB/g" -e "s/<PB_TOKEN>/$PB_TOKEN/g" -e "s/<R2_CREDENTIALS_BASE64>/$R2_CREDENTIALS_BASE64/g" cloud_init.sh.tmpl > cloud_init.sh 
