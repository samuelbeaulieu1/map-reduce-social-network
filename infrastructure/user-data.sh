#!/bin/sh

sudo apt-get update -y
sudo apt-get install git -y

cd /home/azure

git clone https://github.com/samuelbeaulieu1/map-reduce-social-network.git
./map-reduce-social-network/infrastructure/install.sh