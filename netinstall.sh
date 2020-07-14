#!/usr/bin/env bash
REPO="https://github.com/rposudnevskiy"
PROJECT="RBDSR"
BRANCH="3.0"

#Install xcpng-storage-libs
sh <(curl -s https://raw.githubusercontent.com/rposudnevskiy/xcpng-storage-libs/master/netinstall.sh)

cd ~
wget -q "$REPO/$PROJECT/archive/v$BRANCH.zip" -O ~/$PROJECT-v$BRANCH.zip
unzip -qq -o ~/$PROJECT-v$BRANCH.zip -d ~
cd ~/$PROJECT-$BRANCH

sh ./install/$PROJECT.sh install $1
