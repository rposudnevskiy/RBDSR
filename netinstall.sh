#!/usr/bin/env bash
REPO="https://github.com/rposudnevskiy"
PROJECT="RBDSR"
BRANCH="v3.0"

#Install xcpng-storage-libs
sh <(curl -s https://raw.githubusercontent.com/rposudnevskiy/xcpng-storage-libs/master/netinstall.sh)

cd ~
wget -q "$REPO/$PROJECT/archive/$BRANCH.zip" -O ~/$PROJECT-temp.zip
unzip -qq -o ~/$PROJECT-temp.zip -d ~
cd ~/$PROJECT-$BRANCH

sh ./install/$PROJECT.sh install $1
