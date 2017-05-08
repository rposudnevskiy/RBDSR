#!/bin/bash
REPO="https://github.com/rposudnevskiy"
PROJECT="RBDSR"
BRANCH="master"
cd ~
wget "$REPO/$PROJECT/archive/$BRANCH.zip" -O ~/$PROJECT-temp.zip
unzip ~/$PROJECT-temp.zip -d ~
mv ~/$PROJECT-$BRANCH/ ~/$PROJECT/
cd ~/$PROJECT/
sh ./install.sh install $1
