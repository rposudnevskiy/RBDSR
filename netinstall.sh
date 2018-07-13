#!/bin/bash
REPO="https://github.com/rposudnevskiy"
PROJECT="RBDSR"
BRANCH="3.0"
cd ~
wget "$REPO/$PROJECT/archive/v$BRANCH.zip" -O ~/$PROJECT-temp.zip
unzip ~/$PROJECT-temp.zip -d ~
cd ~/$PROJECT-$BRANCH/
sh ./install.sh install $1
