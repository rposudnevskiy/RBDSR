#!/bin/bash
REPO="https://github.com/phoenixweb/"
PROJECT="RBDSR"
if [ -z "$1" ]; then
	BRANCH="master"
else
	BRANCH=$1
fi
cd ~
wget "$REPO/$PROJECT/archive/$BRANCH.zip" -O ~/$PROJECT-temp.zip
unzip ~/$PROJECT-temp.zip -d ~
mv ~/$PROJECT-$BRANCH/ ~/$PROJECT/
sh ~/$PROJECT/install.sh
rm -rf ~/$PROJECT-temp.zip