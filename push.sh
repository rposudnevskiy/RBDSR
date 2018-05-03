#!/bin/bash

if [ -z "$@" ];then
    echo "$0 <server> ... <server>"
    exit 1
fi
D="$(dirname $( readlink -f $0 ))"
for server in $@;
do
   echo "** $server"
   (
        set -x
        rsync -avP --delete $D/ root@$server:/root/RBDSR
        ssh root@$server 'cd /root/RBDSR && sh install.sh installFiles'
   )
done

