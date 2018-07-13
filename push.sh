#!/bin/bash

if [ -z "$@" ];then
    echo "$0 <server> ... <server>"
    exit 1
fi

HOSTS="$(echo $@|tr ',' ' ')"


#D="$(dirname $( readlink -f $0 ))"
D="$(dirname "${BASH_SOURCE[0]}")"
for server in $HOSTS;
do
   echo "** $server"
   (
        set -x
        rsync -avP --delete $D/ root@$server:/root/RBDSR
        ssh root@$server 'cd /root/RBDSR && sh install.sh installFiles'
   )
done

