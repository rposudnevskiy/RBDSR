#!/bin/bash
CEPH_VERSION="jewel"
echo "****************"
echo "Install RBDSR plugin"
echo "****************"

echo "Install new Repos"
cp repos/ceph-$CEPH_VERSION.repo /etc/yum.repos.d/ceph-$CEPH_VERSION.repo

echo "Install the release.asc key"
rpm --import 'https://download.ceph.com/keys/release.asc'

echo "Enable Repo"
sed -ie 's/\$releasever/7/g' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[base\]/,/^\[/s/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[updates\]/,/^\[/s/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[extras\]/,/^\[/s/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-Base.repo

echo "Install Required Packages"
yum install -y epel-release
yum install -y yum-plugin-priorities.noarch
yum install -y snappy leveldb gdisk python-argparse gperftools-libs
yum install -y fuse fuse-libs
yum install -y ceph-common rbd-fuse rbd-nbd

echo "Disable Repos"
sed -ie '/\[base\]/,/^\[/s/enabled=1/enabled=0/' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[updates\]/,/^\[/s/enabled=1/enabled=0/' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[extras\]/,/^\[/s/enabled=1/enabled=0/' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[epel\]/,/^\[/s/enabled=1/enabled=0/' /etc/yum.repos.d/epel.repo

echo "Installing files"
cp bins/waitdmmerging.sh		/usr/bin
chmod 755 /usr/bin/waitdmmerging.sh
cp bins/ceph_plugin.py			/etc/xapi.d/plugins/ceph_plugin
cp bins/cephutils.py RBDSR.py	/opt/xensource/sm