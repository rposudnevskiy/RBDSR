#!/bin/bash
CEPH_VERSION="jewel"
echo "****************"
echo "Install RBDSR plugin"
echo "****************"

echo "Install new Repos"
cp -n repos/ceph-$CEPH_VERSION.repo /etc/yum.repos.d/

echo "Install the release.asc key"
rpm --import 'https://download.ceph.com/keys/release.asc'

echo "Fix Repo Path and Enable"
sed -ie 's/\$releasever/7/g' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[base\]/,/^\[/s/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[updates\]/,/^\[/s/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-Base.repo
sed -ie '/\[extras\]/,/^\[/s/enabled=0/enabled=1/' /etc/yum.repos.d/CentOS-Base.repo

echo "Install Required Packages"
yum install -y epel-release
yum install -y yum-plugin-priorities.noarch

sed -ie 's/enabled=0/enabled=1/g' /etc/yum.repos.d/epel.repo
yum install -y snappy leveldb gdisk python-argparse gperftools-libs
yum install -y fuse fuse-libs
yum install ceph-common rbd-fuse rbd-nbd

echo "Disable Repos"
sed -ie 's/enabled=1/enabled=0/g' /etc/yum.repos.d/CentOS-Base.repo
sed -ie 's/enabled=1/enabled=0/g' /etc/yum.repos.d/epel.repo

echo "Installing files"
cp bins/waitdmmerging.sh		/usr/bin
chmod 755 /usr/bin/waitdmmerging.sh
cp bins/ceph_plugin.py	/etc/xapi.d/plugins/ceph_plugin
chmod +x /etc/xapi.d/plugins/ceph_plugin

cp bins/RBDSR.py		/opt/xensource/sm
cp bins/cephutils.py	/opt/xensource/sm
python -m compileall /opt/xensource/sm/RBDSR.py
python -O -m compileall /opt/xensource/sm/RBDSR.py
python -m compileall /opt/xensource/sm/cephutils.py
python -O -m compileall /opt/xensource/sm/cephutils.py
chmod +x /opt/xensource/sm/RBDSR.py
chmod +x /opt/xensource/sm/cephutils.py
ln -s /opt/xensource/sm/RBDSR.py /opt/xensource/sm/RBDSR

echo "Add RBDSR plugin to whitelist of SM plugins in /etc/xapi.conf"
cp /etc/xapi.conf /etc/xapi.conf.backup
grep "sm-plugins" /etc/xapi.conf
grep "sm-plugins" /etc/xapi.conf | grep -q "rbd" || sed -ie 's/sm-plugins\(.*\)/& rbd/g' /etc/xapi.conf
grep "sm-plugins" /etc/xapi.conf | grep "rbd"
