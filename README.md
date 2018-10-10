# RBDSR - XenServer 7.5 / XCP-ng 7.5 Storage Manager plugin for CEPH (v3.0) for SMAPIv3
This plugin adds support for Ceph block devices into XenServer / XCP-ng.
It supports the creation of VDI as RBD device in Ceph pool.

Please note that `v1.0` and `v2.0` and `v3.0` are not compatible. At the moment there is no mean to migrate between versions. Hope to implement it later.

Plugin requires `qemu-dp.x86_64` ([xcp-ng-rpms/qemu-dp](https://github.com/xcp-ng-rpms/qemu-dp)), `python-rbd.x86_64` and `rbd-nbd.x86_64` packages installed

## Installation

1. Run this command:

		# sh <(curl -s https://raw.githubusercontent.com/rposudnevskiy/RBDSR/v3.0/netinstall.sh)

   To install certain version of ceph storage, provide version name (```jewel```, ```luminous``` or ```mimic```) as first parameter:

		# sh <(curl -s https://raw.githubusercontent.com/rposudnevskiy/RBDSR/v3.0/netinstall.sh) mimic
 
2. Create ```/etc/ceph/ceph.conf``` accordingly you Ceph cluster. The easiest way is just copy it from your Ceph cluster node

3. Copy ```/etc/ceph/ceph.client.admin.keyring``` to XenServer / XCP-ng hosts from your Ceph cluster node.

4. Restart Xapi storage script plugin server on XenServer / XCP-ng hosts

		# systemctl restart xapi-storage-script.service

## Removal
1. Remove all Ceph RBD SR out of XenServer / XCP-ng with the appropriate commands.

2. Run this command:

		# ~/RDBSRv3.0/install.sh deinstall

3. Restart Xapi storage script plugin server on XenServer / XCP-ng hosts

		# systemctl restart xapi-storage-script.service


## Usage

Create a pool:

		# xe sr-create host-uuid=fb0d42fc-0a4d-459d-8b90-6ed6610c2e4c name-label="CEPH RBD Storage" name-shared=true type=rbdsr content-type=user device-config:cluster=ceph device-config:image-format=qcow2 device-config:datapath=qdisk


The SR should be connected to the XenServer / XCP-ng hosts and be visible in XenCenter.
 / XCP-ng Center
