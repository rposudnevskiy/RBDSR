# RBDSR - XenServer 7.5 / XCP-ng 7.5 Storage Manager plugin for CEPH (v3.0) for SMAPIv3
This plugin adds support for Ceph block devices into XenServer / XCP-ng.
It supports the creation of VDI as RBD device in Ceph pool.

Please note that `v1.0` and `v2.0` and `v3.0` are not compatible. At the moment there is no mean to migrate between versions. Hope to implement it later.

Plugin requires `qemu-dp.x86_64` ([xcp-ng-rpms/qemu-dp](https://github.com/xcp-ng-rpms/qemu-dp)), `python-rbd.x86_64` and `rbd-nbd.x86_64` packages installed

## Installation

1. Run this command:

		# sh <(curl -s https://raw.githubusercontent.com/rposudnevskiy/RBDSR/v3.0/netinstall.sh)

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

1. Create a pool on your Ceph cluster to store VDI images (should be executed on Ceph cluster node). The naming convention RBD_XenStorage-<uuid> is important!:

		# uuidgen
		4ceb0f8a-1539-40a4-bee2-450a025b04e1

		# ceph osd pool create RBD_XenStorage-4ceb0f8a-1539-40a4-bee2-450a025b04e1 128 128 replicated

2. Introduce the pool created in previous step as Storage Repository on XenServer / XCP-ng hosts:

		  xe sr-introduce name-label="CEPH RBD Storage" type=rbdsr uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 shared=true content-type=user

3. Run the ```xe host-list``` command to find out the host UUID for Xenserer / XCP-ng host:

		# xe host-list
		uuid ( RO) : 83f2c775-57fc-457b-9f98-2b9b0a7dbcb5
		name-label ( RW): xenserver1
		name-description ( RO): Default install of XenServer

4. Create the PBD using the device SCSI ID, host UUID and SR UUID detected above:

		# xe pbd-create sr-uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 host-uuid=83f2c775-57fc-457b-9f98-2b9b0a7dbcb5 device-config:cluster=ceph device-config:image-format=raw device-config:datapath=qdisk
		aec2c6fc-e1fb-0a27-2437-9862cffe213e

5. Attach the PBD created with xe pbd-plug command:

		# xe pbd-plug uuid=aec2c6fc-e1fb-0a27-2437-9862cffe213e

	The SR should be connected to the XenServer / XCP-ng hosts and be visible in XenCenter.
 / XCP-ng Center