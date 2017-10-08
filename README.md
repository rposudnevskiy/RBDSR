# RBDSR - XenServer Storage Manager plugin for CEPH (v2.0)
This plugin adds support for Ceph block devices into XenServer.
It supports the creation of VDI as RBD device in Ceph pool.
It uses three different approaches in three different driver types to handle VDI snapshots and clones:
- VHD driver uses VHD Differencing images
- DMP driver uses Device-Mapper snapshot* targets [(Device-mapper snapshot support)](https://www.kernel.org/doc/Documentation/device-mapper/snapshot.txt)
- RBD driver uses native Ceph snapshots and clones.

It also supports Xapi Storage Migration (XSM) and XenServer High Availability (HA).

You can change the following device configs using `device-config` args when creating PBDs on each host:
 - `rbd-mode` - SR mount mode (optional): `kernel`, `fuse`, `nbd` (default)
 - `driver-type` - Driver type (optional): `vhd` (default), `dmp`, `rbd`
 - `cephx-id` - Cephx id to be used (optional): default is `admin`
 - `use-rbd-meta` - Store VDI params in rbd metadata (optional): `True` (default), `False` (Use a separate image to store VDIs metadata. It's not implemented yet)
 - `vdi-update-existing` - Update params of existing VDIs on scan (optional): `True`, `False` (default)

Please note that `v1.0` and `v2.0` are not compatible. At the moment there is no mean to migrate from `v1.0` to `v2.0`. Hope to implement it later.

## Installation

This plugin uses `rbd`, `rbd-nbd` add `rbd-fuse` utilities for manipulating RBD devices, so the install script will install ceph-common, rbd-nbd and rbd-fuse packages from ceph repository on your XenServer hosts.

1. Run this command:

		# sh <(curl -s https://raw.githubusercontent.com/rposudnevskiy/RBDSR/v2.0/netinstall.sh)

2. Create ```/etc/ceph/ceph.conf``` accordingly you Ceph cluster. The easiest way is just copy it from your Ceph cluster node

3. Copy ```/etc/ceph/ceph.client.admin.keyring``` to XenServer hosts from your Ceph cluster node.

4. Restart XAPI tool-stack on XenServer hosts

		# xe-toolstack-restart

## Removal
1. Remove all Ceph RBD SR out of XenServer with the appropriate commands.

2. Run this command:

		# ~/RDBSR/install.sh deinstall

3. Restart XAPI tool-stack on XenServer hosts

		# xe-toolstack-restart


## Usage

1. Create a pool on your Ceph cluster to store VDI images (should be executed on Ceph cluster node). The naming convention RBD_XenStorage-<uuid> is important!:

		# uuidgen
		4ceb0f8a-1539-40a4-bee2-450a025b04e1

		# ceph osd pool create RBD_XenStorage-4ceb0f8a-1539-40a4-bee2-450a025b04e1 128 128 replicated

2. Introduce the pool created in previous step as Storage Repository on XenServer hosts:

		  xe sr-introduce name-label="CEPH RBD Storage" type=rbd uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 shared=true content-type=user

3. Run the ```xe host-list``` command to find out the host UUID for Xenserer host:

		# xe host-list
		uuid ( RO) : 83f2c775-57fc-457b-9f98-2b9b0a7dbcb5
		name-label ( RW): xenserver1
		name-description ( RO): Default install of XenServer

4. Create the PBD using the device SCSI ID, host UUID and SR UUID detected above:

		# xe pbd-create sr-uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 host-uuid=83f2c775-57fc-457b-9f98-2b9b0a7dbcb5
		aec2c6fc-e1fb-0a27-2437-9862cffe213e

	If you would like to use a different cephx user or rbd mode, use the follwing device-config:

		# xe pbd-create sr-uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 host-uuid=83f2c775-57fc-457b-9f98-2b9b0a7dbcb5 device-config:cephx-id=xenserver device-config:rbd-mode=kernel


5. Attach the PBD created with xe pbd-plug command:

		# xe pbd-plug uuid=aec2c6fc-e1fb-0a27-2437-9862cffe213e

	The SR should be connected to the XenServer hosts and be visible in XenCenter.
