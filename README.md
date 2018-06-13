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
 - `rbd-pool-suffix` - Use a suffix to 'RBD_XenStorage' to better label the SR, default: ''
 - `vdi-update-existing` - Update params of existing VDIs on scan (optional): `True`, `False` (default)

Please note that `v1.0` and `v2.0` are not compatible. At the moment there is no mean to migrate from `v1.0` to `v2.0`. Hope to implement it later.

## Installation

This plugin uses `rbd`, `rbd-nbd` add `rbd-fuse` utilities for manipulating RBD devices, so the install script will install ceph-common, rbd-nbd and rbd-fuse packages from ceph repository on your XenServer hosts.


1. Run this command at your workstation or your management host and checkout a suitable branch:
   ```bash
   git clone https://github.com/rposudnevskiy/RBDSR 
   git checkout master
   ```
2. Push the installation files to your xenhosts
   ```bash
   ./push root@xen-001 root@xen-002 root@xen-003 root@xen-004
   ```
3. Create ```/etc/ceph/ceph.conf``` accordingly you Ceph cluster. Think about tuning rbd access.<BR>
   The easiest way is just copy it from your Ceph cluster node <BR>
   (see also http://docs.ceph.com/docs/luminous/rbd/rbd-config-ref/)
   ```
   [global]
   fsid = 90883703-ae94-4b17-9c43-90d92fcd5621
 
   mon host = 10.23.21.13,10.23.21.14,10.23.21.15
 
   public network = 10.23.21.0/24
   ```
4. Restart XAPI tool-stack on XenServer hosts
   ```
   xe-toolstack-restart
   ```
5. XenServer Tuning
   ```
    /opt/xensource/libexec/xen-cmdline --set-dom0 blkbk.reqs=256
     
    # Order 4 => 2^4 = 16 inflight requests
    /opt/xensource/libexec/xen-cmdline --set_dom0 blkbk.max_ring_page_order=4
     
    /opt/xensource/libexec/xen-cmdline --get-xen dom0_mem
    dom0_mem=4096M,max:4096M
    /opt/xensource/libexec/xen-cmdline --set-xen dom0_mem=16384M,max:16384M
    /opt/xensource/libexec/xen-cmdline --get-xen dom0_max_vcpus
    dom0_max_vcpus=16
    /opt/xensource/libexec/xen-cmdline --set-xen dom0_max_vcpus=32
     
    # pin cpus
    /opt/xensource/libexec/xen-cmdline --set-xen dom0_vcpus_pin
   ```
6. Reboot XenServers

## Removal

1. Remove all Ceph RBD SR out of XenServer with the appropriate commands.
2. Run this command:
   ```bash
   ~/RDBSR/install.sh deinstall
   ```
3. Restart XAPI tool-stack on XenServer hosts
   ```bash
   xe-toolstack-restart
   ```

## Usage

1. Create a pool on your Ceph cluster to store VDI images (should be executed on Ceph cluster node and the placemengroups should be suitable to your setup). <BR>
   The naming convention RBD_XenStorage-<type>-<uuid> is important!:
   ```bash
	# uuidgen
	4ceb0f8a-1539-40a4-bee2-450a025b04e1

	# ceph osd pool create RBD_XenStorage-DEV-SSD-4ceb0f8a-1539-40a4-bee2-450a025b04e1 128 128 replicated
    # ceph osd pool application enable RBD_XenStorage-DEV-SSD-4ceb0f8a-1539-40a4-bee2-450a025b04e1 rbd

    ```
4. Create one ore more access keyrings to provide XenServer access to your ceph setup.
   ```bash
   ceph auth get-or-create client.xen_test -o /etc/ceph/ceph.client.xen.keyring
   ceph auth caps client.xen_test mon 'allow profile rbd' osd 'allow class-read object_prefix rbd_children, allow rwx pool=RBD_XenStorage-DEV-SSD-4ceb0f8a-1539-40a4-bee2-450a025b04e1'
   rbd --id xen ls -p RBD_XenStorage-DEV-SSD-4ceb0f8a-1539-40a4-bee2-450a025b04e
   ```
6. Copy Keyring to all XENServers
   ```bash
   /etc/ceph/ceph.client.xen.keyring
   ```

2. Introduce the pool created in previous step as Storage Repository on XenServer hosts:
   ```bash
	# xe sr-introduce name-label="CEPH RBD Storage" type=rbd uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 shared=true content-type=user
    ```
3. Run the ```xe host-list``` command to find out the host UUID for Xenserer host:
   ```bash
    # xe host-list
    uuid ( RO) : 83f2c775-57fc-457b-9f98-2b9b0a7dbcb5
    name-label ( RW): xenserver1
    name-description ( RO): Default install of XenServer
    ```
4. Create the PBD using the device SCSI ID, host UUID and SR UUID detected above:
   ```bash
    # xe pbd-create sr-uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 host-uuid=83f2c775-57fc-457b-9f98-2b9b0a7dbcb5
    aec2c6fc-e1fb-0a27-2437-9862cffe213e
   ```
	If you would like to use a different cephx user or rbd mode, use the following device-config:
   ```bash
	# xe pbd-create sr-uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 host-uuid=83f2c775-57fc-457b-9f98-2b9b0a7dbcb5 device-config:cephx-id=xenserver device-config:rbd-mode=nbd
   ```

5. Attach the PBD created with xe pbd-plug command:
   ```bash
	# xe pbd-plug uuid=aec2c6fc-e1fb-0a27-2437-9862cffe213e
   ```
   The SR should be connected to the XenServer hosts and be visible in XenCenter.


## Debugging

Verbose logging in  /var/log/SMlog:

```
echo "VERBOSE = True" > /opt/xensource/sm/local_settings.py
```
