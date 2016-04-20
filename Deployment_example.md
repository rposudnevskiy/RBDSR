Let's imagine that you have four new HP DL360 G8 servers, each with 2x480GB SSD and 6x2TB HDD, and you want to set up XenServer pool using local server disks as storage. Here is a short description how you can get it.

#Preparing XenServer hosts

You need two USB-sticks for each server. The first one for XenServer itself and the second one as local storage to place Ceph cluster node VM on it. I don't recommend using the one USB-stick for both XenServer and local storage as you can have performance issues with such configuration. 
Anyway, I ran into performance problems when I used only one USB-stick.

1. Install XenServer on each server.

2. Download and install RBDSR plugin [http://github.com](http://github.com)  on each server. This plugin uses ****rbd**** utility for manipulating RBD devices, so you need to install ceph-common package from ceph repository.

	- Make backup copy of ```CentOS-Base.repo``` and ```Citrix.repo```
 
			# cd /etc/yum.repos.d/
			# cp CentOS-Base.repo CentOS-Base.repo.orig
			# cp Citrix.repo Citrix.repo.orig

	- Make these changes in ```CentOS-Base.repo```

		In ```[base]``` section

		comment out the line
	
			baseurl=http://www.uk.xensource.com/linux/distros/CentOS/7.0.1406/os/x86_64/

		uncomment the line
	
			#baseurl=http://mirror.centos.org/centos/$releasever/os/$basearch/

		In ```[updates]``` section

		comment out the line

			baseurl=http://www.uk.xensource.com/linux/distros/CentOS/7.0.1406/updates-20140901/x86_64/

		uncomment the line

			#baseurl=http://mirror.centos.org/centos/$releasever/updates/$basearch/

		In ```[extras]``` section

		comment out the line

			baseurl=http://www.uk.xensource.com/linux/distros/CentOS/7.0.1406/extras-20140902/x86_64/

		uncomment the line

			#baseurl=http://mirror.centos.org/centos/$releasever/extras/$basearch/

		replace ```$releasever``` in uncommented lines with 7.2.1511

	- disable Citrix.repo

			[citrix]
			enabled=0

	- Create ```ceph.repo``` file and put in it next lines:

			[ceph]
			name=Ceph packages for $basearch
			baseurl=http://download.ceph.com/rpm-infernalis/el7/$basearch
			enabled=1
			priority=2
			gpgcheck=1
			type=rpm-md
			gpgkey=https://download.ceph.com/keys/release.asc
		
			[ceph-noarch]
			name=Ceph noarch packages
			baseurl=http://download.ceph.com/rpm-infernalis/el7/noarch
			enabled=1
			priority=2
			gpgcheck=1
			type=rpm-md
			gpgkey=https://download.ceph.com/keys/release.asc
		
			[ceph-source]
			name=Ceph source packages
			baseurl=http://download.ceph.com/rpm-infernalis/el7/SRPMS
			enabled=0
			priority=2
			gpgcheck=1
			type=rpm-md
			gpgkey=https://download.ceph.com/keys/release.asc

		I use Infernalis release but you can use other.
		If so then you should change the ```baseurl``` lines in ceph.repo accordingly your release

	- Install ```the release.asc``` key:

			# rpm --import 'https://download.ceph.com/keys/release.asc'

	- Install required packages:

			# yum install epel-release	
			# yum install yum-plugin-priorities.noarch
			# yum install snappy leveldb gdisk python-argparse gperftools-libs
			# yum install ceph-common

	- Restore backup copy of ```CentOS-Base.repo``` and ```Citrix.repo```

			# cp CentOS-Base.repo.orig CentOS-Base.repo
			# cp Citrix.repo.orig Citrix.repo

	- Disable ```epel.repo```

			[epel]
			enabled=0
	
			[epel-debuginfo]
			enabled=0
	
			[epel-source]
			enabled=0

	- Disable ceph.repo

			[ceph]
			enabled=0
	
			[ceph-noarch]
			enabled=0
	
			[ceph-source]
			enabled=0

	- Download RBDSR plugin [http://github.com](http://github.com)

	- Put the ```waitdmmerging.sh``` into ```/usr/bin/``` and change permission: 

			# chmod 755 /usr/bin/waitdmmerging.sh

	- Put the ```RBDSR.py``` and ```cephutils.py``` into  ```/opt/xensource/sm``` and compile them:

			# python -m compileall RBDSR.py
			# python -O -m compileall RBDSR.py
			# python -m compileall cephutils.py
			# python -O -m compileall cephutils.py

	- Make softlink to ```RBDSR.py``` in ```/opt/xensource/sm```
	
			# ln -s RBDSR.py RBDSR 


	- Add RBDSR plugin to whitelist of SM plugins in ```/etc/xapi.conf```

			# Whitelist of SM plugins
			sm-plugins= rbd cifs ext nfs iscsi lvmoiscsi dummy file hba rawhba udev iso lvm lvmohba lvmofcoe

	- Restart XAPI tool-stack on XenServer hosts

			# xe-toolstack-restart 

2. Prepare the separate USB-stick as local storage on each server. Something like this:

	     xe sr-create host-uuid=5aded62f-73d3-4a08-a11f-458f9858c067 name-label="Local storage" type=ext device-config:device=/dev/disk/by-id/usb-TOSHIBA_TransMemory_FFFFFFFFFFFFEE8140002A5C-0:0-part3

3. We will use PCI passtrough to give control of server SATA controllers to Ceph cluster node VM. You can read about PCI passthrough here [http://wiki.xen.org/wiki/Xen_PCI_Passthrough](http://wiki.xen.org/wiki/Xen_PCI_Passthrough)
	
	- First of all let's determine the SATA controllers BDF:
		    
			# lspci -k | grep SATA
			00:11.4 SATA controller: Intel Corporation Wellsburg sSATA Controller [AHCI mode] (rev 05)
			00:1f.2 SATA controller: Intel Corporation Wellsburg 6-Port SATA Controller [AHCI mode] (rev 05)

		here ```00:11.4``` and ```00:1f.2``` are BDFs of our SATA controllers.

	- Add add ```xen-pciback.hide=(00:11.4)(00:1f.2)``` to the dom0 linux kernel command line in ```/boot/efi/EFI/xenserver/grub.cfg```

			...
			menuentry 'XenServer' {
        	search --label --set root root-dxzcye
        	multiboot2 /boot/xen.gz dom0_mem=4096M,max:4096M watchdog dom0_max_vcpus=16 crashkernel=128M@32M cpuid_mask_xsave_eax=0 console=vga vga=mode-0x0311
        	module2 /boot/vmlinuz-3.10-xen root=LABEL=root-dxzcye ro nolvm hpet=disable xencons=hvc console=hvc0 console=tty0 quiet vga=785 splash xen-pciback.hide=(00:11.4)(00:1f.2)
        	module2 /boot/initrd-3.10-xen.img
			}
			...

	- reboot the server
	
	- verify that devices are ready to be assigned to guest
		
		 	# xl pci-assignable-list
			0000:00:11.4
			0000:00:1f.2

4. Create new VMs with 16GB RAM using CentOS 7 template on each server
5. Attach SATA controllers to new VMs

		xe vm-param-set other-config:pci=0/0000:00:11.4,0/0000:00:1f.2 uuid=74a1b0a2-7c74-63df-7f1c-dfe6ee2a26eb

	here uuid is uuid of our VM

6. Download and install "CentOS 7 (1511) Minimal x86_64" in new VMs [https://wiki.centos.org/Download](https://wiki.centos.org/Download)

#Deploying Ceph

1. On each new VMs install and configure ntpd . It's very important to have clock synced on your Ceph nodes.

			# yum install ntp
			# systemctl enable ntpd.service
			# systemctl start ntpd.service

2. Install Ceph packages

	- Create ```/etc/yum.repos.d/ceph.repo``` file and put in it next lines:

			[ceph]
			name=Ceph packages for $basearch
			baseurl=http://download.ceph.com/rpm-infernalis/el7/$basearch
			enabled=1
			priority=2
			gpgcheck=1
			type=rpm-md
			gpgkey=https://download.ceph.com/keys/release.asc
			
			[ceph-noarch]
			name=Ceph noarch packages
			baseurl=http://download.ceph.com/rpm-infernalis/el7/noarch
			enabled=1
			priority=2
			gpgcheck=1
			type=rpm-md
			gpgkey=https://download.ceph.com/keys/release.asc
			
			[ceph-source]
			name=Ceph source packages
			baseurl=http://download.ceph.com/rpm-infernalis/el7/SRPMS
			enabled=0
			priority=2
			gpgcheck=1
			type=rpm-md
			gpgkey=https://download.ceph.com/keys/release.asc

	I use Infernalis release but you can use other.
	If so then you should change the ```baseurl``` lines in ceph.repo accordingly your release

	- Install ```the release.asc``` key:

			# rpm --import 'https://download.ceph.com/keys/release.asc'

	- Install required packages:

			# yum install epel-release	
			# yum install redhat-lsb.x86_64
			# yum install yum-plugin-priorities.noarch
			# yum install snappy leveldb gdisk python-argparse gperftools-libs
			# yum install ceph

3. Prepare ```/etc/ceph/ceph.conf``` and start Ceph monitor daemons

	- generate fsid for our Ceph cluster
		
			# uuidgen
 			6a50cba9-2ba1-418d-b814-63051ffda596

	- determine the IPs for you Ceph cluster nodes and create ```/etc/ceph/ceph.conf``` like this:
			
			 [global]
			        fsid = 6a50cba9-2ba1-418d-b814-63051ffda596
        			mon initial members = VMCEPH1,VMCEPH2,VMCEPH3,VMHCEPH4
        			mon host = 10.126.70.14,10.126.70.15,10.126.70.16,10.126.70.17
        			public network = 10.126.70.0/24
        			auth cluster required = cephx
        			auth service required = cephx
        			auth client required = cephx
        			filestore xattr use omap = true
        			osd pool default size = 2
        			osd pool default min size = 1
        			osd pool default pg num = 768
        			osd pool default pgp num = 768
        			osd crush chooseleaf type = 1

			[osd]
        			osd op threads = 32
        			filestore op threads = 32
        			filestore journal writeahead = true

			[mon]

			[mon.VMCEPH1]
        			host = VMCEPH1
        			mon addr = 10.126.70.14:6789

			[mon.VMCEPH2]
        			host = VMCEPH2
        			mon addr = 10.126.70.15:6789

			[mon.VMCEPH3]
        			host = VMCEPH3
        			mon addr = 10.126.70.16:6789

			[mon.VMCEPH4]
        			host = VMCEPH4
        			mon addr = 10.126.70.17:6789

		you can find the full  ```/etc/ceph/ceph.conf``` for my configuration here [http://github.com](http://github.com)

	- Create a keyring for your cluster, generate a monitor secret key, generate an administrator keyring, generate a ```client.admin``` user and add the user to the keyring, add the client.admin key to the ```ceph.mon.keyring```. Do it only on first Ceph node:

			# ceph-authtool --create-keyring /tmp/ceph.mon.keyring --gen-key -n mon. --cap mon 'allow *'
			# ceph-authtool --create-keyring /etc/ceph/ceph.client.admin.keyring --gen-key -n client.admin --set-uid=0 --cap mon 'allow *' --cap osd 'allow *' --cap mds 'allow'
			# ceph-authtool /tmp/ceph.mon.keyring --import-keyring /etc/ceph/ceph.client.admin.keyring
			# monmaptool --create --add VMCEPH1 10.126.70.14 --add VMCEPH2 10.126.70.15 --add VMCEPH3 10.126.70.16 --add VMCEPH4 10.126.70.17 --fsid 6a50cba9-2ba1-418d-b814-63051ffda596 /tmp/monmap

	- Create a default data directories on each Ceph VM:
			
			# mkdir /var/lib/ceph/mon/ceph-VMCEPH1
			...
			# mkdir /var/lib/ceph/mon/ceph-VMCEPH2
			...
			# mkdir /var/lib/ceph/mon/ceph-VMCEPH3
			...
			# mkdir /var/lib/ceph/mon/ceph-VMCEPH4

	- copy the monitor map and keyring from first VM to others
			
			# scp /tmp/monmap 10.126.70.15:/tmp
			# scp /tmp/monmap 10.126.70.16:/tmp
			# scp /tmp/monmap 10.126.70.17:/tmp

			# scp /tmp/ceph.mon.keyring 10.126.70.15:/tmp
			# scp /tmp/ceph.mon.keyring 10.126.70.16:/tmp
			# scp /tmp/ceph.mon.keyring 10.126.70.17:/tmp

	- populate the monitor daemon with the monitor map and keyring on each Ceph VM:
	
			
			# ceph-mon --mkfs -i VMCEPH1 --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring
			...
			# ceph-mon --mkfs -i VMCEPH2 --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring
			...
			# ceph-mon --mkfs -i VMCEPH3 --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring
			...
			# ceph-mon --mkfs -i VMCEPH4 --monmap /tmp/monmap --keyring /tmp/ceph.mon.keyring


	- enable and start ceph-mon service on each Ceph VM 
			
			# systemctl enable ceph-mon@VMCEPH1.service
			# systemctl start ceph-mon@VMCEPH1.service
			...
			# systemctl enable ceph-mon@VMCEPH2.service
			# systemctl start ceph-mon@VMCEPH2.service
			...
			# systemctl enable ceph-mon@VMCEPH3.service
			# systemctl start ceph-mon@VMCEPH3.service
			...
			# systemctl enable ceph-mon@VMCEPH4.service
			# systemctl start ceph-mon@VMCEPH4.service
	
	- verify that the monitors are running.
	
			# ceph -s

		You should see output that the monitor you started is up and running. It should look something like this:

			cluster 6a50cba9-2ba1-418d-b814-63051ffda596
  			  health HEALTH_ERR 192 pgs stuck inactive; 192 pgs stuck unclean; no osds
  			  monmap e1: 1 mons at {node1=192.168.0.1:6789/0}, election epoch 1, quorum 0 node1
  			  osdmap e1: 0 osds: 0 up, 0 in
  			  pgmap v2: 192 pgs, 3 pools, 0 bytes data, 0 objects
     		  0 kB used, 0 kB / 0 kB avail
     		  192 creating

4. Add OSDs (repeat steps on each VM)

	- Verify that our VMs see the server's local disks

			# ls -1 /dev/sd? | xargs -I%% fdisk -l %% 2>/dev/null | grep "/dev"
			Disk /dev/sda: 2000.4 GB, 2000398934016 bytes, 3907029168 sectors
			Disk /dev/sdb: 2000.4 GB, 2000398934016 bytes, 3907029168 sectors
			Disk /dev/sdc: 2000.4 GB, 2000398934016 bytes, 3907029168 sectors
			Disk /dev/sdd: 2000.4 GB, 2000398934016 bytes, 3907029168 sectors
			Disk /dev/sde: 2000.4 GB, 2000398934016 bytes, 3907029168 sectors
			Disk /dev/sdf: 2000.4 GB, 2000398934016 bytes, 3907029168 sectors
			Disk /dev/sdg: 480.1 GB, 480103981056 bytes, 937703088 sectors
			Disk /dev/sdh: 480.1 GB, 480103981056 bytes, 937703088 sectors

	- we will use separate OSD for each of 2TB disks and ```/dev/sdg``` for journals. Let's do partitioning
	
			# parted -s /dev/sdg mklabel gpt
			# parted -s /dev/sdg -a opt mkpart 'journal-0' 1048576B 79457943551B
			# parted -s /dev/sdg -a opt mkpart 'journal-1' 79457943552B 158914838527B
 			# parted -s /dev/sdg -a opt mkpart 'journal-2' 158914838528B 238371733503B
 			# parted -s /dev/sdg -a opt mkpart 'journal-3' 238371733504B 317828628479B
 			# parted -s /dev/sdg -a opt mkpart 'journal-4' 317828628480B 397285523455B
 			# parted -s /dev/sdg -a opt mkpart 'journal-5' 397285523456B 476742418431B
			
			# parted -s /dev/sda -a opt mklabel gpt mkpart 'ceph_data_0' 1049kB 2000GB
			# parted -s /dev/sdb -a opt mklabel gpt mkpart 'ceph_data_1' 1049kB 2000GB
			# parted -s /dev/sdc -a opt mklabel gpt mkpart 'ceph_data_2' 1049kB 2000GB
			# parted -s /dev/sdd -a opt mklabel gpt mkpart 'ceph_data_3' 1049kB 2000GB
			# parted -s /dev/sde -a opt mklabel gpt mkpart 'ceph_data_4' 1049kB 2000GB
			# parted -s /dev/sdf -a opt mklabel gpt mkpart 'ceph_data_5' 1049kB 2000GB

	- create directories for OSDs:
	
			# mkdir /var/lib/ceph/osd/ceph-0
			# mkdir /var/lib/ceph/osd/ceph-1
			# mkdir /var/lib/ceph/osd/ceph-2
			# mkdir /var/lib/ceph/osd/ceph-3
			# mkdir /var/lib/ceph/osd/ceph-4
			# mkdir /var/lib/ceph/osd/ceph-5
			# chown -R ceph:ceph /var/lib/ceph

	- prepare drives for use with Ceph, and mount it to the directories you just created:
	
			# mkfs -t ext4 /dev/sda1
			# mkfs -t ext4 /dev/sdb1 
			# mkfs -t ext4 /dev/sdc1 
			# mkfs -t ext4 /dev/sdd1 
			# mkfs -t ext4 /dev/sde1 
			# mkfs -t ext4 /dev/sdf1 
			# mount -o user_xattr /dev/sda1 /var/lib/ceph/osd/ceph-0
			# mount -o user_xattr /dev/sdb1 /var/lib/ceph/osd/ceph-1
			# mount -o user_xattr /dev/sdc1 /var/lib/ceph/osd/ceph-2
			# mount -o user_xattr /dev/sdd1 /var/lib/ceph/osd/ceph-3
			# mount -o user_xattr /dev/sde1 /var/lib/ceph/osd/ceph-4
			# mount -o user_xattr /dev/sdf1 /var/lib/ceph/osd/ceph-5

	- create OSDs for data pool (repeat steps for each of six OSDs)
	
		- generate a UUID for the OSD
			
				# uuidgen
				9d4eba6d-e7b1-4420-b704-ee194b477534

		- create the OSD
		
				# ceph osd create 9d4eba6d-e7b1-4420-b704-ee194b477534
				0

				# ceph-osd -i 0 --mkjournal --osd-journal=/dev/sdg1 --mkfs --mkkey --osd-uuid 9d4eba6d-e7b1-4420-b704-ee194b477534
				# ceph auth add osd.0 osd 'allow *' mon 'allow rwx' -i /var/lib/ceph/osd/ceph-0/keyring
				# ceph osd crush add-bucket VMCEPH1 host
				# ceph osd crush move VMCEPH1 root=default
				# ceph osd crush add osd.0 1.0 host=VMCEPH1

	- create OSDs for cache pool (repeat steps on each VM)
					
				# uuidgen
				de03c20e-180d-4107-92b7-8ea47d8a2273

				# ceph osd create de03c20e-180d-4107-92b7-8ea47d8a2273
				24

				# mkfs -t ext4 /dev/sdh1
				# mount -o user_xattr /dev/sdh1 /var/lib/ceph/osd/ceph-24

				# ceph-osd -i 24 --mkfs --mkkey --osd-uuid de03c20e-180d-4107-92b7-8ea47d8a2273
				# ceph auth add osd.24 osd 'allow *' mon 'allow rwx' -i /var/lib/ceph/osd/ceph-24/keyring
				# ceph osd crush add osd.24 1.0 host=VMCEPH1

5. Create new crushmap. We will put data and cache OSDs into separate buckets and configure separate rulesets for data and cache pool.
	- get and decompile current crushmap

			# ceph osd getcrushmap -o /tmp/crush
			# crushtool -d /tmp/crush -o /tmp/crush.decompiled

	- edit ```/tmp/crush.decompiled```. Here crushmap that i use [http://github.com](http://github.com)         

	- compile and set new crushmap


			# crushtool -c /tmp/crush.decompiled -o /tmp/crush
			# ceph osd setcrushmap -i /tmp/crush

10. Create Ceph data poop that we will use as XenServer Storage Repository

		# uuidgen
		5aab7115-2d2c-466d-818c-909cff689467

	this uuid wil be used as uuid of XenServer's SR 

		# ceph osd pool create RBD_XenStorage-5aab7115-2d2c-466d-818c-909cff689467 768 768 replicated data_ruleset
 
11. Create and configure read-only cache pool

		# ceph osd pool create CACHE 128 128 replicated cache_ruleset

		# ceph osd tier add RBD_XenStorage-5aab7115-2d2c-466d-818c-909cff689467 CACHE
		# ceph osd tier cache-mode CACHE readonly

#Configuring XenServer hosts

1. Create ```/etc/ceph/ceph.conf``` accordingly you Ceph cluster. The easyest way is just copy it from your Ceph cluster VM

2. Create a ```client.xenserver``` key, and save a copy of the key for your XenServer host (should be executed on Ceph cluster VM):

		# ceph auth get-or-create client.xenserver mon 'allow *' osd 'allow *' -o /etc/ceph/ceph.client.xenserver.keyring

3. Copy ```/etc/ceph/ceph.client.xenserver.keyring``` to XenServer hosts. 

4. Introduce the Ceph pool created earlier as Storage Repository on XenServer hosts:

		  xe sr-introduce name-label="CEPH RBD Storage" type=rbd uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 shared=true content-type=user

5. Run the ```xe host-list``` command to find out the host UUID for Xenserer host:

		# xe host-list
		uuid ( RO) : 83f2c775-57fc-457b-9f98-2b9b0a7dbcb5
		name-label ( RW): xenserver1
		name-description ( RO): Default install of XenServer

6. Create the PBD using the device SCSI ID, host UUID and SR UUID detected above:

		# xe pbd-create sr-uuid=4ceb0f8a-1539-40a4-bee2-450a025b04e1 host-uuid=83f2c775-57fc-457b-9f98-2b9b0a7dbcb5
		aec2c6fc-e1fb-0a27-2437-9862cffe213e

7. Attach the PBD created with xe pbd-plug command:

		# xe pbd-plug uuid=aec2c6fc-e1fb-0a27-2437-9862cffe213e
		
	The SR should be connected to the XenServer hosts and be visible in XenCenter. 