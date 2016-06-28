# How to rebuild CEPH MONs if you lost all of them

Recently I have lost all CEPH monitors because of my carelessness, but OSD's weren't corrupted.
In searching the internet I had discovered that it is possible to rebuild MONs ([http://www.spinics.net/lists/ceph-devel/msg06662.html](http://www.spinics.net/lists/ceph-devel/msg06662.html)) but there weren't any instructions how to do this, so I had decided to write my own. Maybe it can be useful for someone. 

1. Install `ceph-test` package if not installed.   

2. Rebuild all MONs from scratch

3. Determine the last osdmap epoch

    	# find /var/lib/ceph/osd/ceph-24 | grep "\/osdmap." | awk -F\/ '{print $11}' | awk -F\_ '{print $1}' | awk -F\. '{print $2}' | sort -n```

    The biggest digit will be the last osdmap epoch. In my case it was 20640

4. Retrieve the osdmap for certain (last in our case) epoch from OSD
    
    	# ceph-objectstore-tool --op get-osdmap --file osdmap.20640 --epoch 20640 --data-path /var/lib/ceph/osd/ceph-24 --journal-path /var/lib/ceph/osd/ceph-24/journal
   
5. Load the retrieved osdmap into MONs store

    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db set osdmap 20640 in osdmap.20640
    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db set osdmap full_20640 in osdmap.20640

6. Retrieve current values for keys: `osdmap.last_committed`, `osdmap.full_latest`, `pgmap_meta.last_osdmap_epoch`, `osdmap.first_committed` from MON store

    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db get osdmap last_committed out last_committed
    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db get osdmap full_latest out full_latest
    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db get pgmap_meta last_osdmap_epoch out last_osdmap_epoch
    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db get osdmap first_committed out first_committed

7. Using any hex editor change the epoch in all received files from previous step to last epoch

8. Update the values in MONs store

    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db set osdmap last_committed in last_committed
    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db set osdmap full_latest in full_latest
    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db set pgmap_meta last_osdmap_epoch in last_osdmap_epoch
    	# ceph-kvstore-tool leveldb /var/lib/ceph/mon/ceph-mon1/store.db set osdmap first_committed in first_committed

9. Start all MONs. The cluster should be up and running