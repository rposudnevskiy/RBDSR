# How to deploy the plugin and SR using ansible

Recently I have lost all CEPH monitors because of my carelessness, but OSD's weren't corrupted.
In searching the internet I had discovered that it is possible to rebuild MONs ([http://www.spinics.net/lists/ceph-devel/msg06662.html](http://www.spinics.net/lists/ceph-devel/msg06662.html)) but there weren't any instructions how to do this, so I had decided to write my own. Maybe it can be useful for someone. 

1. Copy the ansible/config.yml and ansible/hosts to your working directory

2. Modify the config.yml and hosts files according to your infrastructure

3. Run the playbook:

	# ansible-playbook -i hosts -e @config.yml ansible/xs_rbdsr.yml

