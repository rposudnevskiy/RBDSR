# How to deploy the plugin and SR using ansible

1. Copy the ansible/config.yml and ansible/hosts files to your working directory

2. Modify the config.yml and hosts files according to your infrastructure and indications

3. Run the playbook:

	ansible-playbook -i hosts -e @config.yml ansible/xs_rbdsr.yml


Requirements: Ansible >= 2.0
