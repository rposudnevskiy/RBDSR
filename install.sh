#!/bin/bash
DEFAULT_CEPH_VERSION="luminous"

# Usage: installCephRepo <ceph-version>
function installCephRepo {
  echo "Install new Repos"
  yum install --enablerepo="extras,base" -y centos-release-ceph-$1.noarch
  echo "centos" > /etc/yum/vars/contentdir
}

# Usage: removeCephRepo <ceph-version>
function removeCephRepo {
  yum erase -y centos-release-ceph-$1.noarch
}

# Usage: removeXCPngRepo
function removeXCPngRepo {
  rm -f /etc/yum.repos.d/xcp-ng.repo
}

# Usage: installXCPngRepo
function installXCPngRepo {
  major_version=`cat /etc/centos-release | awk '{print $3}' | awk -F. '{print $1}'`
  major_minor_version=`cat /etc/centos-release | awk '{print $3}' | awk -F. '{print $1"."$2}'`
  cat << EOF >/etc/yum.repos.d/xcp-ng.repo
[xcp-ng-extras_testing]
name=XCP-ng Extras Testing Repository
baseurl=https://updates.xcp-ng.org/${major_version}/${major_minor_version}/extras_testing/x86_64/
enabled=0
gpgcheck=0
EOF
}

# Usage: confirmInstallation
function confirmInstallation {
  echo "This script is going to install 'xcp-ng-extras_testing' repository"
  echo "and upgrade 'glibc' and 'qemu-dp' packages."
  echo "Please note that Ceph support is experimental and can lead to"
  echo "an unstable system and data loss"
  default='no'
  while true; do
    read -p "Continue? (y[es]/n[o]) [${default}]: " yesorno
    if [ -z ${yesorno} ];then
      yesorno=${default}
    fi
    case ${yesorno} in
      yes|y)
            ret=0
            break
            ;;
       no|n)
            ret=1
            break
            ;;
    esac
  done
  return ${ret}
}

# Usage: setReposEnabled <repo filename> <section name> <0|1>
function setReposEnabled {
  echo "Set $1 $2 enabled = $3"
  sed -ie "/\[$2\]/,/^\[/s/enabled=[01]/enabled=$3/" /etc/yum.repos.d/$1
}

# Usage: backupFile <path>
function backupFile {
  echo "Backing Up file $1"
  if [ -e $1.bkp ]; then
    echo "$1.bkp already in place, not backing up!"
  else
    mv $1 $1-orig
  fi
}

# Usage: restoreFile <path>
function restoreFile {
  echo "Restore file $1"
  if [ -e $1.bkp ]; then
    mv $1.bkp $1
  else
    echo "No $1-orig in place, not restoring!"
  fi
}

# Usage: copyFile <source path> <destination path>
function copyFile {
  cp $1 $2
  chmod +x $2
}

function configureFirewall {
  iptables -A INPUT -p tcp --dport 6789 -j ACCEPT
  iptables -A INPUT -m multiport -p tcp --dports 6800:7300 -j ACCEPT
  service iptables save
}

function unconfigureFirewall {
  iptables -D INPUT -p tcp --dport 6789 -j ACCEPT
  iptables -D INPUT -m multiport -p tcp --dports 6800:7300 -j ACCEPT
  service iptables save
}

function installCeph {
  echo "Install Ceph API"
  yum install --enablerepo="extras,base" -y python-rbd rbd-nbd
}

function uninstallCeph {
  echo "Uninstall Ceph API"
  yum erase -y python-rbd rbd-nbd
}

function upgradeDeps {
  yum install --enablerepo="xcp-ng-extras_testing*" -y qemu-dp
  yum install --enablerepo="extras,base" -y glibc-2.17-222.el7
}

function downgradeDeps {
  yum history undo -y `yum history packages-list glibc | head -4 | tail -1 | awk -F\| '{gsub(/ /, "", $0); print $1}'`
  yum history undo -y `yum history packages-list qemu-dp | head -4 | tail -1 | awk -F\| '{gsub(/ /, "", $0); print $1}'`
}

function installFiles {
  echo "Install RBDSR Files"
  rm -rf /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr
  mkdir -p /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr

  copyFile "volume/org.xen.xapi.storage.rbdsr/plugin.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/plugin.py"
  copyFile "volume/org.xen.xapi.storage.rbdsr/sr.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/sr.py"
  copyFile "volume/org.xen.xapi.storage.rbdsr/volume.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/volume.py"

  ln -s plugin.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Plugin.diagnostics
  ln -s plugin.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Plugin.Query
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.probe
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.attach
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.create
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.destroy
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.detach
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.ls
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.stat
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.set_description
  ln -s sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.set_name
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.clone
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.create
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.destroy
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.resize
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set_description
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set_name
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.snapshot
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.stat
  ln -s volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.unset

  python -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/plugin.py
  python -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/sr.py
  python -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/volume.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/plugin.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/sr.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/volume.py

  rm -rf /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk
  mkdir -p /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk

  copyFile "datapath/rbd+raw+qdisk/plugin.py" "/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/plugin.py"
  copyFile "datapath/rbd+raw+qdisk/datapath.py" "/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/datapath.py"

  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.activate
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.attach
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.close
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.deactivate
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.detach
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.open
  ln -s plugin.py /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Plugin.Query

  python -m compileall /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/plugin.py
  python -m compileall /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/datapath.py
  python -O -m compileall /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/plugin.py
  python -O -m compileall /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/datapath.py

  ln -s rbd+raw+qdisk /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk

  rm -rf /lib/python2.7/site-packages/xapi/storage/libs/librbd
  mkdir /lib/python2.7/site-packages/xapi/storage/libs/librbd

  copyFile "xapi/storage/libs/librbd/__init__.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/__init__.py"
  copyFile "xapi/storage/libs/librbd/ceph_utils.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/ceph_utils.py"
  copyFile "xapi/storage/libs/librbd/datapath.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/datapath.py"
  copyFile "xapi/storage/libs/librbd/meta.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/meta.py"
  copyFile "xapi/storage/libs/librbd/qemudisk.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/qemudisk.py"
  copyFile "xapi/storage/libs/librbd/rbd_utils.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/rbd_utils.py"
  # copyFile "xapi/storage/libs/librbd/tapdisk.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/tapdisk.py"
  copyFile "xapi/storage/libs/librbd/utils.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/utils.py"
  copyFile "xapi/storage/libs/librbd/volume.py" "/lib/python2.7/site-packages/xapi/storage/libs/librbd/volume.py"

  ln -s /usr/share/qemu/qmp/qmp.py /lib/python2.7/site-packages/xapi/storage/libs/librbd/qmp.py

  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/__init__.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/ceph_utils.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/datapath.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/meta.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/qemudisk.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/rbd_utils.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/tapdisk.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/utils.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/volume.py
  python -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/qmp.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/__init__.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/ceph_utils.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/datapath.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/meta.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/qemudisk.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/rbd_utils.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/tapdisk.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/utils.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/volume.py
  python -O -m compileall /lib/python2.7/site-packages/xapi/storage/libs/librbd/qmp.py
}

function removeFiles {
  echo "Removing RBDSR Files"
  rm -rf /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr
  rm -rf /usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk
  rm -rf /lib/python2.7/site-packages/xapi/storage/libs/librbd
  }

function install {
  installCephRepo $1
  installCeph
  installFiles
  installXCPngRepo
  upgradeDeps
  configureFirewall
}

function deinstall {
  unconfigureFirewall
  downgradeDeps
  removeXCPngRepo
  removeFiles
  uninstallCeph
  removeCephRepo $1
}

case $1 in
    install)
        if confirmInstallation; then
            if [ -z "$2" ]; then
                install ${DEFAULT_CEPH_VERSION}
            else
                if [ -z `echo $2|egrep "^jewel$|^kraken$|^luminous$|^mimic$"` ]; then
                    echo "[ERROR]: Unsupported Ceph version specified '$2'"
                    exit 1
                else
                    install $2
                fi
            fi
        fi
        ;;
    deinstall)
        CEPH_INSTALLED_VERSION=`ls /etc/yum.repos.d/ | awk 'match($0, /CentOS-Ceph-(.*).repo/, a) {print a[1]}' | awk '{print tolower($0)}'`
        if [ -z "${CEPH_INSTALLED_VERSION}" ]; then
            echo "[ERROR]: Can't determine installed version of Ceph."
            echo "         RBDSR plugin is not installed or corrupted."
            exit 2
        fi
        if [ -z "$2" ]; then
            deinstall ${CEPH_INSTALLED_VERSION}
        else
            if [ "$2" != "${CEPH_INSTALLED_VERSION}" ]; then
                echo "[ERROR]: Installed version of Ceph is '${CEPH_INSTALLED_VERSION}'"
                exit 3
            else
                deinstall $2
            fi
        fi
        ;;
    *)
        echo "Usage: $0 install|deinstall [jewel|kraken|luminous|mimic]"
        exit 1
        ;;
esac
