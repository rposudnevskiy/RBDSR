#!/bin/bash
DEFAULT_CEPH_VERSION="luminous"

# Usage: installRepo <ceph-version>
function installRepo {
  echo "Install new Repos"
  yum install -y centos-release-ceph-$1.noarch
}

# Usage: removeRepo <ceph-version>
function removeRepo {
  yum erase -y centos-release-ceph-$1.noarch
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
  yum install -y python-rbd
}

function uninstallCeph {
  echo "Uninstall Ceph API"
  yum erase -y python-rbd
}

function installFiles {
  echo "Install RBDSR Files"
  mkdir -p /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr

  copyFile "volume/org.xen.xapi.storage.rbdsr/plugin.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/plugin.py"
  copyFile "volume/org.xen.xapi.storage.rbdsr/ceph_utils.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/ceph_utils.py"
  copyFile "volume/org.xen.xapi.storage.rbdsr/rbd_utils.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/rbd_utils.py"
  copyFile "volume/org.xen.xapi.storage.rbdsr/utils.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/utils.py"
  copyFile "volume/org.xen.xapi.storage.rbdsr/sr.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/sr.py"
  copyFile "volume/org.xen.xapi.storage.rbdsr/volume.py" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/volume.py"

  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Plugin.diagnostics 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Plugin.Query 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.probe 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.attach 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.create 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.destroy 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.detach 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.ls 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.stat 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.set_description 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.set_name 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.clone 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.create 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.destroy 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.resize 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set_description 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set_name 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.snapshot 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.stat 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.unset 1>/dev/null 2>&1

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
  python -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/ceph_utils.py
  python -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/rbd_utils.py
  python -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/utils.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/plugin.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/sr.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/volume.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/ceph_utils.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/rbd_utils.py
  python -O -m compileall /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/utils.py

  copyFile "datapath/rbd/plugin.py" "/usr/libexec/xapi-storage-script/datapath/rbd/plugin.py"
  copyFile "datapath/rbd/datapath.py" "/usr/libexec/xapi-storage-script/datapath/rbd/datapth.py"

  unlink /usr/libexec/xapi-storage-script/datapath/rbd/ceph_utils.py 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/rbd_utils.py 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/utils.py 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/qmp.py 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.activate 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.attach 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.close 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.deactivate 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.detach 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.open 1>/dev/null 2>&1
  unlink /usr/libexec/xapi-storage-script/datapath/rbd/Plugin.Query 1>/dev/null 2>&1

  ln ../../volume/org.xen.xapi.storage.rbdsr/ceph_utils.py /usr/libexec/xapi-storage-script/datapath/rbd/ceph_utils.py
  ln ../../volume/org.xen.xapi.storage.rbdsr/rbd_utils.py /usr/libexec/xapi-storage-script/datapath/rbd/rbd_utils.py
  ln ../../volume/org.xen.xapi.storage.rbdsr/utils.py /usr/libexec/xapi-storage-script/datapath/rbd/utils.py
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.activate
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.attach
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.close
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.deactivate
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.detach
  ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd/Datapath.open
  ln -s plugin.py /usr/libexec/xapi-storage-script/datapath/rbd/Plugin.Query
  ln -s /usr/share/qemu/qmp/qmp.py /usr/libexec/xapi-storage-script/datapath/rbd/qmp.py

  python -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/plugin.py
  python -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/datapath.py
  python -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/ceph_utils.py
  python -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/rbd_utils.py
  python -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/utils.py
  python -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/qmp.py
  python -O -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/plugin.py
  python -O -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/datapath.py
  python -O -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/ceph_utils.py
  python -O -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/rbd_utils.py
  python -O -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/utils.py
  python -O -m compileall /usr/libexec/xapi-storage-script/datapath/rbd/qmp.py

}

function removeFiles {
  echo "Removing RBDSR Files"
  rm -rf /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr
  rm -rf /usr/libexec/xapi-storage-script/datapath/rbd
  }

function install {
  installRepo $1
  setReposEnabled "CentOS-Base.repo" "base" 1
  setReposEnabled "CentOS-Base.repo" "extras" 1
  installCeph
  setReposEnabled "CentOS-Base.repo" "base" 0
  setReposEnabled "CentOS-Base.repo" "extras" 0
  installFiles
  configureFirewall
}

function deinstall {
  unconfigureFirewall
  removeFiles
  uninstallCeph
  removeRepo $1
}

case $1 in
    install)
        if [ -z "$2" ]; then
            install $DEFAULT_CEPH_VERSION
        else
            if [ -z `echo $2|egrep "^jewel$|^kraken$|^luminous$"` ]; then
                echo "[ERROR]: Unsupported Ceph version specified '$2'"
                exit 1
            else
                install $2
            fi
        fi
        ;;
    deinstall)
        CEPH_INSTALLED_VERSION=`ls /etc/yum.repos.d/ | awk 'match($0, /CentOS-Ceph-(.*).repo/, a) {print a[1]}' | awk '{print tolower($0)}'`
        if [ -z "$CEPH_INSTALLED_VERSION" ]; then
            echo "[ERROR]: Can't determine installed version of Ceph."
            echo "         RBDSR plugin is not installed or corrupted."
            exit 2
        fi
        if [ -z "$2" ]; then
            deinstall $CEPH_INSTALLED_VERSION
        else
            if [ "$2" != "$CEPH_INSTALLED_VERSION" ]; then
                echo "[ERROR]: Installed version of Ceph is '$CEPH_INSTALLED_VERSION'"
                exit 3
            else
                deinstall $2
            fi
        fi
        ;;
    *)
        echo "Usage: $0 install|deinstall [jewel|kraken|luminous]"
        exit 1
        ;;
esac
