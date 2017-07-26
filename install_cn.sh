#!/bin/bash
DEFAULT_CEPH_VERSION="jewel"
CEPH_REPO="mirrors.163.com\/ceph"

# Usage: installRepo <ceph-version>
function installRepo {
  echo "Install the release.asc key"
  rpm --import 'https://download.ceph.com/keys/release.asc'
  echo "Install new Repos"
  cp -n repos/ceph-$1.repo /etc/yum.repos.d/
  sed -i "s/download.ceph.com/$CEPH_REPO/g" /etc/yum.repos.d/ceph-$1.repo
}

function deinstallRepo {
  echo "deinstallRepo $1"
  rm -f /etc/yum.repos.d/ceph-$1.repo
}

# Usage: removeRepo <ceph-version>
function removeRepo {
  rm -f /etc/yum.repos.d/
}

# Usage: setReposEnabled <repo filename> <section name> <0|1>
function setReposEnabled {
  echo "Set $1 $2 enabled = $3"
  sed -ie "/\[$2\]/,/^\[/s/enabled=[01]/enabled=$3/" /etc/yum.repos.d/$1
}

# Usage: backupFile <path>
function backupFile {
  echo "Backing Up file $1"
  if [ -e $1-orig ]; then
    echo "$1-orig already in place, not backing up!"
  else
    mv $1 $1-orig
  fi
}

# Usage: restoreFile <path>
function restoreFile {
  echo "Backing Up file $1"
  if [ -e $1-orig ]; then
    mv $1 $1-orig
  else
    echo "No $1-orig in place, not restoring!"
  fi
}

# Usage: copyFile <source path> <destination path>
function copyFile {
  cp $1 $2
  chmod +x $2
}

function enableRBDSR {
  echo "Add RBDSR plugin to whitelist of SM plugins in /etc/xapi.conf"
  grep "sm-plugins" /etc/xapi.conf
  grep "sm-plugins" /etc/xapi.conf | grep -q "rbd" || sed -ie 's/sm-plugins\(.*\)/& rbd/g' /etc/xapi.conf
  grep "sm-plugins" /etc/xapi.conf | grep "rbd"
}

function disableRBDSR {
  echo "Remove RBDSR plugin to whitelist of SM plugins in /etc/xapi.conf"
  grep "sm-plugins" /etc/xapi.conf
  grep "sm-plugins" /etc/xapi.conf | grep -q "rbd" || sed -ie 's/\(sm-plugins\)\(.*\)rbd\(.*\)/\1\2\3/g' /etc/xapi.conf
  grep "sm-plugins" /etc/xapi.conf | grep "rbd"
}

function installEpel {
  echo "Install Required Packages"
  yum install -y epel-release  yum-plugin-priorities.noarch --enablerepo=base,extras --releasever=7
}

function installCeph {
  echo "Install RBDSR depenencies"
  yum install -y snappy leveldb gdisk python-argparse gperftools-libs fuse fuse-libs ceph-common rbd-fuse rbd-nbd --enablerepo=base,extras --releasever=7
}

function installFiles {
  echo "Install RBDSR Files"
  copyFile "bins/waitdmmerging.sh"      "/usr/bin/waitdmmerging.sh"
  copyFile "bins/ceph_plugin.py"        "/etc/xapi.d/plugins/ceph_plugin"
  copyFile "bins/RBDSR.py"              "/opt/xensource/sm/RBDSR"
  copyFile "bins/cephutils.py"          "/opt/xensource/sm/cephutils.py"

  #copyFile "bins/tap-ctl"              "/sbin/tap-ctl"
  copyFile "bins/vhd-tool"             "/bin/vhd-tool"
  copyFile "bins/sparse_dd"            "/usr/libexec/xapi/sparse_dd"

  copyFile "bins/rbd2vhd.py"           "/bin/rbd2vhd"

  ln "/bin/rbd2vhd" "/bin/vhd2rbd"
  ln "/bin/rbd2vhd" "/bin/rbd2raw"
  ln "/bin/rbd2vhd" "/bin/rbd2nbd"
}

function removeFiles {
  echo "Removing RBDSR Files"
  rm -f "/usr/bin/waitdmmerging.sh"
  rm -f "/etc/xapi.d/plugins/ceph_plugin"
  rm -f "/opt/xensource/sm/RBDSR"
  rm -f "/opt/xensource/sm/cephutils.py"

  rm -f "/sbin/tap-ctl"
  rm -f "/bin/vhd-tool"
  rm -f "/usr/libexec/xapi/sparse_dd"

  rm -f "/bin/rbd2vhd"

  rm -f "/bin/vhd2rbd"
  rm -f "/bin/rbd2raw"
  rm -f "/bin/rbd2nbd"
}


function install {
  installRepo $1
  installEpel
  installCeph

  #backupFile "/sbin/tap-ctl"
  backupFile "/bin/vhd-tool"
  backupFile "/usr/libexec/xapi/sparse_dd"

  installFiles

  enableRBDSR
}

function deinstall {
  disableRBDSR
  removeFiles
  restoreFile "/sbin/tap-ctl"
  restoreFile "/bin/vhd-tool"
  restoreFile "/usr/libexec/xapi/sparse_dd"
  deinstallRepo $1
}

case $1 in
    update)
  	yum update -y ceph-common rbd-fuse rbd-nbd --enablerepo=base,extras --releasever=7
	;;
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
        CEPH_INSTALLED_VERSION = `ls -1 | grep ceph | awk 'match($0, /ceph-(.*).repo/, a) {print a[1]}'`
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
        echo "Usage: $0 install|deinstall|update [jewel|kraken|luminous]"
        exit 1
        ;;
esac
