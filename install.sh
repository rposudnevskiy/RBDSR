#!/bin/bash

DEFAULT_CEPH_VERSION="luminous"

# If executed with "bash -x" or "set -x" is added in the sourcecode
# output is more verbose
export PS4='+${BASH_SOURCE}:${LINENO}:${FUNCNAME[0]-*no-function*}: '

function exec_cmd(){
   local CMD="$1"
   echo "+ $CMD"
   eval "$CMD 2>&1"
   local RET="$?"
   if [ "$RET" != "0" ];then
      echo "** ERROR: execution failed (returncode $RET)"
      exit $RET
   fi
   return 0
}

# Usage: installRepo <ceph-version>
function installRepo {
  echo "Install the release.asc key"
  exec_cmd "rpm --import 'https://download.ceph.com/keys/release.asc'"
  echo "Install new Repos"
  exec_cmd "cp -n repos/ceph-$1.repo /etc/yum.repos.d/"
}

# Usage: removeRepo <ceph-version>
function removeRepo {
  exec_cmd "rm -f /etc/yum.repos.d/ceph-$1.repo"
}

# Usage: setReposEnabled <repo filename> <section name> <0|1>
function setReposEnabled {
  echo "Set $1 $2 enabled = $3"
  exec_cmd "sed -ie \"/\[$2\]/,/^\[/s/enabled=[01]/enabled=$3/\" /etc/yum.repos.d/$1"
}

# Usage: backupFile <path>
function backupFile {
  echo "Backing Up file $1"
  if [ -e "$1-orig" ]; then
    echo "$1-orig already in place, not backing up!"
  else
    exec_cmd "mv '$1' '$1-orig'"
  fi
}

# Usage: restoreFile <path>
function restoreFile {
  echo "Restore file $1"
  if [ -e "$1-orig" ]; then
    exec_cmd "mv '$1-orig' '$1'"
  else
    echo "WARN: No $1-orig in place, not restoring!"
  fi
}

# Usage: copyFile <source path> <destination path>
function copyFile {
  exec_cmd "cp -a $1 $2"
  exec_cmd "chmod +x $2"
}

function configFirewall {
  backupFile "/etc/sysconfig/iptables"
  if ( ! (iptables -C INPUT -p tcp --dport 6789 -j ACCEPT >/dev/null 2>&1));then
     exec_cmd "iptables -A INPUT -p tcp --dport 6789 -j ACCEPT"
     exec_cmd "service iptables save"
  fi
  if ( ! (iptables -A INPUT -m multiport -p tcp --dports 6800:7300 -j ACCEPT >/dev/null 2>&1));then
     exec_cmd "iptables -A INPUT -m multiport -p tcp --dports 6800:7300 -j ACCEPT"
     exec_cmd "service iptables save"
  fi
}

function unconfigFirewall {
  echo "INFO: not restoring /etc/sysconfig/iptables-orig to /etc/sysconfig/iptables"
}

function enableRBDSR {
  echo "Add RBDSR plugin to whitelist of SM plugins in /etc/xapi.conf"
  if (grep -q -P "^sm-plugins[ ]*=[ ]*.*rbd([ ].*$|)$" /etc/xapi.conf);then
     echo "already activated"
  else
     exec_cmd "cp /etc/xapi.conf /etc/xapi.conf-orig"
     exec_cmd "sed -ie 's/sm-plugins\(.*\)/& rbd/g' /etc/xapi.conf"
  fi
  grep "sm-plugins" /etc/xapi.conf
}

function disableRBDSR {
  echo "Remove RBDSR plugin to whitelist of SM plugins in /etc/xapi.conf"
  if (grep -q -P "^sm-plugins[ ]*=[ ]*.*rbd([ ].*$|)$" /etc/xapi.conf);then
     exec_cmd "sed -ie 's/\(sm-plugins\)\(.*\)rbd\(.*\)/\1\2\3/g' /etc/xapi.conf"
  else
     echo "already deactivated"
  fi
}

function installEpel {
  echo "Install Required Packages"
  exec_cmd "yum install -y epel-release yum-plugin-priorities.noarch"
}

function installCeph {
  echo "Install RBDSR depenencies"
  exec_cmd "yum install -y snappy leveldb gdisk python-argparse gperftools-libs fuse fuse-libs"
  echo "Install Ceph"
  exec_cmd "yum install -y ceph-common rbd-fuse rbd-nbd"
}

function installFiles {
  echo "Install RBDSR Files"
  copyFile "bins/waitdmmerging.sh"      "/usr/bin/waitdmmerging.sh"
  copyFile "bins/ceph_plugin.py"        "/etc/xapi.d/plugins/ceph_plugin"
  copyFile "bins/RBDSR.py"              "/opt/xensource/sm/RBDSR.py"
  copyFile "bins/rbdsr_vhd.py"          "/opt/xensource/sm/rbdsr_vhd.py"
  copyFile "bins/rbdsr_dmp.py"          "/opt/xensource/sm/rbdsr_dmp.py"
  copyFile "bins/rbdsr_rbd.py"          "/opt/xensource/sm/rbdsr_rbd.py"
  copyFile "bins/rbdsr_common.py"       "/opt/xensource/sm/rbdsr_common.py"
  copyFile "bins/rbdsr_lock.py"         "/opt/xensource/sm/rbdsr_lock.py"

  copyFile "bins/cleanup.py"            "/opt/xensource/sm/cleanup.py"
  copyFile "bins/tap-ctl"               "/sbin/tap-ctl"
  copyFile "bins/vhd-tool"              "/bin/vhd-tool"
  copyFile "bins/sparse_dd"             "/usr/libexec/xapi/sparse_dd"

  copyFile "bins/rbd2vhd.py"            "/bin/rbd2vhd"

  exec_cmd "ln -nf '/bin/rbd2vhd' '/bin/vhd2rbd'"
  exec_cmd "ln -nf '/bin/rbd2vhd' '/bin/rbd2raw'"
  exec_cmd "ln -nf '/bin/rbd2vhd' '/bin/rbd2nbd'"
  exec_cmd "ln -snf '/opt/xensource/sm/RBDSR.py' '/opt/xensource/sm/RBDSR'"
}

function removeFiles {
  echo "Removing RBDSR Files"
  exec_cmd "rm -f '/usr/bin/waitdmmerging.sh'"
  exec_cmd "rm -f '/etc/xapi.d/plugins/ceph_plugin'"
  exec_cmd "rm -f '/opt/xensource/sm/RBDSR'"
  exec_cmd "rm -f '/opt/xensource/sm/rbdsr_vhd.py'"
  exec_cmd "rm -f '/opt/xensource/sm/rbdsr_dmp.py'"
  exec_cmd "rm -f '/opt/xensource/sm/rbdsr_rbd.py'"
  exec_cmd "rm -f '/opt/xensource/sm/rbdsr_common.py'"
  exec_cmd "rm -f '/opt/xensource/sm/rbdsr_lock.py'"

  exec_cmd "rm -f '/opt/xensource/sm/cleanup.py'"
  exec_cmd "rm -f '/sbin/tap-ctl'"
  exec_cmd "rm -f '/bin/vhd-tool'"
  exec_cmd "rm -f '/usr/libexec/xapi/sparse_dd'"

  exec_cmd "rm -f '/bin/rbd2vhd'"

  exec_cmd "rm -f '/bin/vhd2rbd'"
  exec_cmd "rm -f '/bin/rbd2raw'"
  exec_cmd "rm -f '/bin/rbd2nbd'"
}


function install {
  installRepo "$1"
  setReposEnabled "CentOS-Base.repo" "base" 1
  setReposEnabled "CentOS-Base.repo" "extras" 1
  installEpel
  setReposEnabled "epel.repo" "epel" 1
  installCeph "$1"
  setReposEnabled "CentOS-Base.repo" "base" 0
  setReposEnabled "CentOS-Base.repo" "extras" 0
  setReposEnabled "epel.repo" "epel" 0

  backupFile "/sbin/tap-ctl"
  backupFile "/bin/vhd-tool"
  backupFile "/usr/libexec/xapi/sparse_dd"
  backupFile "/opt/xensource/sm/cleanup.py"

  installFiles

  configFirewall
  enableRBDSR
}

function deinstall {
  disableRBDSR
  removeFiles
  restoreFile "/sbin/tap-ctl"
  restoreFile "/bin/vhd-tool"
  restoreFile "/usr/libexec/xapi/sparse_dd"
  restoreFile "/opt/xensource/sm/cleanup.py"
  removeRepo $1
  unconfigFirewall
}

case $1 in
    install)
        if [ -z "$2" ]; then
            install "$DEFAULT_CEPH_VERSION"
        else
           if (echo "$2"|egrep -q "^jewel$|^kraken$|^luminous$"); then
                echo "[ERROR]: Unsupported Ceph version specified '$2'"
                exit 1
            else
                install "$2"
            fi
        fi
        ;;

    installFiles)
        installFiles
        ;;

    deinstall)
        CEPH_INSTALLED_VERSION="$(ls /etc/yum.repos.d/ | grep ceph | awk 'match($0, /ceph-(.*).repo/, a) {print a[1]}')"
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
