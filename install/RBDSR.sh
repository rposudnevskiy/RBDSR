#!/bin/bash
DEFAULT_CEPH_VERSION="nautilus"

# Usage: installCephRepo <ceph-version>
function installCephRepo {
    echo "Install Ceph Repo"
    yum install --enablerepo="extras,base" -q -y centos-release-ceph-$1.noarch
    echo "centos" > /etc/yum/vars/contentdir
}

# Usage: removeCephRepo <ceph-version>
function removeCephRepo {
    yum erase -y centos-release-ceph-$1.noarch
}

# Usage: removeXCPngRepo
#function removeXCPngRepo {
#    rm -f /etc/yum.repos.d/xcp-ng.repo
#}

# Usage: installXCPngRepo
#function installXCPngRepo {
#    major_version=`cat /etc/centos-release | awk '{print $3}' | awk -F. '{print $1}'`
#    major_minor_version=`cat /etc/centos-release | awk '{print $3}' | awk -F. '{print $1"."$2}'`
#    cat << EOF >/etc/yum.repos.d/xcp-ng.repo
#[xcp-ng-extras_testing]
#name=XCP-ng Extras Testing Repository
#baseurl=https://updates.xcp-ng.org/${major_version}/${major_minor_version}/extras_testing/x86_64/
#enabled=0
#gpgcheck=0
#EOF
#}

# Usage: confirmInstallation
function confirmInstallation {
    echo "This script is going to install 'RBDSR' storage plugin and its dependencies"
    echo "which include 'xcp-ng-extras_testing' repository, upgrade 'glibc' and 'qemu-dp' packages."
    echo "Please note that Ceph support is experimental and can lead to an unstable system and data loss"
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
    yum install --enablerepo="extras,base" -q -y python-rbd rbd-nbd
}

function uninstallCeph {
    echo "Uninstall Ceph API"
    yum erase -q -y python-rbd rbd-nbd
}

function upgradeDeps {
    yum install --enablerepo="extras,base" -q -y glibc-2.17-222.el7
}

function downgradeDeps {
    yum history undo -q -y `yum history packages-list glibc | head -4 | tail -1 | awk -F\| '{gsub(/ /, "", $0); print $1}'`
}

function installFiles {
    echo "Install RBDSR Files"
    rm -rf /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr
    mkdir -p /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr

    copyFile "src/volume/org.xen.xapi.storage.rbdsr/plugin.json" "/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/plugin.json"

    ln -s /lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/plugin.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/plugin.py
    ln -s /lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/sr.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/sr.py
    ln -s /lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/volume.py /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/volume.py

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

    rm -rf /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk
    mkdir -p /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk

    copyFile "src/datapath/rbd+qcow2+qdisk/plugin.json" "/usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/plugin.json"

    ln -s /lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/plugin.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/plugin.py
    ln -s /lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/datapath.py
    ln -s /lib/python2.7/site-packages/xapi/storage/libs/xcpng/scripts/data.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/data.py

    ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/Datapath.activate
    ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/Datapath.attach
    ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/Datapath.close
    ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/Datapath.deactivate
    ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/Datapath.detach
    ln -s datapath.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/Datapath.open
    ln -s plugin.py /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk/Plugin.Query

    rm -rf /lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd
    mkdir /lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd

    copyFile "src/xapi/storage/libs/xcpng/librbd/__init__.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd/__init__.py"
    copyFile "src/xapi/storage/libs/xcpng/librbd/datapath.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd/datapath.py"
    copyFile "src/xapi/storage/libs/xcpng/librbd/locks.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd/locks.py"
    copyFile "src/xapi/storage/libs/xcpng/librbd/meta.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd/meta.py"
    copyFile "src/xapi/storage/libs/xcpng/librbd/rbd_utils.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd/rbd_utils.py"
    copyFile "src/xapi/storage/libs/xcpng/librbd/sr.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd/sr.py"
    copyFile "src/xapi/storage/libs/xcpng/librbd/volume.py" "/lib/python2.7/site-packages/xapi/storage/libs/xcpng/librbd/volume.py"
}

function removeFiles {
  echo "Removing RBDSR Files"
  rm -rf /usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr
  rm -rf /usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk
  rm -rf /lib/python2.7/site-packages/xapi/storage/libs/librbd
  }

function install {
  installCephRepo $1
  installCeph
  installFiles
  upgradeDeps
  configureFirewall
}

function deinstall {
  unconfigureFirewall
  downgradeDeps
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
