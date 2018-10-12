Summary: RBDSR - XenServer/XCP-ng Storage Manager plugin for CEPH
Name: RBDSR
Epoch: 3
Version: 3.0
Release: 1
License: LGPL
Group: Utilities/System
BuildArch: noarch
URL: https://github.com/rposudnevskiy/%{name}
Requires: python-rbd
Requires: rbd-nbd
Requires: qemu
Requires: qemu-dp
Requires: glibc >= 2.17-222.el7
%undefine _disable_source_fetch
Source0: https://github.com/rposudnevskiy/%{name}/archive/v%{version}.zip

%description
This package contains RBDSR - XenServer/XCP-ng Storage Manager plugin for CEPH


%prep
%autosetup


%install
rm -rf %{builddir}
rm -rf %{buildroot}
#---
install -D -m 755 -o 0 -g 0 volume/org.xen.xapi.storage.rbdsr/plugin.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/plugin.py
install -D -m 755 -o 0 -g 0 volume/org.xen.xapi.storage.rbdsr/sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/sr.py
install -D -m 755 -o 0 -g 0 volume/org.xen.xapi.storage.rbdsr/volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/volume.py
#---
install -D -m 755 -o 0 -g 0 datapath/rbd+raw+qdisk/plugin.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/plugin.py
install -D -m 755 -o 0 -g 0 datapath/rbd+raw+qdisk/datapath.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/datapath.py
install -D -m 755 -o 0 -g 0 datapath/rbd+raw+qdisk/data.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/data.py
#---
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/__init__.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/__init__.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/ceph_utils.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/ceph_utils.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/datapath.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/datapath.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/meta.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/meta.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/qemudisk.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/qemudisk.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/rbd_utils.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/rbd_utils.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/utils.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/utils.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/volume.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/volume.py
wget -O xapi/storage/libs/librbd/qmp.py https://github.com/qemu/qemu/raw/stable-2.10/scripts/qmp/qmp.py
install -D -m 755 -o 0 -g 0 xapi/storage/libs/librbd/qmp.py %{buildroot}/lib/python2.7/site-packages/xapi/storage/libs/librbd/qmp.py
#---
ln -s plugin.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Plugin.diagnostics
ln -s plugin.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Plugin.Query
#---
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.probe
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.attach
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.create
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.destroy
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.detach
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.ls
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.stat
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.set_description
ln -s sr.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/SR.set_name
#---
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.clone
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.create
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.destroy
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.resize
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set_description
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.set_name
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.snapshot
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.stat
ln -s volume.py %{buildroot}/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr/Volume.unset
#---
ln -s plugin.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Plugin.Query
#---
ln -s datapath.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.activate
ln -s datapath.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.attach
ln -s datapath.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.close
ln -s datapath.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.deactivate
ln -s datapath.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.detach
ln -s datapath.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Datapath.open
#---
ln -s data.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Data.copy
ln -s data.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Data.mirror
ln -s data.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Data.stat
ln -s data.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Data.cancel
ln -s data.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Data.destory
ln -s data.py %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk/Data.ls
#---
ln -s rbd+raw+qdisk %{buildroot}/usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk


%files
/usr/libexec/xapi-storage-script/volume/org.xen.xapi.storage.rbdsr
/usr/libexec/xapi-storage-script/datapath/rbd+raw+qdisk
/usr/libexec/xapi-storage-script/datapath/rbd+qcow2+qdisk
/lib/python2.7/site-packages/xapi/storage/libs/librbd


%changelog
* Sun Oct 07 2018 rposudnevskiy <ramzes_r@yahoo.com> - 3.0-1
- Added requirements for glibc >= 2.17-222.el7 (Issue #88)

* Sun Oct 07 2018 rposudnevskiy <ramzes_r@yahoo.com> - 3.0-1
- Added Data interface

* Tue Sep 25 2018 rposudnevskiy <ramzes_r@yahoo.com> - 3.0-1
- First packaging
