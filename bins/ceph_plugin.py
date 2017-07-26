#!/usr/bin/python
#
# Copyright (C) Roman V. Posudnevskiy (ramzes_r@yahoo.com)
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation; version 2.1 only.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

import os
import sys
import XenAPIPlugin
sys.path.append("/opt/xensource/sm/")
import util
import os.path

def _disable_rbd_caching():
    if not os.path.isfile("/etc/ceph/ceph.conf.nocaching"):
        os.system("printf \"[client]\\n\\trbd cache = false\\n\\n\" > /etc/ceph/ceph.conf.nocaching")
        os.system("cat /etc/ceph/ceph.conf >> /etc/ceph/ceph.conf.nocaching")

def _merge(session, arg_dict):
    mode = arg_dict['mode']
    mirror_dev_name = arg_dict['mirror_dev_name']
    _mirror_dev_name = arg_dict['_mirror_dev_name']
    _mirror_dmdev_name = arg_dict['_mirror_dmdev_name']
    _mirror_dm_name = arg_dict['_mirror_dm_name']
    base_dev_name = arg_dict['base_dev_name']
    _base_dev_name = arg_dict['_base_dev_name']
    _base_dmdev_name = arg_dict['_base_dmdev_name']
    _base_dm_name = arg_dict['_base_dm_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    size = arg_dict['size']

    util.pread2(["dmsetup", "suspend", _base_dm_name])
    util.pread2(["dmsetup", "reload", _base_dm_name, "--table", "0 %s snapshot-merge %s %s P 1" % (str(int(size) / 512), _base_dev_name, _mirror_dev_name)])
    util.pread2(["dmsetup", "resume", _base_dm_name])
    # we should wait until the merge is completed
    util.pread2(["waitdmmerging.sh", _base_dm_name])

    return "merged"

def _map(session, arg_dict):
    mode = arg_dict['mode']
    dev_name = arg_dict['dev_name']
    _dev_name = arg_dict['_dev_name']
    _dmdev_name = arg_dict['_dmdev_name']
    _dm_name = arg_dict['_dm_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    sharable = arg_dict['sharable']
    size = arg_dict['size']
    dm = arg_dict['dm']

    if arg_dict.has_key("_snap_name"):
        _vdi_name = arg_dict["_snap_name"]
    else:
        _vdi_name = arg_dict['_vdi_name']
    vdi_name = arg_dict['vdi_name']

    if mode == "kernel":
        dev = util.pread2(["rbd", "map", _vdi_name, "--pool", CEPH_POOL_NAME, "--name", CEPH_USER])
    elif mode == "fuse":
        pass
    elif mode == "nbd":
        dev = "%s%s" % ("/dev/nbd", arg_dict['dev'])
        if sharable == "true":
            _disable_rbd_caching()
            util.pread2(["rbd-nbd", "--device", dev, "--nbds_max", NBDS_MAX, "-c", "/etc/ceph/ceph.conf.nocaching", "map", "%s/%s" % (CEPH_POOL_NAME, _vdi_name), "--name", CEPH_USER]).rstrip('\n')
        else:
            util.pread2(["rbd-nbd", "--device", dev, "--nbds_max", NBDS_MAX, "map", "%s/%s" % (CEPH_POOL_NAME, _vdi_name), "--name", CEPH_USER]).rstrip('\n')
        util.pread2(["ln", "-fs", dev, _dev_name])

    if dm == "linear":
        util.pread2(["dmsetup", "create", _dm_name, "--table", "0 %s linear %s 0" % (str(int(size) / 512), dev)])
        util.pread2(["ln", "-fs", _dmdev_name, dev_name])
    elif dm == "mirror":
        _dmzero_name = "%s%s" % (_dm_name, "-zero")
        _dmzerodev_name = "%s%s" % (_dmdev_name,"-zero",)
        util.pread2(["dmsetup", "create", _dmzero_name, "--table", "0 %s zero" % str(int(size) / 512)])
        util.pread2(["dmsetup", "create", _dm_name, "--table", "0 %s snapshot %s %s P 1" % (str(int(size) / 512), _dmzerodev_name, dev)])
        util.pread2(["ln", "-fs", _dmdev_name, dev_name])
    elif dm == "base":
        util.pread2(["dmsetup", "create", _dm_name, "--table", "0 %s snapshot-origin %s" % (str(int(size) / 512), dev)])
        util.pread2(["ln", "-fs", _dmdev_name, dev_name])
    else:
        util.pread2(["ln", "-fs", dev, dev_name])
    return "mapped"

def _unmap(session, arg_dict):
    mode = arg_dict['mode']
    dev_name = arg_dict['dev_name']
    _dev_name = arg_dict['_dev_name']
    _dmdev_name = arg_dict['_dmdev_name']
    _dm_name = arg_dict['_dm_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    sharable = arg_dict['sharable']
    size = arg_dict['size']
    dm = arg_dict['dm']

    if arg_dict.has_key("_snap_name"):
        _vdi_name = arg_dict["_snap_name"]
    else:
        _vdi_name = arg_dict['_vdi_name']
    vdi_name = arg_dict['vdi_name']

    dev = util.pread2(["realpath", _dev_name]).rstrip('\n')

    util.pread2(["unlink", dev_name])
    if dm == "linear":
        util.pread2(["dmsetup", "remove", _dm_name])
    elif dm == "mirror":
        _dmzero_name = "%s%s" % (_dm_name, "-zero")
        util.pread2(["dmsetup", "remove", _dm_name])
        util.pread2(["dmsetup", "remove", _dmzero_name])
    elif dm == "base":
        util.pread2(["dmsetup", "remove", _dm_name])

    if mode == "kernel":
        util.pread2(["rbd", "unmap", dev, "--name", CEPH_USER])
    elif mode == "fuse":
        pass
    elif mode == "nbd":
        util.pread2(["unlink", _dev_name])
        util.pread2(["rbd-nbd", "unmap", dev, "--name", CEPH_USER])
    return "unmapped"

def __map(session, arg_dict):
    mode = arg_dict['mode']
    _dev_name = arg_dict['_dev_name']
    _dmdev_name = arg_dict['_dmdev_name']
    _dm_name = arg_dict['_dm_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    sharable = arg_dict['sharable']
    dm = arg_dict['dm']
    _vdi_name = arg_dict['_vdi_name']

    if mode == "kernel":
        dev = util.pread2(["rbd", "map", _vdi_name, "--pool", CEPH_POOL_NAME, "--name", CEPH_USER])
    elif mode == "fuse":
        pass
    elif mode == "nbd":
        dev = "%s%s" % ("/dev/nbd", arg_dict['dev'])
        if sharable == "true":
            _disable_rbd_caching()
            dev = util.pread2(["rbd-nbd", "--device", dev, "--nbds_max", NBDS_MAX, "-c", "/etc/ceph/ceph.conf.nocaching", "map", "%s/%s" % (CEPH_POOL_NAME, _vdi_name), "--name", CEPH_USER]).rstrip('\n')
        else:
            dev = util.pread2(["rbd-nbd", "--device", dev, "--nbds_max", NBDS_MAX, "map", "%s/%s" % (CEPH_POOL_NAME, _vdi_name), "--name", CEPH_USER]).rstrip('\n')

    if dm != "none":
        util.pread2(["dmsetup", "resume", _dm_name])

    return "mapped"

def __unmap(session, arg_dict):
    mode = arg_dict['mode']
    _dev_name = arg_dict['_dev_name']
    _dmdev_name = arg_dict['_dmdev_name']
    _dm_name = arg_dict['_dm_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    sharable = arg_dict['sharable']
    dm = arg_dict['dm']
    _vdi_name = arg_dict['_vdi_name']

    dev = util.pread2(["realpath", _dev_name]).rstrip('\n')

    if dm != "none":
        util.pread2(["dmsetup", "suspend", _dm_name])

    if mode == "kernel":
        util.pread2(["rbd", "unmap", dev, "--name", CEPH_USER])
    elif mode == "fuse":
        pass
    elif mode == "nbd":
        util.pread2(["rbd-nbd", "unmap", dev, "--name", CEPH_USER])

    return "unmapped"

if __name__ == "__main__":
    XenAPIPlugin.dispatch({"map": _map,
                           "unmap": _unmap,
                           "_map": __map,
                           "_unmap": __unmap,
                           "merge": _merge})
