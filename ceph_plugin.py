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

def _map(session, arg_dict):
    mode = arg_dict['mode']
    dev_name = arg_dict['dev_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    
    if arg_dict.has_key("snap_name"):
        vdi_name = arg_dict["snap_name"]
    else:
        vdi_name = arg_dict['vdi_name']
        
    if mode == "kernel":
        util.pread2(["rbd", "map", vdi_name, "--pool", CEPH_POOL_NAME, "--name", CEPH_USER])
    elif mode == "fuse":
        pass
    elif mode == "nbd":
        cmdout = util.pread2(["rbd-nbd", "--nbds_max", NBDS_MAX, "map", "%s/%s" % (CEPH_POOL_NAME, vdi_name), "--name", CEPH_USER]).rstrip('\n')
        util.pread2(["ln", "-s", cmdout, dev_name])
    return "mapped"

def _unmap(session, arg_dict):
    mode = arg_dict['mode']
    dev_name = arg_dict['dev_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    
    if arg_dict.has_key("snap_name"):
        vdi_name = arg_dict["snap_name"]
    else:
        vdi_name = arg_dict['vdi_name']
    
    if mode == "kernel":
        util.pread2(["rbd", "unmap", dev_name, "--name", CEPH_USER])
    elif mode == "fuse":
        pass
    elif mode == "nbd":
        nbddev = util.pread2(["realpath", dev_name]).rstrip('\n')
        util.pread2(["unlink", dev_name])
        util.pread2(["rbd-nbd", "unmap", nbddev, "--name", CEPH_USER])
    return "unmapped"

if __name__ == "__main__":
    XenAPIPlugin.dispatch({"map": _map,
                           "unmap": _unmap})
