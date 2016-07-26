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

import xml.dom.minidom
import util
import re
import json
import sys
import os

RBDPOOL_PREFIX = "RBD_XenStorage-"
SR_MOUNT_PREFIX ="/run/sr-mount"
VDI_PREFIX = "VHD-"
SXM_PREFIX = "SXM-"
SNAPSHOT_PREFIX = "SNAP-"
CLONE_PREFIX = "VHD-"
DEV_RBD_ROOT = "/dev/rbd"
DEV_NBD_ROOT = "/dev/nbd"
BLOCK_SIZE = 21 #2097152 bytes
NBDS_MAX = 64

def fuse_pool_mount(sr_uuid):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    POOL_MOUNT_PATH = "%s/%s" % (SR_MOUNT_PREFIX, sr_uuid)
    cmd = ["mkdir", "-p", POOL_MOUNT_PATH]
    cmdout = util.pread2(cmd)
    cmd = ["rbd-fuse", "-p", POOL_NAME, POOL_MOUNT_PATH]
    cmdout = util.pread2(cmd)

def fuse_pool_umount(sr_uuid):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    POOL_MOUNT_PATH = "%s/%s" % (SR_MOUNT_PREFIX, sr_uuid)
    cmd = ["fusermount", "-u", POOL_MOUNT_PATH]
    cmdout = util.pread2(cmd)
    cmd = ["rm", "-rf", POOL_MOUNT_PATH]
    cmdout = util.pread2(cmd)

def nbd_pool_mount(sr_uuid):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    POOL_MOUNT_PATH = "%s/%s" % (DEV_NBD_ROOT, POOL_NAME)
    cmd = ["mkdir", "-p", POOL_MOUNT_PATH]
    cmdout = util.pread2(cmd)

def nbd_pool_umount(sr_uuid):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    POOL_MOUNT_PATH = "%s/%s" % (DEV_NBD_ROOT, POOL_NAME)
    cmd = ["rm", "-rf", POOL_MOUNT_PATH]
    cmdout = util.pread2(cmd)

def dm_setup_mirror_kernel(sr_uuid, vdi_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    RBD_VDI_MIRROR_DEVNAME = "/dev/rbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    RBD_SXM_MIRROR_DEVNAME = "/dev/rbd/%s/%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_ZERO_NAME = "%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_ZERO_DEVNAME = "/dev/mapper/%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_MIRROR_NAME = "%s-%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_MIRROR_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    change_image_prefix_sxm(sr_uuid, vdi_uuid)
    map_sxm_blkdev(sr_uuid, vdi_uuid)
    cmd = ["dmsetup", "create", DM_ZERO_NAME, "--table", "0 %s zero" % str(int(size)/512)]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "create", DM_MIRROR_NAME, "--table", "0 %s snapshot %s %s P 1" % (str(int(size)/512), DM_ZERO_DEVNAME, RBD_SXM_MIRROR_DEVNAME)]
    cmdout = util.pread2(cmd)
    cmd = ["ln", "-s", DM_MIRROR_DEVNAME, RBD_VDI_MIRROR_DEVNAME]
    cmdout = util.pread2(cmd)
    
def dm_setup_mirror_nbd(sr_uuid, vdi_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    RBD_VDI_MIRROR_DEVNAME = "/dev/nbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    RBD_SXM_MIRROR_DEVNAME = "/dev/nbd/%s/%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_ZERO_NAME = "%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_ZERO_DEVNAME = "/dev/mapper/%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_MIRROR_NAME = "%s-%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_MIRROR_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    change_image_prefix_sxm(sr_uuid, vdi_uuid)
    map_sxm_nbddev(sr_uuid, vdi_uuid)
    cmd = ["dmsetup", "create", DM_ZERO_NAME, "--table", "0 %s zero" % str(int(size)/512)]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "create", DM_MIRROR_NAME, "--table", "0 %s snapshot %s %s P 1" % (str(int(size)/512), DM_ZERO_DEVNAME, RBD_SXM_MIRROR_DEVNAME)]
    cmdout = util.pread2(cmd)
    cmd = ["ln", "-s", DM_MIRROR_DEVNAME, RBD_VDI_MIRROR_DEVNAME]
    cmdout = util.pread2(cmd)

def dm_setup_mirror_fuse(sr_uuid, vdi_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    POOL_MOUNT_PATH = "%s/%s" % (SR_MOUNT_PREFIX, sr_uuid)
    RBD_VDI_MIRROR_DEVNAME = "%s/%s%s" % (POOL_MOUNT_PATH, VDI_PREFIX, vdi_uuid)
    RBD_SXM_MIRROR_DEVNAME = "%s/%s%s" % (POOL_MOUNT_PATH, SXM_PREFIX, vdi_uuid)
    DM_ZERO_NAME = "%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_ZERO_DEVNAME = "/dev/mapper/%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_MIRROR_NAME = "%s-%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    DM_MIRROR_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    change_image_prefix_sxm(sr_uuid, vdi_uuid)
    #map_sxm_blkdev(sr_uuid, vdi_uuid)
    cmd = ["dmsetup", "create", DM_ZERO_NAME, "--table", "0 %s zero" % str(int(size)/512)]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "create", DM_MIRROR_NAME, "--table", "0 %s snapshot %s %s P 1" % (str(int(size)/512), DM_ZERO_DEVNAME, RBD_SXM_MIRROR_DEVNAME)]
    cmdout = util.pread2(cmd)
    cmd = ["ln", "-s", DM_MIRROR_DEVNAME, RBD_VDI_MIRROR_DEVNAME]
    cmdout = util.pread2(cmd)

def dm_setup_base_kernel(sr_uuid, vdi_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    RBD_VDI_BASE_DEVNAME = "/dev/rbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    DM_BASE_NAME = "%s-%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    DM_BASE_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    cmd = ["dmsetup", "create", DM_BASE_NAME, "--table", "0 %s snapshot-origin %s" % (str(int(size)/512), RBD_VDI_BASE_DEVNAME)]
    cmdout = util.pread2(cmd)

def dm_setup_base_nbd(sr_uuid, vdi_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    RBD_VDI_BASE_DEVNAME = "/dev/nbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    DM_BASE_NAME = "%s-%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    DM_BASE_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    cmd = ["dmsetup", "create", DM_BASE_NAME, "--table", "0 %s snapshot-origin %s" % (str(int(size)/512), RBD_VDI_BASE_DEVNAME)]
    cmdout = util.pread2(cmd)

def dm_setup_base_fuse(sr_uuid, vdi_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    POOL_MOUNT_PATH = "%s/%s" % (SR_MOUNT_PREFIX, sr_uuid)
    RBD_VDI_BASE_DEVNAME = "%s/%s%s" % (POOL_MOUNT_PATH, VDI_PREFIX, vdi_uuid)
    DM_BASE_NAME = "%s-%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    DM_BASE_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    cmd = ["dmsetup", "create", DM_BASE_NAME, "--table", "0 %s snapshot-origin %s" % (str(int(size)/512), RBD_VDI_BASE_DEVNAME)]
    cmdout = util.pread2(cmd)

def merge_diff_kernel(sr_uuid, mirror_uuid, snap_uuid, base_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    DM_ZERO_NAME = "%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    RBD_VDI_BASE_DEVNAME = "/dev/rbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    RBD_VDI_MIRROR_DEVNAME = "/dev/rbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, mirror_uuid)
    RBD_SXM_MIRROR_DEVNAME = "/dev/rbd/%s/%s%s" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    DM_BASE_NAME = "%s-%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    DM_BASE_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    DM_MIRROR_NAME = "%s-%s%s" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    DM_MIRROR_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    RBD_BASE_DEVNAME = "/dev/rbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    cmd = ["unlink", RBD_VDI_MIRROR_DEVNAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_MIRROR_NAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_ZERO_NAME]
    cmdout = util.pread2(cmd)
    map_rbd_blkdev(sr_uuid, base_uuid)
    dm_setup_base_kernel(sr_uuid, base_uuid, size)
    cmd = ["dmsetup", "suspend", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "reload", DM_BASE_NAME, "--table", "0 %s snapshot-merge %s %s P 1" % (str(int(size)/512), RBD_VDI_BASE_DEVNAME, RBD_SXM_MIRROR_DEVNAME)]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "resume", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    # we should wait until the merge is completed
    cmd = ["waitdmmerging.sh", DM_BASE_NAME]
    # -------------------------------------------
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    unmap_rbd_blkdev(sr_uuid, base_uuid)
    unmap_sxm_blkdev(sr_uuid, mirror_uuid)
    revert_image_prefix_sxm(sr_uuid, mirror_uuid)
    #-----
    tmp_uuid = "temporary" #util.gen_uuid()
    rename_image(sr_uuid, mirror_uuid, tmp_uuid)
    rename_image(sr_uuid, base_uuid, mirror_uuid)
    rename_image(sr_uuid, tmp_uuid, base_uuid)
    #-----
    map_rbd_blkdev(sr_uuid, mirror_uuid)

def merge_diff_nbd(sr_uuid, mirror_uuid, snap_uuid, base_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    DM_ZERO_NAME = "%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    RBD_VDI_BASE_DEVNAME = "/dev/nbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    RBD_VDI_MIRROR_DEVNAME = "/dev/nbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, mirror_uuid)
    RBD_SXM_MIRROR_DEVNAME = "/dev/nbd/%s/%s%s" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    DM_BASE_NAME = "%s-%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    DM_BASE_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    DM_MIRROR_NAME = "%s-%s%s" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    DM_MIRROR_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    RBD_BASE_DEVNAME = "/dev/nbd/%s/%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    cmd = ["unlink", RBD_VDI_MIRROR_DEVNAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_MIRROR_NAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_ZERO_NAME]
    cmdout = util.pread2(cmd)
    map_rbd_nbddev(sr_uuid, base_uuid)
    dm_setup_base_kernel(sr_uuid, base_uuid, size)
    cmd = ["dmsetup", "suspend", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "reload", DM_BASE_NAME, "--table", "0 %s snapshot-merge %s %s P 1" % (str(int(size)/512), RBD_VDI_BASE_DEVNAME, RBD_SXM_MIRROR_DEVNAME)]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "resume", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    # we should wait until the merge is completed
    cmd = ["waitdmmerging.sh", DM_BASE_NAME]
    # -------------------------------------------
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    unmap_rbd_nbddev(sr_uuid, base_uuid)
    unmap_sxm_nbddev(sr_uuid, mirror_uuid)
    revert_image_prefix_sxm(sr_uuid, mirror_uuid)
    #-----
    tmp_uuid = "temporary" #util.gen_uuid()
    rename_image(sr_uuid, mirror_uuid, tmp_uuid)
    rename_image(sr_uuid, base_uuid, mirror_uuid)
    rename_image(sr_uuid, tmp_uuid, base_uuid)
    #-----
    map_rbd_nbddev(sr_uuid, mirror_uuid)

def merge_diff_fuse(sr_uuid, mirror_uuid, snap_uuid, base_uuid, size):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    POOL_MOUNT_PATH = "%s/%s" % (SR_MOUNT_PREFIX, sr_uuid)
    DM_ZERO_NAME = "%s-%s%s-zero" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    RBD_VDI_BASE_DEVNAME = "%s/%s%s" % (POOL_MOUNT_PATH, VDI_PREFIX, base_uuid)
    RBD_VDI_MIRROR_DEVNAME = "%s/%s%s" % (POOL_MOUNT_PATH, VDI_PREFIX, mirror_uuid)
    RBD_SXM_MIRROR_DEVNAME = "%s/%s%s" % (POOL_MOUNT_PATH, SXM_PREFIX, mirror_uuid)
    DM_BASE_NAME = "%s-%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    DM_BASE_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, VDI_PREFIX, base_uuid)
    DM_MIRROR_NAME = "%s-%s%s" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    DM_MIRROR_DEVNAME = "/dev/mapper/%s-%s%s" % (POOL_NAME, SXM_PREFIX, mirror_uuid)
    RBD_BASE_DEVNAME = "%s/%s%s" % (POOL_MOUNT_PATH, VDI_PREFIX, base_uuid)
    cmd = ["unlink", RBD_VDI_MIRROR_DEVNAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_MIRROR_NAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_ZERO_NAME]
    cmdout = util.pread2(cmd)
    #map_rbd_blkdev(sr_uuid, base_uuid)
    dm_setup_base_fuse(sr_uuid, base_uuid, size)
    cmd = ["dmsetup", "suspend", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "reload", DM_BASE_NAME, "--table", "0 %s snapshot-merge %s %s P 1" % (str(int(size)/512), RBD_VDI_BASE_DEVNAME, RBD_SXM_MIRROR_DEVNAME)]
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "resume", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    # we should wait until the merge is completed
    cmd = ["waitdmmerging.sh", DM_BASE_NAME]
    # -------------------------------------------
    cmdout = util.pread2(cmd)
    cmd = ["dmsetup", "remove", DM_BASE_NAME]
    cmdout = util.pread2(cmd)
    #unmap_rbd_blkdev(sr_uuid, base_uuid)
    #unmap_sxm_blkdev(sr_uuid, mirror_uuid)
    revert_image_prefix_sxm(sr_uuid, mirror_uuid)
    #-----
    tmp_uuid = "temporary" #util.gen_uuid()
    rename_image(sr_uuid, mirror_uuid, tmp_uuid)
    rename_image(sr_uuid, base_uuid, mirror_uuid)
    rename_image(sr_uuid, tmp_uuid, base_uuid)
    #-----
    #map_rbd_blkdev(sr_uuid, mirror_uuid)

def change_image_prefix_sxm(sr_uuid, vdi_uuid):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    ORIG_NAME = "%s/%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    NEW_NAME = "%s/%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    cmd = ["rbd", "mv", ORIG_NAME, NEW_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def revert_image_prefix_sxm(sr_uuid, vdi_uuid):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    ORIG_NAME = "%s/%s%s" % (POOL_NAME, SXM_PREFIX, vdi_uuid)
    NEW_NAME = "%s/%s%s" % (POOL_NAME, VDI_PREFIX, vdi_uuid)
    cmd = ["rbd", "mv", ORIG_NAME, NEW_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def rename_image(sr_uuid, orig_uuid, new_uuid):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    ORIG_NAME = "%s/%s%s" % (POOL_NAME, VDI_PREFIX, orig_uuid)
    NEW_NAME = "%s/%s%s" % (POOL_NAME, VDI_PREFIX, new_uuid)
    cmd = ["rbd", "mv", ORIG_NAME, NEW_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def flatten_clone(sr_uuid, clone_uuid):
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    CLONE_NAME = "%s/%s%s" % (POOL_NAME, CLONE_PREFIX, clone_uuid)
    cmd = ["rbd", "flatten", CLONE_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def do_clone(sr_uuid, vdi_uuid, snap_uuid, clone_uuid, vdi_label):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    SNAPSHOT_NAME = "%s/%s@%s%s" % (POOL_NAME, VDI_NAME, SNAPSHOT_PREFIX, snap_uuid)
    CLONE_NAME = "%s/%s%s" % (POOL_NAME, CLONE_PREFIX, clone_uuid)
    cmd = ["rbd", "clone", SNAPSHOT_NAME, CLONE_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    cmd = ["rbd", "image-meta", "set", CLONE_NAME, "VDI_LABEL", vdi_label, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    cmd = ["rbd", "image-meta", "set", CLONE_NAME, "CLONE_OF", snap_uuid, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def rollback_snapshot(sr_uuid, base_uuid, snap_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, base_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    SNAPSHOT_NAME = "%s@%s%s" % (VDI_NAME, SNAPSHOT_PREFIX, snap_uuid)
    cmd = ["rbd", "snap", "rollback", SNAPSHOT_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def do_snapshot(sr_uuid, vdi_uuid, snap_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    SNAPSHOT_NAME = "%s@%s%s" % (VDI_NAME, SNAPSHOT_PREFIX, snap_uuid)
    cmd = ["rbd", "snap", "create", SNAPSHOT_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    cmd = ["rbd", "snap", "protect", SNAPSHOT_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def delete_snapshot(sr_uuid, vdi_uuid, snap_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    SNAPSHOT_NAME = "%s@%s%s" % (VDI_NAME, SNAPSHOT_PREFIX, snap_uuid)
    SHORT_SNAP_NAME = "%s%s" % (SNAPSHOT_PREFIX, snap_uuid)
    cmd = ["rbd", "snap", "unprotect", SNAPSHOT_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    cmd = ["rbd", "snap", "rm", SNAPSHOT_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    cmd = ["rbd", "image-meta", "remove", VDI_NAME, SHORT_SNAP_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def resize(sr_uuid, vdi_uuid, size):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    image_size = size/1024/1024
    NEW_SIZE = "%s" % (image_size)
    cmd = ["rbd", "resize", "--size", NEW_SIZE, "--allow-shrink", VDI_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def get_snap_path_kernel(sr_uuid, vdi_uuid, snap_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    SNAPSHOT_NAME = "%s@%s%s" % (VDI_NAME, SNAPSHOT_PREFIX, snap_uuid)
    blkdev_path = os.path.join(DEV_RBD_ROOT, POOL_NAME, SNAPSHOT_NAME)
    return blkdev_path

def get_snap_path_nbd(sr_uuid, vdi_uuid, snap_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    SNAPSHOT_NAME = "%s@%s%s" % (VDI_NAME, SNAPSHOT_PREFIX, snap_uuid)
    nbddev_path = os.path.join(DEV_NBD_ROOT, POOL_NAME, SNAPSHOT_NAME)
    return nbddev_path

def get_snap_path_fuse(sr_uuid, vdi_uuid, snap_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    SNAPSHOT_NAME = "%s@%s%s" % (VDI_NAME, SNAPSHOT_PREFIX, snap_uuid)
    fuse_path = os.path.join(SR_MOUNT_PREFIX, sr_uuid, SNAPSHOT_NAME)
    return fuse_path

def get_clone_path_kernel(sr_uuid, vdi_uuid):
    CLONE_NAME = "%s%s" % (CLONE_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    blkdev_path = os.path.join(DEV_RBD_ROOT, POOL_NAME, CLONE_NAME)
    return blkdev_path

def get_clone_path_nbd(sr_uuid, vdi_uuid):
    CLONE_NAME = "%s%s" % (CLONE_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    nbddev_path = os.path.join(DEV_NBD_ROOT, POOL_NAME, CLONE_NAME)
    return nbddev_path

def get_clone_path_fuse(sr_uuid, vdi_uuid):
    CLONE_NAME = "%s%s" % (CLONE_PREFIX, vdi_uuid)
    fuse_path = os.path.join(SR_MOUNT_PREFIX, sr_uuid, CLONE_NAME)
    return fuse_path

def get_sxm_blkdev_path(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (SXM_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    blkdev_path = os.path.join(DEV_RBD_ROOT, POOL_NAME, VDI_NAME)
    return blkdev_path

def get_sxm_nbddev_path(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (SXM_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    nbddev_path = os.path.join(DEV_NBD_ROOT, POOL_NAME, VDI_NAME)
    return nbddev_path

def get_sxm_fuse_path(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (SXM_PREFIX, vdi_uuid)
    fuse_path = os.path.join(SR_MOUNT_PREFIX, sr_uuid, VDI_NAME)
    return fuse_path

def map_sxm_blkdev(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (SXM_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    cmd = ["rbd", "map", VDI_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    
def map_sxm_nbddev(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (SXM_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    NBD_DEVNAME = "/dev/nbd/%s/%s" % (POOL_NAME, VDI_NAME)
    cmd = ["rbd-nbd", "--nbds_max", NBDS_MAX, "map", "%s/%s" % (POOL_NAME,VDI_NAME)]
    cmdout = util.pread2(cmd)
    cmd = ["ln", "-s", cmdout, NBD_DEVNAME]
    cmdout = util.pread2(cmd)

def unmap_sxm_blkdev(sr_uuid, vdi_uuid):
    dev_path=get_sxm_blkdev_path(sr_uuid, vdi_uuid)
    cmd = ["rbd", "unmap", dev_path, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    
def unmap_sxm_nbddev(sr_uuid, vdi_uuid):
    dev_path=get_sxm_nbddev_path(sr_uuid, vdi_uuid)
    cmd = ["ls", "-l", dev_path, "|", "awk", "-F\" -> \"", "{'print $2'}"]
    nbddev = util.pread2(cmd)
    cmd = ["unlink", dev_path]
    cmdout = util.pread2(cmd)
    cmd = ["rbd-nbd", "unmap", nbddev]
    cmdout = util.pread2(cmd)    

def get_blkdev_path(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    blkdev_path = os.path.join(DEV_RBD_ROOT, POOL_NAME, VDI_NAME)
    return blkdev_path

def get_nbddev_path(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    nbddev_path = os.path.join(DEV_NBD_ROOT, POOL_NAME, VDI_NAME)
    return nbddev_path

def get_fuse_path(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    fuse_path = os.path.join(SR_MOUNT_PREFIX, sr_uuid, VDI_NAME)
    return fuse_path

def map_rbd_blkdev(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    cmd = ["rbd", "map", VDI_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def map_nbd_blkdev(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    FULL_VDI_NAME = "%s/%s" % (POOL_NAME,VDI_NAME)
    NBD_DEVNAME = "/dev/nbd/%s/%s" % (POOL_NAME, VDI_NAME)
    cmd = ["echo", FULL_VDI_NAME, ">", "/tmp/test.out"]
    cmdout = util.pread2(cmd)
    cmd = ["rbd-nbd", "--nbds_max", str(NBDS_MAX), "map", FULL_VDI_NAME]
    cmdout = util.pread2(cmd).rstrip('\n')
    cmd = ["ln", "-s", cmdout, NBD_DEVNAME]
    cmdout = util.pread2(cmd)

def unmap_rbd_blkdev(sr_uuid, vdi_uuid):
    dev_path=get_blkdev_path(sr_uuid, vdi_uuid)
    cmd = ["rbd", "unmap", dev_path, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    
def unmap_nbd_blkdev(sr_uuid, vdi_uuid):
    dev_path=get_nbddev_path(sr_uuid, vdi_uuid)
    cmd = ["realpath", dev_path]
    nbddev = util.pread2(cmd).rstrip('\n')
    cmd = ["unlink", dev_path]
    cmdout = util.pread2(cmd)
    cmd = ["rbd-nbd", "unmap", nbddev]
    cmdout = util.pread2(cmd)     

def check_uuid(uuid):
    RBDPOOLs = {}
    cmd = ["ceph", "osd", "pool", "ls", "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)[:-1]
    if len(cmdout) == 0:
        return []
    else:
        pools = cmdout.split('\n')
    
    for pool in sorted(_filtered(pools)):
        sr_uuid = _get_sr_uuid(pool)
        if len(sr_uuid):
            if uuid == sr_uuid:
                return True
    return False

def _get_pool_info(pool,info):
    cmd = ["ceph", "df", "-f", "json", "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    decoded = json.loads(cmdout)
    for poolinfo in decoded['pools']:
        regex = re.compile(pool)
        if regex.search(poolinfo['name']):
            if info == 'size':
                return poolinfo['stats']['max_avail']
            elif info == 'used':
                return poolinfo['stats']['bytes_used']
            elif info == 'objects':
                return poolinfo['stats']['objects']

def _filtered(pools):
    filtered = []
    regex = re.compile(RBDPOOL_PREFIX)
    for pool in sorted(pools):
        if regex.search(pool):
            filtered.append(pool)
    return filtered

def _get_sr_uuid(pool):
    regex = re.compile(RBDPOOL_PREFIX)
    return regex.sub('',pool)

def _get_vdi_uuid(vdi):
    regex = re.compile(VDI_PREFIX)
    return regex.sub('',vdi)

def _get_snap_uuid(vdi):
    regex = re.compile(SNAPSHOT_PREFIX)
    return regex.sub('',vdi)

def get_allocated_size(RBDVDIs):
    allocated_bytes=0
    for vdi_uuid in RBDVDIs.keys():
        allocated_bytes += RBDVDIs[vdi_uuid]['size']
    return allocated_bytes

def scan_vdilist(pool):
    RBDVDIs = {}
    cmd = ["rbd", "ls", "-l", "--format", "json", "--pool", pool, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    decoded = json.loads(cmdout)
    for vdi in decoded:
        if vdi['image'].find("SXM") == -1 :
            if vdi.has_key('snapshot'):
                snap_uuid = _get_snap_uuid(vdi['snapshot'])
                RBDVDIs[snap_uuid]=vdi
            else:
                vdi_uuid = _get_vdi_uuid(vdi['image'])
                RBDVDIs[vdi_uuid]=vdi
    return RBDVDIs

def scan_srlist():
    RBDPOOLs = {}
    cmd = ["ceph", "osd", "pool", "ls", "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)[:-1]
    if len(cmdout) == 0:
        return []
    else:
        
        pools = cmdout.split('\n')
    
    for pool in sorted(_filtered(pools)):
        sr_uuid = _get_sr_uuid(pool)
        if len(sr_uuid):
            if RBDPOOLs.has_key(sr_uuid):
                RBDPOOLs[sr_uuid] += ",%s" % pool
            else:
                RBDPOOLs[sr_uuid] = pool
    return RBDPOOLs

def srlist_toxml(RBDPOOLs):
    dom = xml.dom.minidom.Document()
    element = dom.createElement("SRlist")
    dom.appendChild(element)
    
    for val in RBDPOOLs:
        entry = dom.createElement('SR')
        element.appendChild(entry)
        
        subentry = dom.createElement("UUID")
        entry.appendChild(subentry)
        textnode = dom.createTextNode(val)
        subentry.appendChild(textnode)
        
        subentry = dom.createElement("PoolName")
        entry.appendChild(subentry)
        textnode = dom.createTextNode(RBDPOOLs[val])
        subentry.appendChild(textnode)
        
        subentry = dom.createElement("Size")
        entry.appendChild(subentry)
        size = str(_get_pool_info(val,'size'))
        textnode = dom.createTextNode(size)
        subentry.appendChild(textnode)
        
        subentry = dom.createElement("BytesUses")
        entry.appendChild(subentry)
        bytesused = str(_get_pool_info(val,'used'))
        textnode = dom.createTextNode(bytesused)
        subentry.appendChild(textnode)
        
        subentry = dom.createElement("Objects")
        entry.appendChild(subentry)
        objects = str(_get_pool_info(val,'objects'))
        textnode = dom.createTextNode(objects)
        subentry.appendChild(textnode)
    
    return dom.toprettyxml()

def update_vdi(sr_uuid, vdi_uuid, vdi_label, vdi_description, snapshots):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    if vdi_label:
        cmd = ["rbd", "image-meta", "set", VDI_NAME, "VDI_LABEL", vdi_label, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
        cmdout = util.pread2(cmd)
    if vdi_description:
        cmd = ["rbd", "image-meta", "set", VDI_NAME, "VDI_DESCRIPTION", vdi_description, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
        cmdout = util.pread2(cmd)
    for snapshot_uuid in snapshots.keys():
        SNAPSHOT_NAME = "%s%s" % (SNAPSHOT_PREFIX, snapshot_uuid)
        cmd = ["rbd", "image-meta", "set", VDI_NAME, SNAPSHOT_NAME, str(snapshots[snapshot_uuid]), "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
        cmdout = util.pread2(cmd)

def create_vdi(sr_uuid, vdi_uuid, vdi_label, vdi_description, size):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    image_size = size/1024/1024
    cmd = ["rbd", "create", VDI_NAME, "--size", str(image_size), "--order", str(BLOCK_SIZE), "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    if vdi_label:
        cmd = ["rbd", "image-meta", "set", VDI_NAME, "VDI_LABEL", vdi_label, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
        cmdout = util.pread2(cmd)
    if vdi_description:
        cmd = ["rbd", "image-meta", "set", VDI_NAME, "VDI_DESCRIPTION", vdi_description, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
        cmdout = util.pread2(cmd)

def delete_vdi_kernel(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    cmd = ["rbd", "rm", VDI_NAME, "--pool", POOL_NAME, "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)

def delete_vdi_fuse(sr_uuid, vdi_uuid):
    POOL_MOUNT_PATH = "%s/%s" % (SR_MOUNT_PREFIX, sr_uuid)
    RBD_VDI_PATH = "%s/%s%s" % (POOL_MOUNT_PATH, VDI_PREFIX, vdi_uuid)
    cmd = ["rm", "-f", RBD_VDI_PATH]
    cmdout = util.pread2(cmd)

def _get_vdi_info(sr_uuid, vdi_uuid):
    VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
    POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
    cmd = ["rbd", "image-meta", "list", VDI_NAME, "--pool", POOL_NAME, "--format", "json", "--id", "xenserver", "--keyring", "/etc/ceph/ceph.client.xenserver.keyring"]
    cmdout = util.pread2(cmd)
    if len(cmdout) != 0:
        decoded = json.loads(cmdout)
        return decoded
    else:
        return {}
