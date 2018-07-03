#!/usr/bin/python
#
# Copyright (C) Sebastien Fuchs (sfuchs@emmene-moi.fr)
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

import sys
sys.path.append('/opt/xensource/sm')
import util
import json
from srmetadata import NAME_LABEL_TAG, NAME_DESCRIPTION_TAG, UUID_TAG, IS_A_SNAPSHOT_TAG, SNAPSHOT_OF_TAG, TYPE_TAG, \
    VDI_TYPE_TAG, READ_ONLY_TAG, MANAGED_TAG, SNAPSHOT_TIME_TAG, METADATA_OF_POOL_TAG, \
    METADATA_UPDATE_OBJECT_TYPE_TAG, METADATA_OBJECT_TYPE_SR, METADATA_OBJECT_TYPE_VDI

# Configure the script here. Use the full Ceph pool name and CephX ID.
POOL_NAME = "RBD_XenStorage-<sruuid>"
CEPH_ID = "admin"


CEPH_USER = "client." + CEPH_ID

cmd = ["rbd", "ls", "-l", "--format", "json", "--pool", POOL_NAME, "--name", CEPH_USER]
cmdout = util.pread2(cmd)
rbds_list = json.loads(cmdout)

for img_count, rbd in enumerate(rbds_list):
    cmdout = util.pread2(["rbd", "image-meta", "list", rbd['image'], "--pool", POOL_NAME,
                              "--format", "json", "--name", CEPH_USER])
    if len(cmdout) != 0:
        vdi_info = json.loads(cmdout)
        try:
            if 'VDI_LABEL' in vdi_info:
                util.SMlog("rbd_meta_migration: update metadata of poolname = %s, rbdimage = %s : %s" % (POOL_NAME, rbd['image'], vdi_info))

                tag = ":" + NAME_LABEL_TAG
                if tag not in vdi_info:
                    util.SMlog("rbd_meta_migration: poolname = %s, rbdimage = %s METADATA VDI_LABEL(%s) => %s" % (POOL_NAME, rbd['image'], vdi_info['VDI_LABEL'], tag ))
                    util.pread2(["rbd", "image-meta", "set", rbd['image'], tag, str(vdi_info['VDI_LABEL']), "--pool", POOL_NAME, "--name", CEPH_USER])

                tag = ":" + NAME_DESCRIPTION_TAG
                if 'VDI_DESCRIPTION' in vdi_info and tag not in vdi_info:
                    util.SMlog("rbd_meta_migration: poolname = %s, rbdimage = %s METADATA VDI_DESCRIPTION(%s) => %s" % (POOL_NAME, rbd['image'], vdi_info['VDI_DESCRIPTION'], tag ))
                    util.pread2(["rbd", "image-meta", "set", rbd['image'], tag, str(vdi_info['VDI_DESCRIPTION']), "--pool", POOL_NAME, "--name", CEPH_USER])
            
                # Set mandatory new meta
                type = "vhd" if rbd['image'].startswith("VHD-") else "rbd"
                tag = ":" + IS_A_SNAPSHOT_TAG
                if tag not in vdi_info:
                    #util.pread2(["rbd", "image-meta", "set", rbd['image'], ":"+UUID_TAG, str("vhd"), "--pool", POOL_NAME, "--name", CEPH_USER])
                    util.pread2(["rbd", "image-meta", "set", rbd['image'], tag, str(0), "--pool", POOL_NAME, "--name", CEPH_USER])
                tag = ":" + MANAGED_TAG
                if tag not in vdi_info:
                    util.pread2(["rbd", "image-meta", "set", rbd['image'], tag, str(1), "--pool", POOL_NAME, "--name", CEPH_USER])
                tag = ":" + READ_ONLY_TAG
                if tag not in vdi_info:
                    util.pread2(["rbd", "image-meta", "set", rbd['image'], tag, str(0), "--pool", POOL_NAME, "--name", CEPH_USER])
                tag = ":" + TYPE_TAG
                if tag not in vdi_info:
                    util.pread2(["rbd", "image-meta", "set", rbd['image'], tag, str("user"), "--pool", POOL_NAME, "--name", CEPH_USER])
                tag = ":" + VDI_TYPE_TAG
                if tag not in vdi_info:
                    util.pread2(["rbd", "image-meta", "set", rbd['image'], tag, str( type), "--pool", POOL_NAME, "--name", CEPH_USER])
                tag = ":sm_config"
                if tag not in vdi_info:
                    util.pread2(["rbd", "image-meta", "set", rbd['image'], tag, str('{"vdi_type": "'+type+'"}'), "--pool", POOL_NAME, "--name", CEPH_USER])
                    
        except Exception as e:
            util.SMlog("rbdsr_common.RBDMetadataHandler.updateMetadata: Exception: rbd image-meta set failed: (%s)" % str(e))
