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
import blktap2
import XenAPI
import inventory


RBDPOOL_PREFIX = "RBD_XenStorage-"
VDI_PREFIX = "VHD-"
SXM_PREFIX = "SXM-"
SNAPSHOT_PREFIX = "SNAP-"
CLONE_PREFIX = "VHD-"

FUSE_PREFIX = "/run/sr-mount"
RBD_PREFIX = "/dev/rbd"
NBD_PREFIX = "/dev/nbd"
DM_PREFIX = "/dev/mapper"

NBDS_MAX = 64
BLOCK_SIZE = 21 #2097152 bytes
OBJECT_SIZE_IN_B = 2097152

IMAGE_FORMAT = 2

class SR:

    def __init__(self):
        self.mode = ''
        self.uuid = ''
        self.SR_ROOT = ''

    def _get_vdi_uuid(self, vdi):
        regex = re.compile(VDI_PREFIX)
        return regex.sub('', vdi)

    def _get_snap_uuid(self, vdi):
        regex = re.compile(SNAPSHOT_PREFIX)
        return regex.sub('', vdi)

    def _get_vdi_info(self, vdi_uuid):
        VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)
        cmdout = util.pread2(["rbd", "image-meta", "list", VDI_NAME, "--pool", self.CEPH_POOL_NAME, "--format", "json", "--name", self.CEPH_USER])
        if len(cmdout) != 0:
            decoded = json.loads(cmdout)
            return decoded
        else:
            return {}

    def _get_vdilist(self, pool):
        RBDVDIs = {}
        cmd = ["rbd", "ls", "-l", "--format", "json", "--pool", pool, "--name", self.CEPH_USER]
        cmdout = util.pread2(cmd)
        decoded = json.loads(cmdout)
        for vdi in decoded:
            if vdi['image'].find("SXM") == -1:
                if vdi.has_key('snapshot'):
                    if vdi['image'].startswith(SNAPSHOT_PREFIX):
                        snap_uuid = self._get_snap_uuid(vdi['snapshot'])
                        RBDVDIs[snap_uuid] = vdi
                elif vdi['image'].startswith(VDI_PREFIX) or vdi['image'].startswith(CLONE_PREFIX):
                    vdi_uuid = self._get_vdi_uuid(vdi['image'])
                    RBDVDIs[vdi_uuid] = vdi
        return RBDVDIs

    def _srlist_toxml(self):
        self.RBDPOOLs = self._get_srlist()
        dom = xml.dom.minidom.Document()
        element = dom.createElement("SRlist")
        dom.appendChild(element)

        for sr_uuid in self.RBDPOOLs.keys():
            entry = dom.createElement('SR')
            element.appendChild(entry)

            subentry = dom.createElement("UUID")
            entry.appendChild(subentry)
            textnode = dom.createTextNode(sr_uuid)
            subentry.appendChild(textnode)

            subentry = dom.createElement("PoolName")
            entry.appendChild(subentry)
            textnode = dom.createTextNode(self.RBDPOOLs[sr_uuid]["name"])
            subentry.appendChild(textnode)

            subentry = dom.createElement("Size")
            entry.appendChild(subentry)
            size = str(self.RBDPOOLs[sr_uuid]["stats"]["max_avail"] + self.RBDPOOLs[sr_uuid]["stats"]["bytes_used"])
            textnode = dom.createTextNode(size)
            subentry.appendChild(textnode)

            subentry = dom.createElement("BytesUses")
            entry.appendChild(subentry)
            bytesused = str(self.RBDPOOLs[sr_uuid]["stats"]["bytes_used"])
            textnode = dom.createTextNode(bytesused)
            subentry.appendChild(textnode)

            subentry = dom.createElement("Objects")
            entry.appendChild(subentry)
            objects = str(self.RBDPOOLs[sr_uuid]["stats"]["objects"])
            textnode = dom.createTextNode(objects)
            subentry.appendChild(textnode)

        return dom.toprettyxml()

    def _get_path(self, vdi_uuid):
        ceph_vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        return os.path.join(self.SR_ROOT, ceph_vdi_name)

    def _get_snap_path(self, vdi_uuid, snap_uuid):
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        snapshot_name = "%s@%s%s" % (vdi_name, SNAPSHOT_PREFIX, snap_uuid)
        return os.path.join(self.SR_ROOT, snapshot_name)

    def _get_sr_uuid_by_name(self, pool):
        regex = re.compile(RBDPOOL_PREFIX)
        return regex.sub('', pool)

    def _get_allocated_size(self):
        allocated_bytes = 0

        rbdvdis = self._get_vdilist(self.CEPH_POOL_NAME)
        for vdi_uuid in rbdvdis.keys():
            allocated_bytes += rbdvdis[vdi_uuid]['size']
        return allocated_bytes

    def _isSpaceAvailable(self, size):

        sr_free_space = self.RBDPOOLs[self.uuid]['stats']['max_avail']
        if size > sr_free_space:
            return False
        else:
            return True

    def _get_srlist(self):
        RBDPOOLs = {}

        cmdout = util.pread2(["ceph", "df", "--format", "json", "--name", self.CEPH_USER])
        decoded = json.loads(cmdout)
        for poolinfo in decoded['pools']:
            regex = re.compile(RBDPOOL_PREFIX)
            if regex.search(poolinfo['name']):
                sr_uuid = self._get_sr_uuid_by_name(poolinfo['name'])
                RBDPOOLs[sr_uuid] = poolinfo
        return RBDPOOLs

    def load(self, sr_uuid, ceph_user):
        self.CEPH_USER = ( "client.%s" % ceph_user )
        self.CEPH_POOL_NAME = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
        self.RBDPOOLs = self._get_srlist()

        # Fallback to kernel mode if mode is fuse with different than admin
        # => --name arg not compatible with fuse mode
        if self.CEPH_USER != "client.admin" and self.mode == "fuse":
            self.mode = "kernel"

        if self.mode == "kernel":
             self.SR_ROOT = "%s/%s" % (RBD_PREFIX, self.CEPH_POOL_NAME)
        elif self.mode == "fuse":
             self.SR_ROOT = "%s/%s" % (FUSE_PREFIX, sr_uuid)
        elif self.mode == "nbd":
             self.SR_ROOT = "%s/%s" % (NBD_PREFIX, self.CEPH_POOL_NAME)

    def scan(self, sr_uuid):
        self.load(sr_uuid)

    def attach(self, sr_uuid):

        #self.RBDVDIs = self._get_vdilist(self.CEPH_POOL_NAME)
        self.load(sr_uuid)

        if self.mode != "kernel":
            util.pread2(["mkdir", "-p", self.SR_ROOT])

        if self.mode == "kernel":
            pass
        elif self.mode == "fuse":
            util.pread2(["rbd-fuse", "-p", self.CEPH_POOL_NAME, self.SR_ROOT])
        elif self.mode == "nbd":
            pass

    def detach(self, sr_uuid):

        if self.mode == "kernel":
            pass
        elif self.mode == "fuse":
            util.pread2(["fusermount", "-u", self.SR_ROOT])
        elif self.mode == "nbd":
            pass

        if self.mode != "kernel":
            util.pread2(["rm", "-rf", self.SR_ROOT])

class VDI:

    def load(self, vdi_uuid):
        self.CEPH_VDI_NAME = "%s%s" % (VDI_PREFIX, vdi_uuid)

    def create(self, sr_uuid, vdi_uuid, size):
        image_size = (size + OBJECT_SIZE_IN_B)/ 1024 / 1024
        # before JEWEL: util.pread2(["rbd", "create", self.CEPH_VDI_NAME, "--size", str(image_size), "--order", str(BLOCK_SIZE), "--image-format", str(IMAGE_FORMAT), "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        util.pread2(["rbd", "create", self.CEPH_VDI_NAME, "--size", str(image_size), "--object-size", str(OBJECT_SIZE_IN_B), "--image-format", str(IMAGE_FORMAT), "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        if self.label:
            util.pread2(["rbd", "image-meta", "set", self.CEPH_VDI_NAME, "VDI_LABEL", self.label, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        if self.description:
            util.pread2(["rbd", "image-meta", "set", self.CEPH_VDI_NAME, "VDI_DESCRIPTION", self.description, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])

    def resize(self, sr_uuid, vdi_uuid, size):
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if sm_config.has_key('attached'):
            if not blktap2.VDI.tap_pause(self.session, self,sr.uuid, vdi_uuid):
                raise util.SMException("failed to pause VDI %s" % vdi_uuid)
            self._unmap_VHD(vdi_uuid)
        #---
        image_size = size / 1024 / 1024
        util.pread2(["rbd", "resize", "--size", str(image_size), "--allow-shrink", self.CEPH_VDI_NAME, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        #---
        if sm_config.has_key('attached'):
            self._map_VHD(vdi_uuid)
            blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None)

    def update(self, sr_uuid, vdi_uuid):
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        if self.label:
            util.pread2(["rbd", "image-meta", "set", vdi_name, "VDI_LABEL", self.label, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        if self.description:
            util.pread2(["rbd", "image-meta", "set", vdi_name, "VDI_DESCRIPTION", self.description, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        for snapshot_uuid in self.snaps.keys():
            snapshot_name = "%s%s" % (SNAPSHOT_PREFIX, snapshot_uuid)
            util.pread2(["rbd", "image-meta", "set", vdi_name, snapshot_name, str(self.snaps[snapshot_uuid]), "--pool",self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])

    def _flatten_clone(self, clone_uuid):
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if sm_config.has_key('attached'):
            if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, clone_uuid):
                raise util.SMException("failed to pause VDI %s" % clone_uuid)
            self._unmap_VHD(clone_uuid)
        #--- ?????? CHECK For running VM. What if flattening takes a long time and vdi is paused during this process
        clone_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, CLONE_PREFIX, clone_uuid)
        util.pread2(["rbd", "flatten", clone_name, "--name", self.sr.CEPH_USER])
        #--- ??????
        if sm_config.has_key('attached'):
            self._map_VHD(clone_uuid)
            blktap2.VDI.tap_unpause(self.session, self.sr.uuid, clone_uuid, None)

    def _delete_snapshot(self, vdi_uuid, snap_uuid):
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        snapshot_name = "%s@%s%s" % (vdi_name, SNAPSHOT_PREFIX, snap_uuid)
        short_snap_name = "%s%s" % (SNAPSHOT_PREFIX, snap_uuid)
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if sm_config.has_key('attached'):
            if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                raise util.SMException("failed to pause VDI %s" % vdi_uuid)
            self._unmap_VHD(vdi_uuid)
        #---
        util.pread2(["rbd", "snap", "unprotect", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        util.pread2(["rbd", "snap", "rm", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        util.pread2(["rbd", "image-meta", "remove", vdi_name, short_snap_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        #---
        if sm_config.has_key('attached'):
            self._map_VHD(vdi_uuid)
            blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None)

    def _delete_vdi(self, vdi_uuid):
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        vdi_path = "%s/%s%s" % (self.sr.SR_ROOT, VDI_PREFIX, vdi_uuid)
        if self.mode == "kernel":
            util.pread2(["rbd", "rm", vdi_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        elif self.mode == "fuse":
            util.pread2(["rm", "-f", vdi_path])
        elif self.mode == "nbd":
            util.pread2(["rbd", "rm", vdi_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])

    def _change_image_prefix_to_SXM(self, vdi_uuid):
        orig_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, VDI_PREFIX, vdi_uuid)
        new_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, SXM_PREFIX, vdi_uuid)
        util.pread2(["rbd", "mv", orig_name, new_name, "--name", self.sr.CEPH_USER])

    def _change_image_prefix_to_VHD(self, vdi_uuid):
        orig_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, SXM_PREFIX, vdi_uuid)
        new_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, VDI_PREFIX, vdi_uuid)
        util.pread2(["rbd", "mv", orig_name, new_name, "--name", self.sr.CEPH_USER])

    def _rename_image(self, orig_uuid, new_uuid):
        orig_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, VDI_PREFIX, orig_uuid)
        new_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, VDI_PREFIX, new_uuid)
        util.pread2(["rbd", "mv", orig_name, new_name, "--name", self.sr.CEPH_USER])

    def _do_clone(self, vdi_uuid, snap_uuid, clone_uuid, vdi_label):
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        snapshot_name = "%s/%s@%s%s" % (self.sr.CEPH_POOL_NAME, vdi_name, SNAPSHOT_PREFIX, snap_uuid)
        clone_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, CLONE_PREFIX, clone_uuid)
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if sm_config.has_key('attached'):
            if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                raise util.SMException("failed to pause VDI %s" % vdi_uuid)
            self._unmap_VHD(vdi_uuid)
        #---
        util.pread2(["rbd", "clone", snapshot_name, clone_name, "--name", self.sr.CEPH_USER])
        util.pread2(["rbd", "image-meta", "set", clone_name, "VDI_LABEL", vdi_label, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        util.pread2(["rbd", "image-meta", "set", clone_name, "CLONE_OF", snap_uuid, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        #---
        if sm_config.has_key('attached'):
            self._map_VHD(vdi_uuid)
            blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None)

    def _do_snapshot(self, vdi_uuid, snap_uuid):
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        snapshot_name = "%s@%s%s" % (vdi_name, SNAPSHOT_PREFIX, snap_uuid)
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if sm_config.has_key('attached'):
            if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                raise util.SMException("failed to pause VDI %s" % vdi_uuid)
            self._unmap_VHD(vdi_uuid)
        #---
        util.pread2(["rbd", "snap", "create", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        util.pread2(["rbd", "snap", "protect", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        #---
        if sm_config.has_key('attached'):
            self._map_VHD(vdi_uuid)
            blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None)

    def _rollback_snapshot(self, base_uuid, snap_uuid):
        vdi_name = "%s%s" % (VDI_PREFIX, base_uuid)
        snapshot_name = "%s@%s%s" % (vdi_name, SNAPSHOT_PREFIX, snap_uuid)
        util.pread2(["rbd", "snap", "rollback", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])

    def _get_vdi_info(self, vdi_uuid):
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        cmdout = util.pread2(["rbd", "image-meta", "list", vdi_name, "--pool", self.sr.CEPH_POOL_NAME, "--format", "json", "--name", self.sr.CEPH_USER])
        if len(cmdout) != 0:
            decoded = json.loads(cmdout)
            return decoded
        else:
            return {}

    def _call_plugin(self, op, args):
        vdi_uuid = args['vdi_uuid']
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        util.SMlog("Calling ceph_plugin")

        if filter(lambda x: x.startswith('host_'), sm_config.keys()):
            for key in filter(lambda x: x.startswith('host_'), sm_config.keys()):
                host_ref = key[len('host_'):]
                util.SMlog("Calling rbd/nbd map on host %s" % host_ref)
                if not self.session.xenapi.host.call_plugin(host_ref, "ceph_plugin", op, args):
                    # Failed to pause node
                    raise util.SMException("failed to %s VDI %s" % (op, mirror_uuid))
        else:
            host_uuid = inventory.get_localhost_uuid()
            host_ref = self.session.xenapi.host.get_by_uuid(host_uuid)
            util.SMlog("Calling rbd/nbd map on localhost %s" % host_ref)
            if not self.session.xenapi.host.call_plugin(host_ref, "ceph_plugin", op, args):
                # Failed to pause node
                raise util.SMException("failed to %s VDI %s" % (op, mirror_uuid))

    def _map_SXM(self, vdi_uuid):
        util.SMlog("Calling _map_SXM")
        vdi_name = "%s%s" % (SXM_PREFIX, vdi_uuid)
        dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)
        args = {"mode":self.mode, "vdi_name":vdi_name, "vdi_uuid":vdi_uuid, "dev_name":dev_name,
                "CEPH_POOL_NAME":self.sr.CEPH_POOL_NAME, "NBDS_MAX":str(NBDS_MAX),
                "CEPH_USER":self.sr.CEPH_USER}
        self._call_plugin('map',args)

    def _map_VHD(self, vdi_uuid):
        util.SMlog("Calling _map_VHD")
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)
        args = {"mode":self.mode, "vdi_name":vdi_name, "vdi_uuid":vdi_uuid, "dev_name":dev_name,
                "CEPH_POOL_NAME":self.sr.CEPH_POOL_NAME, "NBDS_MAX":str(NBDS_MAX),
                "CEPH_USER":self.sr.CEPH_USER}
        self._call_plugin('map',args)

    def _unmap_VHD(self, vdi_uuid):
        util.SMlog("Calling _unmap_VHD")
        vdi_name = "%s%s" % (VDI_PREFIX, vdi_uuid)
        dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)
        args = {"mode":self.mode, "vdi_name":vdi_name, "vdi_uuid":vdi_uuid, "dev_name":dev_name,
                "CEPH_POOL_NAME":self.sr.CEPH_POOL_NAME, "NBDS_MAX":str(NBDS_MAX),
                "CEPH_USER":self.sr.CEPH_USER}
        self._call_plugin('unmap',args)

    def _unmap_SXM(self, vdi_uuid):
        util.SMlog("Calling _unmap_SXM")
        vdi_name = "%s%s" % (SXM_PREFIX, vdi_uuid)
        dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)
        args = {"mode":self.mode, "vdi_name":vdi_name, "vdi_uuid":vdi_uuid, "dev_name":dev_name,
                "CEPH_POOL_NAME":self.sr.CEPH_POOL_NAME, "NBDS_MAX":str(NBDS_MAX),
                "CEPH_USER":self.sr.CEPH_USER}
        self._call_plugin('unmap',args)

    def _map_SNAP(self, vdi_uuid, snap_uuid):
        util.SMlog("Calling _map_VHD")
        snap_name = "%s%s@%s%s" % (VDI_PREFIX, vdi_uuid, SNAPSHOT_PREFIX, snap_uuid)
        dev_name = "%s/%s" % (self.sr.SR_ROOT, snap_name)
        args = {"mode":self.mode, "snap_name":snap_name, "vdi_uuid":snap_uuid, "dev_name":dev_name,
                "CEPH_POOL_NAME":self.sr.CEPH_POOL_NAME, "NBDS_MAX":str(NBDS_MAX),
                "CEPH_USER":self.sr.CEPH_USER}
        self._call_plugin('map',args)

    def _unmap_SNAP(self, vdi_uuid, snap_uuid):
        util.SMlog("Calling _unmap_VHD")
        snap_name = "%s%s@%s%s" % (VDI_PREFIX, vdi_uuid, SNAPSHOT_PREFIX, snap_uuid)
        dev_name = "%s/%s" % (self.sr.SR_ROOT, snap_name)
        args = {"mode":self.mode, "snap_name":snap_name, "vdi_uuid":snap_uuid, "dev_name":dev_name,
                "CEPH_POOL_NAME":self.sr.CEPH_POOL_NAME, "NBDS_MAX":str(NBDS_MAX),
                "CEPH_USER":self.sr.CEPH_USER}
        self._call_plugin('unmap',args)

    def _setup_mirror (self, vdi_uuid, size):

        self.RBD_VDI_MIRROR_DEV = "%s/%s%s" % (self.sr.SR_ROOT, VDI_PREFIX, vdi_uuid)
        self.RBD_SXM_MIRROR_DEV = "%s/%s%s" % (self.sr.SR_ROOT, SXM_PREFIX, vdi_uuid)

        self.DM_ZERO_NAME = "%s-%s%s-zero" % (self.sr.CEPH_POOL_NAME, SXM_PREFIX, vdi_uuid)
        self.DM_ZERO_DEV = "%s/%s" % (DM_PREFIX, self.DM_ZERO_NAME)

        self.DM_MIRROR_NAME = "%s-%s%s" % (self.sr.CEPH_POOL_NAME, SXM_PREFIX, vdi_uuid)
        self.DM_MIRROR_DEV = "%s/%s" % (DM_PREFIX, self.DM_MIRROR_NAME)

        self._change_image_prefix_to_SXM(vdi_uuid)

        self._map_SXM(vdi_uuid)

        util.pread2(["dmsetup", "create", self.DM_ZERO_NAME, "--table", "0 %s zero" % str(int(size) / 512)])
        util.pread2(["dmsetup", "create", self.DM_MIRROR_NAME, "--table", "0 %s snapshot %s %s P 1" % (str(int(size) / 512), self.DM_ZERO_DEV, self.RBD_SXM_MIRROR_DEV)])
        util.pread2(["ln", "-s", self.DM_MIRROR_DEV, self.RBD_VDI_MIRROR_DEV])

    def _setup_base (self, vdi_uuid, size):

        self.RBD_VDI_BASE_DEV = "%s/%s%s" % (self.sr.SR_ROOT, VDI_PREFIX, vdi_uuid)
        self.DM_BASE_NAME = "%s-%s%s" % (self.sr.CEPH_POOL_NAME, VDI_PREFIX, vdi_uuid)
        self.DM_BASE_DEV = "%s/%s" % (DM_PREFIX, self.DM_BASE_NAME)
        util.pread2(["dmsetup", "create", self.DM_BASE_NAME, "--table", "0 %s snapshot-origin %s" % (str(int(size) / 512), self.RBD_VDI_BASE_DEV)])

    def _merge_diffs (self, mirror_uuid, snap_uuid, base_uuid, size):

        if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, mirror_uuid):
            raise util.SMException("failed to pause VDI %s" % mirror_uuid)
        #---
        util.pread2(["unlink", self.RBD_VDI_MIRROR_DEV])
        util.pread2(["dmsetup", "remove", self.DM_MIRROR_NAME])
        util.pread2(["dmsetup", "remove", self.DM_ZERO_NAME])

        self._map_VHD(base_uuid)
        self._setup_base(base_uuid, size)

        util.pread2(["dmsetup", "suspend", self.DM_BASE_NAME])
        util.pread2(["dmsetup", "reload", self.DM_BASE_NAME, "--table", "0 %s snapshot-merge %s %s P 1" % (str(int(size) / 512), self.RBD_VDI_BASE_DEV, self.RBD_SXM_MIRROR_DEV)])
        util.pread2(["dmsetup", "resume", self.DM_BASE_NAME])
        # we should wait until the merge is completed
        util.pread2(["waitdmmerging.sh", self.DM_BASE_NAME])
        # -------------------------------------------
        util.pread2(["dmsetup", "remove", self.DM_BASE_NAME])

        self._unmap_VHD(base_uuid)
        self._unmap_SXM(mirror_uuid)

        self._change_image_prefix_to_VHD(mirror_uuid)
        # -----
        tmp_uuid = "temporary"  # util.gen_uuid()
        self._rename_image(mirror_uuid, tmp_uuid)
        self._rename_image(base_uuid, mirror_uuid)
        self._rename_image(tmp_uuid, base_uuid)
        # -----
        self._map_VHD(mirror_uuid)
        #---
        blktap2.VDI.tap_unpause(self.session, self.sr.uuid, mirror_uuid, None)

