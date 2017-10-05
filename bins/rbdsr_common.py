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

import util
import xml.dom.minidom
import re
import json
import xs_errors
import inventory
import SR
import VDI
import blktap2
import xmlrpclib
import scsiutil
import os.path
from srmetadata import NAME_LABEL_TAG, NAME_DESCRIPTION_TAG, UUID_TAG, IS_A_SNAPSHOT_TAG, SNAPSHOT_OF_TAG, TYPE_TAG,\
                       VDI_TYPE_TAG, READ_ONLY_TAG, MANAGED_TAG, SNAPSHOT_TIME_TAG, METADATA_OF_POOL_TAG, \
                       METADATA_UPDATE_OBJECT_TYPE_TAG, METADATA_OBJECT_TYPE_SR, METADATA_OBJECT_TYPE_VDI

DRIVER_CLASS_PREFIX={}

RBDPOOL_PREFIX = "RBD_XenStorage-"
CEPH_USER_DEFAULT = 'admin'

SR_PREFIX = "/run/sr-mount"
FUSE_PREFIX = "/dev/fuse"
RBD_PREFIX = "/dev/rbd"
NBD_PREFIX = "/dev/nbd"
DM_PREFIX = "/dev/mapper"

NBDS_MAX = 64
BLOCK_SIZE = 21  # 2097152 bytes
OBJECT_SIZE_B = 2097152

USE_RBD_META_DEFAULT = True
VDI_UPDATE_EXISTING_DEFAULT = True
MDVOLUME_NAME = "MGT"
MDVOLUME_SIZE_M = 4

IMAGE_FORMAT_DEFAULT = 2


class CSR(SR.SR):

    def __init__(self, srcmd, sr_uuid):
        """
        :param srcmd:
        :param sr_uuid:
        """
        util.SMlog("rbdsr_common.SR.__init__: srcmd = %s, sr_uuid= %s" % (srcmd, sr_uuid))

        self.RBDPOOL_PREFIX = RBDPOOL_PREFIX
        self.VDI_PREFIX = ''  # Should be defined in certain vdi type implementation
        #self.SNAPSHOT_PREFIX = "SNAP-"
        self.SNAPSHOT_PREFIX = ''  # Should be defined in certain vdi type implementation
        self.CEPH_POOL_NAME = "%s%s" % (self.RBDPOOL_PREFIX, sr_uuid)
        self.CEPH_USER = ("client.%s" % CEPH_USER_DEFAULT)
        self.USE_RBD_META = USE_RBD_META_DEFAULT
        self.VDI_UPDATE_EXISTING = VDI_UPDATE_EXISTING_DEFAULT
        self.MDVOLUME_NAME = MDVOLUME_NAME
        self.MDVOLUME_SIZE_M = MDVOLUME_SIZE_M
        self.IMAGE_FORMAT_DEFAULT = IMAGE_FORMAT_DEFAULT
        self.mode = '' # Should be defined in certain vdi type implementation
        self.vdi_type = '' # Should be defined in certain vdi type implementation

        super(CSR, self).__init__(srcmd, sr_uuid)

    def _get_vdi_uuid(self, vdi_name):
        """
        :param vdi_name:
        :return:
        """
        util.SMlog("rbdsr_common.SR._get_vdi_uuid: vdi_name = %s" % vdi_name)
        regex = re.compile(self.VDI_PREFIX)
        return regex.sub('', vdi_name)

    def _get_sr_uuid_by_name(self, rbd_pool_name):
        """
        :param rbd_pool_name:
        :return: sr_uuid:
        """
        util.SMlog("rbdsr_common.SR._get_sr_uuid_by_name: rbd_pool_name = %s" % rbd_pool_name)

        regex = re.compile(self.RBDPOOL_PREFIX)
        return regex.sub('', rbd_pool_name)

    def _get_srsdict(self):
        """
        :return: RBDPOOLs: Dictionary with Storage Repositories found in Ceph cluster
        """
        util.SMlog("rbdsr_common.SR._get_srsdict")
        rbdpools = {}

        cmdout = util.pread2(["ceph", "df", "--format", "json", "--name", self.CEPH_USER])
        decoded = json.loads(cmdout)
        for poolinfo in decoded['pools']:
            regex = re.compile(self.RBDPOOL_PREFIX)
            if regex.search(poolinfo['name']):
                sr_uuid = self._get_sr_uuid_by_name(poolinfo['name'])
                rbdpools[sr_uuid] = poolinfo
        return rbdpools

    def _allocate_dev_instance(self, sr_uuid, vdi_uuid, host_uuid=None):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param host_uuid:
        :return:
        """

        sr_ref = self.session.xenapi.SR.get_by_uuid(sr_uuid)
        sr_sm_config = self.session.xenapi.SR.get_sm_config(sr_ref)
        try:
            vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
            vdi_sm_config = self.session.xenapi.SR.get_sm_config(vdi_ref)
        except Exception:
            host_uuid = inventory.get_localhost_uuid()

        if "dev_instances" not in sr_sm_config:
            sr_dev_instances = {"hosts": {}}
            if host_uuid is None:
                if filter(lambda x: x.startswith('host_'), vdi_sm_config.keys()):
                    for key in filter(lambda x: x.startswith('host_'), vdi_sm_config.keys()):
                        host_ref = key[len('host_'):]
                        host_uuid = self.session.xenapi.host.get_uuid(host_ref)
                        sr_dev_instances["hosts"][host_uuid] = [None] * NBDS_MAX
                        sr_dev_instances["hosts"][host_uuid][0] = [MDVOLUME_NAME, 1]
                        sr_dev_instances["hosts"][host_uuid][1] = "reserved"
                        sr_dev_instances["hosts"][host_uuid][2] = "reserved"
            else:
                host_uuid = inventory.get_localhost_uuid()
                sr_dev_instances["hosts"][host_uuid] = [None] * NBDS_MAX
                sr_dev_instances["hosts"][host_uuid][0] = [MDVOLUME_NAME,1]
                sr_dev_instances["hosts"][host_uuid][1] = "reserved"
                sr_dev_instances["hosts"][host_uuid][2] = "reserved"
        else:
            sr_dev_instances = json.loads(sr_sm_config["dev_instances"])

        ##
        def __allocate__():
            util.SMlog("rbdsr_common._allocate_dev_instance: sr_uuid=%s, vdi_uuid=%s, host_uuid = %s"
                       % (sr_uuid, vdi_uuid, host_uuid))

            dev_instance = self._get_dev_instance(sr_uuid, vdi_uuid, host_uuid)

            if dev_instance is None:
                for i in range(NBDS_MAX):
                    if sr_dev_instances["hosts"][host_uuid][i] is None:
                        dev_instance = i
                        break

            if dev_instance == -1:
                raise xs_errors.XenError('VDIUnavailable', opterr='Could not allocate dev instance for sr %s vdi %s on \
                                         host %s' % (sr_uuid, vdi_uuid, host_uuid))

            if sr_dev_instances["hosts"][host_uuid][dev_instance] is None:
                sr_dev_instances["hosts"][host_uuid][dev_instance] = [vdi_uuid, 1]
            else:
                sr_dev_instances["hosts"][host_uuid][dev_instance][1] += 1

            util.SMlog("rbdsr_common._allocate_dev_instance: Dev instance %s allocated on host %s for sr %s vdi %s"
                       % (dev_instance, host_uuid, sr_uuid, vdi_uuid))
        ##

        if host_uuid is None:
            if filter(lambda x: x.startswith('host_'), vdi_sm_config.keys()):
                for key in filter(lambda x: x.startswith('host_'), vdi_sm_config.keys()):
                    host_ref = key[len('host_'):]
                    host_uuid = self.session.xenapi.host.get_uuid(host_ref)
                    __allocate__()
            else:
                host_uuid = inventory.get_localhost_uuid()
                __allocate__()
        else:
            __allocate__()

        if "dev_instances" in sr_sm_config:
            self.session.xenapi.SR.remove_from_sm_config(sr_ref, "dev_instances")
        self.session.xenapi.SR.add_to_sm_config(sr_ref, "dev_instances", json.dumps(sr_dev_instances))

    def _get_dev_instance(self, sr_uuid, vdi_uuid, host_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param host_uuid:
        :return:
        """
        util.SMlog("rbdsr_common._get_dev_instance: sr_uuid=%s, vdi_uuid=%s, host_uuid = %s"
                   % (sr_uuid, vdi_uuid, host_uuid))

        sr_ref = self.session.xenapi.SR.get_by_uuid(sr_uuid)
        sr_sm_config = self.session.xenapi.SR.get_sm_config(sr_ref)

        dev_instance = None

        if "dev_instances" in sr_sm_config:
            sr_dev_instances = json.loads(sr_sm_config["dev_instances"])
            if host_uuid in sr_dev_instances["hosts"]:
                for i in range(NBDS_MAX):
                    if sr_dev_instances["hosts"][host_uuid][i] is not None:
                        if sr_dev_instances["hosts"][host_uuid][i][0] == vdi_uuid:
                            dev_instance = i
                            break

        return dev_instance

    def _get_instance_ref_count(self, sr_uuid, vdi_uuid, host_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param host_uuid:
        :return:
        """
        util.SMlog("rbdsr_common._get_instance_ref_count: sr_uuid=%s, vdi_uuid=%s, host_uuid = %s"
                   % (sr_uuid, vdi_uuid, host_uuid))

        sr_ref = self.session.xenapi.SR.get_by_uuid(sr_uuid)
        sr_sm_config = self.session.xenapi.SR.get_sm_config(sr_ref)

        ref_count = 0

        if "dev_instances" in sr_sm_config:
            sr_dev_instances = json.loads(sr_sm_config["dev_instances"])
            if host_uuid in sr_dev_instances["hosts"]:
                for i in range(NBDS_MAX):
                    if sr_dev_instances["hosts"][host_uuid][i] is not None:
                        if sr_dev_instances["hosts"][host_uuid][i][0] == vdi_uuid:
                            ref_count = sr_dev_instances["hosts"][host_uuid][i][1]
                            break

        return ref_count

    def _free_dev_instance(self, sr_uuid, vdi_uuid, host_uuid=None):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param host_uuid:
        :return:
        """
        util.SMlog("rbdsr_common._free_dev_instance: sr_uuid=%s, vdi_uuid=%s, host_uuid = %s"
                   % (sr_uuid, vdi_uuid, host_uuid))

        sr_ref = self.session.xenapi.SR.get_by_uuid(sr_uuid)
        sr_sm_config = self.session.xenapi.SR.get_sm_config(sr_ref)
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        vdi_sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        sr_dev_instances = json.loads(sr_sm_config["dev_instances"])

        ##
        def __free__():
            if host_uuid in sr_dev_instances["hosts"]:
                for i in range(NBDS_MAX):
                    if sr_dev_instances["hosts"][host_uuid][i] is not None:
                        if sr_dev_instances["hosts"][host_uuid][i][0] == vdi_uuid:
                            if sr_dev_instances["hosts"][host_uuid][i][1] == 1:
                                sr_dev_instances["hosts"][host_uuid][i] = None
                                ref_count = 0
                            else:
                                sr_dev_instances["hosts"][host_uuid][i][1] -= 1
                                ref_count = sr_dev_instances["hosts"][host_uuid][i][1]
                            break
        ##

        if "dev_instances" in sr_sm_config:
            if host_uuid is None:
                if filter(lambda x: x.startswith('host_'), vdi_sm_config.keys()):
                    for key in filter(lambda x: x.startswith('host_'), vdi_sm_config.keys()):
                        host_ref = key[len('host_'):]
                        host_uuid = self.session.xenapi.host.get_uuid(host_ref)
                        __free__()
                else:
                    host_uuid = inventory.get_localhost_uuid()
                    __free__()
            else:
                __free__()

            self.session.xenapi.SR.remove_from_sm_config(sr_ref, "dev_instances")
            self.session.xenapi.SR.add_to_sm_config(sr_ref, "dev_instances", json.dumps(sr_dev_instances))

    def _get_path(self, vdi_name):
        """
        :param vdi_name:
        :return:
        """
        util.SMlog("rbdsr_common.SR._get_path: vdi_name = %s" % vdi_name)
        return "%s/%s" % (self.SR_ROOT, vdi_name)

    def _create_metadata_volume(self):
        """
        :return:
        """
        util.SMlog("rbdsr_common.SR._createMetadataVolume: sr_uuid = %s, mdpath = %s" % (self.uuid, self.mdpath))

        try:
            self.create_rbd(self.MDVOLUME_NAME, self.MDVOLUME_SIZE_M, self.IMAGE_FORMAT_DEFAULT)
        except Exception, e:
            raise xs_errors.XenError('MetadataError', opterr='Failed to create Metadata Volume: %s' % str(e))

    def _remove_metadata_volume(self):
        """
        :return:
        """
        util.SMlog("rbdsr_common.SR._removeMetadataVolume: sr_uuid = %s, mdpath = %s" % (self.uuid, self.mdpath))

        try:
            self.remove_rbd(MDVOLUME_NAME)
        except Exception, e:
            raise xs_errors.XenError('MetadataError', opterr='Failed to delete MGT Volume: %s' % str(e))

    def _map_metadata_volume(self):
        """
        :return:
        """
        util.SMlog("rbdsr_common.SR._map_metadata_volume: sr_uuid = %s, mdpath = %s" % (self.uuid, self.MDVOLUME_NAME))

        vdi_name = self.MDVOLUME_NAME
        _vdi_name = vdi_name
        _dev_name = vdi_name
        _dmdev_name = "%s%s" % (self.sr.DM_ROOT, _vdi_name)
        _dm_name = "%s-%s" % (self.sr.CEPH_POOL_NAME, _vdi_name)
        dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)

        args = {'mode': self.sr.mode, 'vdi_uuid': vdi_name,
                'vdi_name': vdi_name,  'dev_name': dev_name,
                '_vdi_name': _vdi_name,  '_dev_name': _dev_name,
                '_dmdev_name': _dmdev_name, '_dm_name': _dm_name,
                'CEPH_POOL_NAME': self.sr.CEPH_POOL_NAME,
                'NBDS_MAX': str(NBDS_MAX),
                'CEPH_USER': self.sr.CEPH_USER, 'sharable': 'True',
                'read_only': 'False', 'userbdmeta': self.sr.USE_RBD_META,
                'dmmode': 'None',
                'size': self.MDVOLUME_SIZE_M*1024*1024}

        host_uuid = inventory.get_localhost_uuid()

        try:
            self._call_plugin('map', args, 'ceph_plugin', host_uuid)
        except Exception, e:
            raise xs_errors.XenError('VDIUnavailable', opterr='Failed to map MGT volume, \
                                     host_uuid=%s (%s)' % (host_uuid, str(e)))

    def _unmap_metadata_volume(self):
        """
        :return:
        """
        util.SMlog("rbdsr_common.SR._unmap_metadata_volume: sr_uuid = %s, mdpath = %s" % (self.uuid, self.MDVOLUME_NAME))

        vdi_name = self.MDVOLUME_NAME
        _vdi_name = vdi_name
        _dev_name = vdi_name
        _dmdev_name = "%s%s" % (self.sr.DM_ROOT, _vdi_name)
        _dm_name = "%s-%s" % (self.sr.CEPH_POOL_NAME, _vdi_name)
        dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)

        args = {'mode': self.sr.mode, 'vdi_uuid': vdi_name,
                'vdi_name': vdi_name,  'dev_name': dev_name,
                '_vdi_name': _vdi_name,  '_dev_name': _dev_name,
                '_dmdev_name': _dmdev_name, '_dm_name': _dm_name,
                'CEPH_POOL_NAME': self.sr.CEPH_POOL_NAME,
                'NBDS_MAX': str(NBDS_MAX),
                'CEPH_USER': self.sr.CEPH_USER, 'sharable': 'True',
                'read_only': 'False', 'userbdmeta': self.sr.USE_RBD_META,
                'dmmode': 'None',
                'size': self.MDVOLUME_SIZE_M*1024*1024}

        host_uuid = inventory.get_localhost_uuid()

        try:
            self._call_plugin('unmap', args, 'ceph_plugin', host_uuid)
        except Exception, e:
            raise xs_errors.XenError('VDIUnavailable', opterr='Failed to unmap MGT volume, \
                                     host_uuid=%s (%s)' % (host_uuid, str(e)))

    def _if_rbd_exist(self, rbd_name):
        """
        :param vdi_name:
        :return:
        """
        util.SMlog("rbdsr_common.SR._if_vdi_exist: vdi_name=%s" % rbd_name)

        try:
            util.pread2(["rbd", "info", rbd_name, "--pool", self.CEPH_POOL_NAME, "--format", "json", "--name",
                         self.CEPH_USER])
            return True
        except Exception:
            return False

    def _get_rbds_list(self, pool_name):
        """
        :param pool_name:
        :return:
        """
        util.SMlog("rbdsr_common.SR._get_rbds_list: pool=%s" % pool_name)
        cmd = ["rbd", "ls", "-l", "--format", "json", "--pool", pool_name, "--name", self.CEPH_USER]
        cmdout = util.pread2(cmd)
        decoded = json.loads(cmdout)

        return decoded

    def _check_and_load_nbd(self):
        """
        :return:
        """
        util.SMlog("rbdsr_common.SR._check_and_load_nbd: Check and load nbd kernel driver")
        cmdout = os.popen("lsmod | grep nbd").read()
        if len(cmdout) == 0:
            util.pread2(["modprobe", "nbd", "nbds_max=%s" % NBDS_MAX])
        else:
            util.pread2(["rmmod", "nbd"])
            util.pread2(["modprobe", "nbd", "nbds_max=%s" % NBDS_MAX])

    def _unload_nbd(self):
        """
        :return:
        """
        util.SMlog("rbdsr_common.SR._unload_nbd: Unload nbd kernel driver")
        cmdout = os.popen("lsmod | grep nbd").read()
        if len(cmdout) != 0:
            util.pread2(["rmmod", "nbd"])

    def _updateStats(self, sr_uuid, virtAllocDelta):
        util.SMlog("RBDSR._updateStats: sr_uuid=%s, virtAllocDelta=%s" % (sr_uuid, virtAllocDelta))
        valloc = int(self.session.xenapi.SR.get_virtual_allocation(self.sr_ref))
        self.virtual_allocation = valloc + int(virtAllocDelta)
        self.session.xenapi.SR.set_virtual_allocation(self.sr_ref, str(self.virtual_allocation))
        self.session.xenapi.SR.set_physical_utilisation(self.sr_ref, str(self.RBDPOOLs[sr_uuid]['stats']['bytes_used']))

    def vdi(self, vdi_uuid):
        """
        Create a VDI class
        :param vdi_uuid:
        :return:
        """
        util.SMlog("CSR.SR.vdi vdi_uuid = %s" % vdi_uuid)

        if vdi_uuid not in self.vdis:
            self.vdis[vdi_uuid] = CVDI(self, vdi_uuid)
        return self.vdis[vdi_uuid]

    def scan(self, sr_uuid):
        """
        :param uuid:
        :return:
        """
        util.SMlog("CSR.SR.scan: sr_uuid=%s" % sr_uuid)

        allocated_bytes = 0

        sr_sm_config = self.session.xenapi.SR.get_sm_config(self.sr_ref)
        if "attaching" in sr_sm_config:
            update_existing = True
            self.session.xenapi.SR.remove_from_sm_config(self.sr_ref, "attaching")
        else:
            if self.VDI_UPDATE_EXISTING:
                update_existing = True
            else:
                update_existing = False

        rbds_list = self._get_rbds_list("%s%s" % (RBDPOOL_PREFIX, sr_uuid))
        meta_sources = {}
        snaps_of = {}
        vdi_info = {':uuid':''}

        existing_vdis_refs = self.session.xenapi.SR.get_VDIs(self.sr_ref)
        existing_vdis_uuids = {}
        for existing_vdi_ref in existing_vdis_refs:
            existing_vdis_uuids[(self.session.xenapi.VDI.get_uuid(existing_vdi_ref))] = existing_vdi_ref

        for rbd in rbds_list:
            if rbd['image'].startswith(self.VDI_PREFIX):
                regex = re.compile(self.VDI_PREFIX)
                rbd_vdi_uuid = regex.sub('', rbd['image'])
                if 'snapshot' in rbd:
                    if rbd['snapshot'].startswith(self.SNAPSHOT_PREFIX):
                        regex = re.compile(self.SNAPSHOT_PREFIX)
                        rbd_snap_uuid = regex.sub('', rbd['snapshot'])
                        meta_source = rbd_vdi_uuid
                        rbd_vdi_uuid = rbd_snap_uuid
                        tag_prefix = "%s:" % rbd_snap_uuid
                    else:
                        break
                else:
                    meta_source = rbd_vdi_uuid
                    tag_prefix = ':'
                    allocated_bytes += rbd['size']

                if rbd_vdi_uuid not in existing_vdis_uuids or update_existing:
                    self.vdis[rbd_vdi_uuid] = self.vdi(rbd_vdi_uuid)
                    if vdi_info[':uuid'] != meta_source:
                        if self.USE_RBD_META:
                            vdi_info = RBDMetadataHandler(self, meta_source).retrieveMetadata()
                        else:
                            # TODO: Implement handler for MGT image if we dont use RBD metadata
                            vdi_info = {}

                    for key in filter(lambda x: x.startswith(tag_prefix), vdi_info.keys()):
                        tag = key[len(tag_prefix):]
                        if tag == NAME_LABEL_TAG:
                            self.vdis[rbd_vdi_uuid].label = vdi_info[key]
                        elif tag == NAME_DESCRIPTION_TAG:
                            self.vdis[rbd_vdi_uuid].description = vdi_info[key]
                        elif tag == IS_A_SNAPSHOT_TAG:
                            self.vdis[rbd_vdi_uuid].is_a_snapshot = bool(int(vdi_info[key]))
                        elif tag == SNAPSHOT_TIME_TAG:
                            self.vdis[rbd_vdi_uuid].snapshot_time = str(vdi_info[key])
                        elif tag == TYPE_TAG:
                            self.vdis[rbd_vdi_uuid].type = vdi_info[key]
                        elif tag == VDI_TYPE_TAG:
                            self.vdis[rbd_vdi_uuid].vdi_type = vdi_info[key]
                        elif tag == READ_ONLY_TAG:
                            self.vdis[rbd_vdi_uuid].read_only = bool(int(vdi_info[key]))
                        elif tag == MANAGED_TAG:
                            self.vdis[rbd_vdi_uuid].managed = bool(int(vdi_info[key]))
                        elif tag == 'shareable':
                            self.vdis[rbd_vdi_uuid].shareable = bool(int(vdi_info[key]))
                        #elif tag == METADATA_OF_POOL_TAG:
                        #    self.vdis[rbd_vdi_uuid].metadata_of_pool = vdi_info[key]
                        elif tag == 'sm_config':
                            self.vdis[rbd_vdi_uuid].sm_config = json.loads(vdi_info[key])
                        elif tag == SNAPSHOT_OF_TAG:
                            snaps_of[rbd_vdi_uuid] = vdi_info[key]
                        self.vdis[rbd_vdi_uuid].size = rbd['size']

        self.virtual_allocation = allocated_bytes
        self.physical_size = self.RBDPOOLs[sr_uuid]['stats']['max_avail'] + self.RBDPOOLs[sr_uuid]['stats']['bytes_used']
        self.physical_utilisation = self.RBDPOOLs[sr_uuid]['stats']['bytes_used']
        super(CSR, self).scan(sr_uuid)

        for snap_uuid, vdi_uuid in snaps_of.iteritems():
            self.session.xenapi.VDI.set_snapshot_of(self.session.xenapi.VDI.get_by_uuid(snap_uuid), self.session.xenapi.VDI.get_by_uuid(vdi_uuid))

    def load(self, sr_uuid):
        """
        :param sr_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.SR.load: sr_uuid = %s" % sr_uuid)

        self.RBDPOOLs = self._get_srsdict()

        if self.uuid != '' and self.uuid not in self.RBDPOOLs:
            raise xs_errors.XenError('SRUnavailable',opterr='no pool with uuid: %s' % sr_uuid)

        # Fallback to kernel mode if mode is fuse with different than admin
        # => --name arg not compatible with fuse mode
        if self.CEPH_USER != "client.admin" and self.mode == "fuse":
            self.mode = "kernel"

        if self.mode == "kernel":
             self.DEV_ROOT = "%s/%s" % (RBD_PREFIX, self.CEPH_POOL_NAME)
        elif self.mode == "fuse":
             self.DEV_ROOT = "%s/%s" % (FUSE_PREFIX, self.CEPH_POOL_NAME)
        elif self.mode == "nbd":
             self.DEV_ROOT = "%s/%s" % (NBD_PREFIX, self.CEPH_POOL_NAME)

        self.SR_ROOT = "%s/%s" % (SR_PREFIX, sr_uuid)
        self.DM_ROOT = "%s/%s-" % (DM_PREFIX, self.CEPH_POOL_NAME)

        self.mdpath = self._get_path(MDVOLUME_NAME)

        super(CSR, self).load(sr_uuid)

    def probe(self):
        """
        :return:
        """
        util.SMlog("rbdsr_common..SR._srlist_toxml")

        rbdpools = self._get_srsdict()
        dom = xml.dom.minidom.Document()
        element = dom.createElement("SRlist")
        dom.appendChild(element)

        for sr_uuid in rbdpools.keys():
            entry = dom.createElement('SR')
            element.appendChild(entry)

            subentry = dom.createElement("UUID")
            entry.appendChild(subentry)
            textnode = dom.createTextNode(sr_uuid)
            subentry.appendChild(textnode)

            subentry = dom.createElement("PoolName")
            entry.appendChild(subentry)
            textnode = dom.createTextNode(rbdpools[sr_uuid]["name"])
            subentry.appendChild(textnode)

            subentry = dom.createElement("Size")
            entry.appendChild(subentry)
            size = str(rbdpools[sr_uuid]["stats"]["max_avail"] + rbdpools[sr_uuid]["stats"]["bytes_used"])
            textnode = dom.createTextNode(size)
            subentry.appendChild(textnode)

            subentry = dom.createElement("BytesUses")
            entry.appendChild(subentry)
            bytesused = str(rbdpools[sr_uuid]["stats"]["bytes_used"])
            textnode = dom.createTextNode(bytesused)
            subentry.appendChild(textnode)

            subentry = dom.createElement("Objects")
            entry.appendChild(subentry)
            objects = str(rbdpools[sr_uuid]["stats"]["objects"])
            textnode = dom.createTextNode(objects)
            subentry.appendChild(textnode)

        return dom.toprettyxml()

    def attach(self, sr_uuid):
        """
        :param sr_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.SR.attach: sr_uuid = %s using %s mode" % (sr_uuid, self.mode))

        sr_sm_config = self.session.xenapi.SR.get_sm_config(self.sr_ref)
        if "dev_instances" in sr_sm_config:
            self.session.xenapi.SR.remove_from_sm_config(self.sr_ref, "dev_instances")
        if "attaching" not in sr_sm_config:
            self.session.xenapi.SR.add_to_sm_config(self.sr_ref, 'attaching', 'true')

        util.pread2(["mkdir", "-p", self.SR_ROOT])

        if self.mode == "kernel":
            pass
        elif self.mode == "fuse":
            util.pread2(["mkdir", "-p", self.DEV_ROOT])
            util.pread2(["rbd-fuse", "-p", self.CEPH_POOL_NAME, self.DEV_ROOT])
            util.pread2(["ln -s", "-p", self.DEV_ROOT, self.SR_ROOT])
        elif self.mode == "nbd":
            self._check_and_load_nbd()
            util.pread2(["mkdir", "-p", self.DEV_ROOT])

        if not self.USE_RBD_META:
            if self._if_rbd_exist(MDVOLUME_NAME):
                self._map_metadata_volume()
            else:
                self._create_metadata_volume()
                self._map_metadata_volume()

    def detach(self, sr_uuid):
        """
        :param sr_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.SR.detach: sr_uuid = %s using %s mode" % (sr_uuid, self.mode))

        sr_sm_config = self.session.xenapi.SR.get_sm_config(self.sr_ref)
        if 'dev_instances' in sr_sm_config:
            self.session.xenapi.SR.remove_from_sm_config(self.sr_ref, 'dev_instances')

        if self.mode == 'kernel':
            util.pread2(['rm', '-rf', self.SR_ROOT])
        elif self.mode == 'fuse':
            util.pread2(['unlink', self.SR_ROOT])
            util.pread2(['fusermount', ''-u'', self.DEV_ROOT])
            util.pread2(['rm', '-rf', self.DEV_ROOT])
        elif self.mode == 'nbd':
            util.pread2(['rm', '-rf', self.SR_ROOT])
            util.pread2(['rm', '-rf', self.DEV_ROOT])
            self._unload_nbd()

        if not self.USE_RBD_META:
            self._unmap_metadata_volume()

    def create(self, sr_uuid, size):
        """
        :param sr_uuid:
        :param size:
        :return:
        """
        util.SMlog("rbdsr_common.SR.create: sr_uuid=%s, size=%s" % (sr_uuid, size))

        super(CSR, self).create(sr_uuid, size)

    def delete(self, sr_uuid):
        """
        :param sr_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.SR.delete: sr_uuid=%s" % sr_uuid)

        super(CSR, self).delete(sr_uuid)

    def update(self, sr_uuid):
        """
        :param sr_uuid:
        :return:
        """
        # TODO: Test the method
        util.SMlog("rbdsr_vhd.RBDVHDSR.update: sr_uuid=%s" % sr_uuid)

        self.updateStats(sr_uuid, 0)

        if self.USE_RBD_META:
            #RBDMetadataHandler(self.sr, vdi_uuid).updateMetadata(vdi_info)
            pass
        else:
            # synch name_label in metadata with XAPI
            # update_map = {}
            #update_map = {METADATA_UPDATE_OBJECT_TYPE_TAG: \
            #                  METADATA_OBJECT_TYPE_SR,
            #              NAME_LABEL_TAG: util.to_plain_string( \
            #                  self.session.xenapi.SR.get_name_label(self.sr_ref)),
            #              NAME_DESCRIPTION_TAG: util.to_plain_string( \
            #                  self.session.xenapi.SR.get_name_description(self.sr_ref))
            #              }
            #MetadataHandler(self.mdpath).updateMetadata(update_map)
            pass

    def updateStats(self, sr_uuid, virtAllocDelta):
        """
        :param sr_uuid:
        :param virtAllocDelta:
        :return:
        """
        util.SMlog("rbdsr_common._updateStats: sr_uuid = %s, virtAllocDelta = %s" % (sr_uuid, virtAllocDelta))

        valloc = int(self.session.xenapi.SR.get_virtual_allocation(self.sr_ref))
        self.virtual_allocation = valloc + int(virtAllocDelta)
        self.session.xenapi.SR.set_virtual_allocation(self.sr_ref, str(self.virtual_allocation))
        self.session.xenapi.SR.set_physical_utilisation(self.sr_ref, str(self.RBDPOOLs[sr_uuid]['stats']['bytes_used']))

    def isSpaceAvailable(self, size):
        """
        :param size:
        :return:
        """
        util.SMlog("rbdsr_common.SR.isSpaceAvailable: size=%s" % size)
        sr_free_space = self.RBDPOOLs[self.uuid]['stats']['max_avail']
        if size > sr_free_space:
            return False
        else:
            return True


class CVDI(VDI.VDI):

    def __init__(self, sr_ref, vdi_uuid):
        """
        :param sr_ref:
        :param vdi_uuid:
        """
        util.SMlog("rbdsr_common.VDI.__init__: vdi_uuid=%s" % vdi_uuid)

        super(CVDI, self).__init__(sr_ref, vdi_uuid)

        self.sr = sr_ref
        self.vdi_type = sr_ref.vdi_type
        self.uuid = vdi_uuid
        self.location = self.uuid
        self.vdi_type = self.sr.vdi_type
        self.read_only = False
        self.shareable = False
        self.is_a_snapshot = False
        self.hidden = False
        self.sm_config = {}
        self.path = self.sr._get_path(vdi_uuid)
        self.rbd_info = self._get_rbd_info(vdi_uuid)

        try:
            vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
            self.exist = True
        except Exception:
            self.exist = False

    def _rename_rbd(self, orig_uuid, new_uuid):
        """
        :param orig_uuid:
        :param new_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.VDI._rename_rbd: orig_uuid=%s, new_uuid=%s" % (orig_uuid, new_uuid))
        orig_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, self.sr.VDI_PREFIX, orig_uuid)
        new_name = "%s/%s%s" % (self.sr.CEPH_POOL_NAME, self.sr.VDI_PREFIX, new_uuid)
        util.pread2(["rbd", "mv", orig_name, new_name, "--name", self.sr.CEPH_USER])

    def _map_rbd(self, vdi_uuid, size, host_uuid=None, read_only=None, dmmode='None', devlinks=True, norefcount=False):
        """
        :param vdi_uuid:
        :param size:
        :param host_uuid:
        :param read_only:
        :param dmmode:
        :param devlinks:
        :param norefcount:
        :return:
        """
        util.SMlog("rbdsr_common.VDI._map_rbd: vdi_uuid = %s, size = %s, host_uuid = %s, read_only = %s, dmmode = %s, \
                   devlinks = %s, norefcount = %s" % (vdi_uuid, size, host_uuid, read_only, dmmode, devlinks,
                                                      norefcount))

        _vdi_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)
        _dev_name = "%s/%s" % (self.sr.DEV_ROOT, _vdi_name)
        _dmdev_name = "%s%s" % (self.sr.DM_ROOT, _vdi_name)
        _dm_name = "%s-%s" % (self.sr.CEPH_POOL_NAME, _vdi_name)
        vdi_name = "%s" % vdi_uuid
        dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if read_only is None:
            read_only = self.session.xenapi.VDI.get_read_only(vdi_ref)
        sharable = self.session.xenapi.VDI.get_sharable(vdi_ref)

        args = {"mode": self.sr.mode, "vdi_uuid": vdi_uuid,
                "vdi_name": vdi_name,  "dev_name": dev_name,
                "_vdi_name": _vdi_name,  "_dev_name": _dev_name,
                "_dmdev_name": _dmdev_name, "_dm_name": _dm_name,
                "CEPH_POOL_NAME": self.sr.CEPH_POOL_NAME,
                "NBDS_MAX": str(NBDS_MAX),
                "CEPH_USER": self.sr.CEPH_USER, "sharable": str(sharable),
                "read_only": str(read_only), "userbdmeta": str(self.sr.USE_RBD_META),
                "dmmode": dmmode,
                "size": str(size)}

        if 'dmp-parent' in sm_config:
            args['_dmbasedev_name'] = "%s%s" % (self.sr.DM_ROOT, "%s%s-base" % (self.sr.VDI_PREFIX, sm_config['dmp-parent']))

        def __call_plugin__():
            if not norefcount:
                self.sr._allocate_dev_instance(self.sr.uuid, vdi_uuid, host_uuid)

            if self.sr._get_instance_ref_count(self.sr.uuid, vdi_uuid, host_uuid) == 1 or norefcount:
                try:
                    if devlinks:
                        self._call_plugin('map', args, 'ceph_plugin', host_uuid)
                    else:
                        self._call_plugin('_map', args, 'ceph_plugin', host_uuid)

                    if 'attached' not in sm_config and self.exist:
                        self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'attached', 'true')

                except Exception, e:
                   if not norefcount:
                       self.sr._free_dev_instance(self.sr.uuid, vdi_uuid, host_uuid)
                   raise xs_errors.XenError('VDIUnavailable', opterr='Failed to map RBD sr_uuid=%s, vdi_uuid=%s, \
                                            host_uuid=%s (%s)' % (self.sr.uuid, vdi_uuid, host_uuid, str(e)))

        if host_uuid is None:
            if filter(lambda x: x.startswith('host_'), sm_config.keys()):
                for key in filter(lambda x: x.startswith('host_'), sm_config.keys()):
                    host_ref = key[len('host_'):]
                    host_uuid = self.session.xenapi.host.get_uuid(host_ref)
                    __call_plugin__()
            else:
                host_uuid = inventory.get_localhost_uuid()
                __call_plugin__()
        else:
            __call_plugin__()

        if self.exist:
            if 'dmmode' in sm_config:
                self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'dmmode')
            self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'dmmode', dmmode)

    def _unmap_rbd(self, vdi_uuid, size, host_uuid=None, devlinks=True, norefcount=False):

        """
        :param vdi_uuid:
        :param size:
        :param host_uuid:
        :param devlinks:
        :param norefcount:
        :return:
        """

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if 'dmmode' in sm_config:
            dmmode = sm_config['dmmode']
        else:
            dmmode = 'None'

        util.SMlog("rbdsr_common.VDI._unmap_rbd: vdi_uuid = %s, size = %s, host_uuid = %s, dmmode = %s, \
                   devlinks = %s, norefcount = %s" % (vdi_uuid, size, host_uuid, dmmode, devlinks,
                                                      norefcount))

        _vdi_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)
        _dev_name = "%s/%s" % (self.sr.DEV_ROOT, _vdi_name)
        _dmdev_name = "%s%s" % (self.sr.DM_ROOT, _vdi_name)
        _dm_name = "%s-%s" % (self.sr.CEPH_POOL_NAME, _vdi_name)
        vdi_name = "%s" % vdi_uuid
        dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)

        args = {"mode": self.sr.mode, "vdi_uuid": vdi_uuid,
                "vdi_name": vdi_name,  "dev_name": dev_name,
                "_vdi_name": _vdi_name,  "_dev_name": _dev_name,
                "_dmdev_name": _dmdev_name, "_dm_name": _dm_name,
                "CEPH_POOL_NAME": self.sr.CEPH_POOL_NAME,
                "NBDS_MAX": str(NBDS_MAX),
                "CEPH_USER": self.sr.CEPH_USER,
                "userbdmeta": str(self.sr.USE_RBD_META),
                "dmmode": dmmode,
                "size": str(size)}

        def __call_plugin__():
            if self.sr._get_instance_ref_count(self.sr.uuid, vdi_uuid, host_uuid) == 1 or norefcount:
                try:
                    if devlinks:
                        self._call_plugin('unmap',args, 'ceph_plugin', host_uuid)
                    else:
                        self._call_plugin('_unmap', args, 'ceph_plugin', host_uuid)

                    if 'attached' in sm_config and self.exist:
                        self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'attached')

                except Exception, e:
                    raise xs_errors.XenError('VDIUnavailable', opterr='Failed to unmap RBD for %s (%s)' % (vdi_uuid, str(e)))

            if not norefcount:
                self.sr._free_dev_instance(self.sr.uuid, vdi_uuid, host_uuid)

        if host_uuid is None:
            if filter(lambda x: x.startswith('host_'), sm_config.keys()):
                for key in filter(lambda x: x.startswith('host_'), sm_config.keys()):
                    host_ref = key[len('host_'):]
                    host_uuid = self.session.xenapi.host.get_uuid(host_ref)
                    __call_plugin__()
            else:
                host_uuid = inventory.get_localhost_uuid()
                __call_plugin__()
        else:
            __call_plugin__()

        if self.exist:
            if 'dmmode' in sm_config:
                self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'dmmode')

    def _map_rbd_snap(self, vdi_uuid, snap_uuid, size, host_uuid=None, read_only=None, dmmode='None', devlinks=True,
                      norefcount=False):
        """
        :param vdi_uuid:
        :param snap_uuid:
        :param size:
        :param host_uuid:
        :param read_only:
        :param dmmode:
        :param devlinks:
        :param norefcount:
        :return:
        """
        util.SMlog("rbdsr_common.VDI._map_rbd_snap: vdi_uuid = %s, snap_uuis = %s, size = %s, host_uuid = %s, \
                   read_only = %s, dmmode = %s, devlinks = %s, refcounter = %s"
                   % (vdi_uuid, snap_uuid, size, host_uuid, read_only, dmmode, devlinks, norefcount))
        pass

    def _unmap_rbd_snap(self, vdi_uuid, snap_uuid, size, host_uuid=None, read_only=None, dmmode='None', devlinks=True,
                        norefcount=False):
        """
        :param vdi_uuid:
        :param snap_uuid:
        :param size:
        :param host_uuid:
        :param read_only:
        :param dmmode:
        :param devlinks:
        :param norefcount:
        :return:
        """
        util.SMlog("rbdsr_common.VDI._unmap_rbd_snap: vdi_uuid = %s, snap_uuis = %s, size = %s, host_uuid = %s, \
                   read_only = %s, dmmode = %s, devlinks = %s, refcounter = %s"
                   % (vdi_uuid, snap_uuid, size, host_uuid, read_only, dmmode, devlinks, norefcount))
        pass

    def _call_plugin(self, op, args, plugin, host_uuid):
        """
        :param op:
        :param args:
        :param host_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.VDI._call_plugin: Calling plugin '%s' on host with id '%s' for op '%s'"
                   % (plugin, host_uuid, op))

        vdi_uuid = args['vdi_uuid']
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)

        host_ref = self.session.xenapi.host.get_by_uuid(host_uuid)
        args['dev'] = str(self.sr._get_dev_instance(self.sr.uuid, vdi_uuid, host_uuid))
        if args['dev'] == 'None':
            args['dev'] = '2'
        util.SMlog("Calling '%s' of plugin '%s' on localhost %s" % (op, plugin, host_ref))
        if not self.session.xenapi.host.call_plugin(host_ref, plugin, op, args):
            # Failed to execute op for vdi
            raise util.SMException("Failed to execute '%s' on host with id '%s' VDI %s" % (op, host_uuid, vdi_uuid))

    def _delete_rbd(self, vdi_uuid):
        """
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_commone.VDI._delete_rbd: vdi_uuid=%s" % vdi_uuid)
        # TODO: Checked
        vdi_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)
        fuse_vdi_path = "%s/%s%s" % (self.sr.DEV_ROOT, self.sr.VDI_PREFIX, vdi_uuid)
        if self.sr.mode == "kernel":
            util.pread2(["rbd", "rm", vdi_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        elif self.sr.mode == "fuse":
            util.pread2(["rm", "-f", fuse_vdi_path])
        elif self.sr.mode == "nbd":
            util.pread2(["rbd", "rm", vdi_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])

    def _delete_snap(self, vdi_uuid, snap_uuid):
        """
        :param vdi_uuid:
        :param snap_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.VDI._delete_snap: vdi_uuid=%s, snap_uuid=%s" % (vdi_uuid, snap_uuid))

        vdi_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)
        snapshot_name = "%s@%s%s" % (vdi_name, self.sr.SNAPSHOT_PREFIX, snap_uuid)
        short_snap_name = "%s%s" % (self.sr.SNAPSHOT_PREFIX, snap_uuid)
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if 'attached' in sm_config and 'paused' not in sm_config:
            if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                raise util.SMException("failed to pause VDI %s" % vdi_uuid)
            self._unmap_rbd(vdi_uuid, self.size, devlinks=False, norefcount=True)
        #---
        util.pread2(["rbd", "snap", "unprotect", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name",
                     self.sr.CEPH_USER])
        util.pread2(["rbd", "snap", "rm", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name",
                     self.sr.CEPH_USER])
        if self.sr.USE_RBD_META:
            #util.pread2(["rbd", "image-meta", "remove", vdi_name, short_snap_name, "--pool", self.sr.CEPH_POOL_NAME,
            #             "--name", self.sr.CEPH_USER])
            # TODO: Remove snap vdi_info metadata from image-meta of vdi
            pass
        else:
            pass
        #---
        if 'attached' in sm_config and 'paused' not in sm_config:
            self._map_rbd(vdi_uuid, self.size, devlinks=False, norefcount=True)
            blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None)

    def _get_rbd_info(self, vdi_uuid):
        """
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.VDI._get_rbd_info: vdi_uuid = %s" % vdi_uuid)

        rbds_list = self.sr._get_rbds_list("%s%s" % (self.sr.RBDPOOL_PREFIX, self.sr.uuid))

        retval = None

        for rbd_info in rbds_list:
            if "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid) == rbd_info['image']:
                # vdi is and rbd image
                retval = ('image', rbd_info)
                break
            elif 'snapshot' in rbd_info:
                if "%s%s" % (self.sr.SNAPSHOT_PREFIX, vdi_uuid) == rbd_info['snapshot']:
                    # vdi is and rbd snapshot
                    retval = ('snapshot', rbd_info)
                    break

        return retval

    def _disable_rbd_caching(self, userbdmeta, ceph_pool_name, _vdi_name):
        if userbdmeta == 'True':
            util.pread2(['rbd', 'image-meta', 'set', "%s/%s" % (ceph_pool_name, _vdi_name), 'conf_rbd_cache', 'false'])
        if userbdmeta == 'False':
            if not os.path.isfile("/etc/ceph/ceph.conf.nocaching"):
                os.system("printf \"[client]\\n\\trbd cache = false\\n\\n\" > /etc/ceph/ceph.conf.nocaching")
                os.system("cat /etc/ceph/ceph.conf >> /etc/ceph/ceph.conf.nocaching")

    def load(self, vdi_uuid):
        """
        :param vdi_uuid:
        """
        util.SMlog("rbdsr_common.VDI.load: vdi_uuid=%s" % vdi_uuid)

        super(CVDI, self).load(vdi_uuid)

    def delete(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.VDI.delete: sr_uuid = %s, vdi_uuid = %s" % (sr_uuid, vdi_uuid))
        # TODO: Checked
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        if self.rbd_info is not None:
            if self.rbd_info[0] == 'snapshot':
                self._delete_snap(self.session.xenapi.VDI.get_uuid(self.session.xenapi.VDI.get_snapshot_of(vdi_ref)),
                                  vdi_uuid)
            else:
                self._delete_rbd(vdi_uuid)

            self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))
            self.sr._updateStats(self.sr.uuid, -self.size)
            self._db_forget()

    def create(self, sr_uuid, vdi_uuid, size):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param size:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_common.VDI.create: sr_uuid = %s, vdi_uuid = %s, size = %s" % (sr_uuid, vdi_uuid, size))

        if self.rbd_info is not None:
            raise xs_errors.XenError('VDIExists')

        if not self.sr.isSpaceAvailable(size):
            util.SMlog('rbdsr_common.VDI.create: vdi size is too big: ' + \
                    '(vdi size: %d, sr free space size: %d)' % (size, self.sr.RBDPOOLs[sr_uuid]['stats']['max_avail']))
            raise xs_errors.XenError('VDISize', opterr='vdi size is too big: vdi size: %d, sr free space size: %d'
                                                       % (size, self.sr.RBDPOOLs[sr_uuid]['stats']['max_avail']))
        ####
        if vdi_uuid == 'MGT':
            rbd_name = vdi_uuid
        else:
            rbd_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)

        if size < OBJECT_SIZE_B:
            size_M = OBJECT_SIZE_B // 1024 // 1024
        else:
            size_M = size // 1024 // 1024

        util.SMlog("rbdsr_common.VDI.create: rbd_name = %s, size_M = %s, format = %s"
                   % (rbd_name, size_M, self.sr.IMAGE_FORMAT_DEFAULT))
        ###
        try:
            util.pread2(["rbd", "create", rbd_name, "--size", str(size_M), "--object-size",
                         str(OBJECT_SIZE_B), "--image-format", str(self.sr.IMAGE_FORMAT_DEFAULT), "--pool",
                         self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        except Exception, e:
            util.SMlog("rbdsr_common.VDI.create_rbd: Failed to create: rbd_name = %s, size_M = %s, format = %s"
                       % (rbd_name, size_M, self.sr.IMAGE_FORMAT_DEFAULT))
            raise xs_errors.XenError('VDICreate', opterr='Failed to create Volume: %s' % str(e))
        ###
        self.size = size
        self.utilisation = self.size
        self.sm_config["vdi_type"] = self.vdi_type
        sm_config_dump = json.dumps(self.sm_config)

        self.sr.updateStats(self.sr.uuid, self.size)
        self.ref = self._db_introduce()
        self.rbd_info = self._get_rbd_info(vdi_uuid)
        ###
        tag_prefix = ':'
        vdi_info = { "%s%s" % (tag_prefix, UUID_TAG): self.uuid,
                     "%s%s" % (tag_prefix, NAME_LABEL_TAG): util.to_plain_string(self.label),
                     "%s%s" % (tag_prefix, NAME_DESCRIPTION_TAG): util.to_plain_string(self.description),
                     "%s%s" % (tag_prefix, IS_A_SNAPSHOT_TAG): int(self.is_a_snapshot),
                     "%s%s" % (tag_prefix, SNAPSHOT_OF_TAG): self.session.xenapi.VDI.get_uuid(self.snapshot_of) if self.is_a_snapshot else '',
                     "%s%s" % (tag_prefix, SNAPSHOT_TIME_TAG): '',
                     "%s%s" % (tag_prefix, TYPE_TAG): self.ty,
                     "%s%s" % (tag_prefix, VDI_TYPE_TAG): self.vdi_type,
                     "%s%s" % (tag_prefix, READ_ONLY_TAG): int(self.read_only),
                     "%s%s" % (tag_prefix, MANAGED_TAG): int(self.managed),
                     "%s%s" % (tag_prefix, METADATA_OF_POOL_TAG): '',
                     "%s%s" % (tag_prefix, 'sm_config'): sm_config_dump
                }

        if self.sr.USE_RBD_META:
            RBDMetadataHandler(self.sr, vdi_uuid).updateMetadata(vdi_info)
        else:
            # TODO: Implement handler for MGT image if we dont use RBD metadata
            # MetadataHandler(self.sr.mdpath).addVdi(vdi_info)
            pass

        return self.get_params()

    def update(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.VDI.update: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
        # TODO: Checked

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        new_sm_config = {}
        for key, val in sm_config.iteritems():
            if key in ["vdi_type", "vhd-parent", "dmp-parent"]:
                new_sm_config[key] = val
        sm_config_dump = json.dumps(new_sm_config)

        name_label = self.session.xenapi.VDI.get_name_label(vdi_ref)
        name_description = self.session.xenapi.VDI.get_name_description(vdi_ref)
        is_a_snapshot = int(self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref))
        snapshot_of = self.session.xenapi.VDI.get_uuid(self.session.xenapi.VDI.get_snapshot_of(vdi_ref))\
            if is_a_snapshot else ''
        snapshot_time = self.session.xenapi.VDI.get_snapshot_time(vdi_ref) if is_a_snapshot else ''
        read_only = int(self.session.xenapi.VDI.get_read_only(vdi_ref))
        managed = int(self.session.xenapi.VDI.get_managed(vdi_ref))
        shareable = int(self.session.xenapi.VDI.get_sharable(vdi_ref))
        #metadata_of_pool = self.session.xenapi.VDI.get_metadata_of_pool(vdi_ref) # TODO We should use uuid as in snapshot_of (not ref) but what if ref is null

        if self.rbd_info is not None:

            if self.rbd_info[0] == 'image':
                tag_prefix = ':'
                uuid_to_update = vdi_uuid
            else:
                tag_prefix = '%s:' % self.uuid
                uuid_to_update = self.rbd_info[1]['image']

            vdi_info = {'%s%s' % (tag_prefix, UUID_TAG): self.uuid,
                        '%s%s' % (tag_prefix, NAME_LABEL_TAG): name_label,
                        '%s%s' % (tag_prefix, NAME_DESCRIPTION_TAG): name_description,
                        '%s%s' % (tag_prefix, IS_A_SNAPSHOT_TAG): is_a_snapshot,
                        '%s%s' % (tag_prefix, SNAPSHOT_OF_TAG): snapshot_of,
                        '%s%s' % (tag_prefix, SNAPSHOT_TIME_TAG): snapshot_time,
                        '%s%s' % (tag_prefix, TYPE_TAG): self.ty,
                        '%s%s' % (tag_prefix, VDI_TYPE_TAG): self.vdi_type,
                        '%s%s' % (tag_prefix, READ_ONLY_TAG): read_only,
                        '%s%s' % (tag_prefix, MANAGED_TAG): managed,
                        #'%s%s' % (tag_prefix, METADATA_OF_POOL_TAG): metadata_of_pool,
                        '%s%s' % (tag_prefix, 'shareable'): shareable,
                        '%s%s' % (tag_prefix, 'sm_config'): sm_config_dump
                        }

            if self.sr.USE_RBD_META:
                RBDMetadataHandler(self.sr, uuid_to_update).updateMetadata(vdi_info)
            else:
                # TODO: Implement handler for MGT image if we dont use RBD metadata
                #Synch the name_label of this VDI on storage with the name_label in XAPI
                #vdi_ref = self.session.xenapi.VDI.get_by_uuid(self.uuid)
                #update_map = {}
                #update_map[METADATA_UPDATE_OBJECT_TYPE_TAG] = \
                #    METADATA_OBJECT_TYPE_VDI
                #update_map[UUID_TAG] = self.uuid
                #update_map[NAME_LABEL_TAG] = util.to_plain_string(\
                #    self.session.xenapi.VDI.get_name_label(vdi_ref))
                #update_map[NAME_DESCRIPTION_TAG] = util.to_plain_string(\
                #    self.session.xenapi.VDI.get_name_description(vdi_ref))
                #update_map[SNAPSHOT_TIME_TAG] = \
                #    self.session.xenapi.VDI.get_snapshot_time(vdi_ref)
                #update_map[METADATA_OF_POOL_TAG] = \
                #    self.session.xenapi.VDI.get_metadata_of_pool(vdi_ref)
                #MetadataHandler(self.sr.mdpath).updateMetadata(update_map)
                pass
        else:
            raise xs_errors.XenError('VDIUnavailable',
                                     opterr='Could not find image %s in pool %s' % (vdi_uuid, self.sr.uuid))

    def introduce(self, sr_uuid, vdi_uuid):
        """
        Explicitly introduce a particular VDI.
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.CVDI.introduce: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
        # TODO: Checked
        # TODO: What if introduce snapshot before image? We can not introduce snap befor image.

        need_update = False
        try:
            vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
            raise xs_errors.XenError('VDIExists')
        except:
            if self.rbd_info is not None:
                if self.rbd_info[0] == 'snapshot':
                    uuid_to_retrieve_meta = self.rbd_info[1]['image']
                    tag_prefix = "%s:" % vdi_uuid
                else:
                    uuid_to_retrieve_meta = vdi_uuid
                    tag_prefix = ':'

                if self.sr.USE_RBD_META:
                    vdi_info = RBDMetadataHandler(self.sr, uuid_to_retrieve_meta).retrieveMetadata()
                else:
                    # TODO: Implement handler for MGT image if we dont use RBD metadata
                    # vdi_info =
                    pass

                if self.label == '':
                    self.label = vdi_info["%s%s" % (tag_prefix, NAME_LABEL_TAG)] if "%s%s" % \
                                                                                    (tag_prefix, NAME_LABEL_TAG) \
                                                                                    in vdi_info else ''
                else:
                    need_update = True

                if self.description == '':
                    self.description = vdi_info["%s%s" % (tag_prefix, NAME_DESCRIPTION_TAG)] \
                        if "%s%s" % (tag_prefix, NAME_DESCRIPTION_TAG) in vdi_info else ''
                else:
                    need_update = True

                if "%s%s" % (tag_prefix, IS_A_SNAPSHOT_TAG) in vdi_info:
                    self.is_a_snapshot = bool(int(vdi_info["%s%s" % (tag_prefix, IS_A_SNAPSHOT_TAG)]))
                else:
                    self.is_a_snapshot = False

                if "%s%s" % (tag_prefix, SNAPSHOT_OF_TAG) in vdi_info:
                    self.snapshot_of = self.session.xenapi.VDI.get_by_uuid(vdi_info["%s%s" % (tag_prefix,
                                                                                               SNAPSHOT_OF_TAG)])

                if "%s%s" % (tag_prefix, SNAPSHOT_TIME_TAG) in vdi_info:
                    self.snapshot_time = str(vdi_info["%s%s" % (tag_prefix, SNAPSHOT_TIME_TAG)])

                if "%s%s" % (tag_prefix, TYPE_TAG) in vdi_info:
                    self.ty = vdi_info["%s%s" % (tag_prefix, TYPE_TAG)]

                if "%s%s" % (tag_prefix, VDI_TYPE_TAG) in vdi_info:
                    self.vdi_type = vdi_info["%s%s" % (tag_prefix, VDI_TYPE_TAG)]

                if "%s%s" % (tag_prefix, READ_ONLY_TAG) in vdi_info:
                    self.read_only = bool(int(vdi_info["%s%s" % (tag_prefix, READ_ONLY_TAG)]))
                else:
                    self.read_only = False

                if "%s%s" % (tag_prefix, MANAGED_TAG) in vdi_info:
                    self.managed = bool(int(vdi_info["%s%s" % (tag_prefix, MANAGED_TAG)]))
                else:
                    self.managed = False

                if "%s%s" % (tag_prefix, 'shareable') in vdi_info:
                    self.shareable = bool(int(vdi_info["%s%s" % (tag_prefix, 'shareable')]))
                else:
                    self.shareable = False

                #if "%s%s" % (tag_prefix, METADATA_OF_POOL_TAG) in vdi_info:
                #    self.metadata_of_pool = vdi_info["%s%s" % (tag_prefix, METADATA_OF_POOL_TAG)]

                if "%s%s" % (tag_prefix, 'sm_config') in vdi_info:
                    self.sm_config = json.loads(vdi_info["%s%s" % (tag_prefix, 'sm_config')])

                self.sm_config["vdi_type"] = self.vdi_type

                self.size = self.rbd_info[1]["size"]
                self.utilisation = self.size

                self.ref = self._db_introduce()
                self.sr._updateStats(self.sr.uuid, self.size)

                if need_update:
                    self.update(sr_uuid, vdi_uuid)

                return self.get_params()
            else:
                raise xs_errors.XenError('VDIUnavailable', opterr='Could not find image %s in pool %s' %
                                                                  (vdi_uuid, sr_uuid))

    def attach(self, sr_uuid, vdi_uuid, dmmode='None'):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.CVDI.attach: sr_uuid=%s, vdi_uuid=%s, dmmode=%s" % (sr_uuid, vdi_uuid, dmmode))
        # TODO: Test the method

        if not hasattr(self,'xenstore_data'):
            self.xenstore_data = {}

        self.xenstore_data.update(scsiutil.update_XS_SCSIdata(self.uuid, scsiutil.gen_synthetic_page_data(self.uuid)))

        self.xenstore_data['storage-type']='rbd'
        self.xenstore_data['vdi-type']=self.vdi_type

        if self.rbd_info is not None:
            if self.rbd_info[0] == 'image':
                self._map_rbd(vdi_uuid, self.rbd_info[1]['size'], dmmode=dmmode)
            else:
                self._map_rbd_snap(self.rbd_info[1]['image'], vdi_uuid, self.rbd_info[1]['size'], dmmode=dmmode)

            return super(CVDI, self).attach(sr_uuid, vdi_uuid)
        else:
            raise xs_errors.XenError('VDIUnavailable', opterr='Could not find image %s in pool %s' %
                                                              (vdi_uuid, sr_uuid))

    def detach(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.CVDI.detach: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
        # TODO: Test the method

        if self.rbd_info is not None:
            if self.rbd_info[0] == 'image':
                self._unmap_rbd(vdi_uuid, self.rbd_info[1]['size'])
            else:
                self._unmap_rbd_snap(self.rbd_info[1]['image'], vdi_uuid, self.rbd_info[1]['size'])
        else:
            raise xs_errors.XenError('VDIUnavailable', opterr='Could not find image %s in pool %s' %
                                                              (vdi_uuid, sr_uuid))

    def clone(self, sr_uuid, snap_uuid):
        """
        :param sr_uuid:
        :param snap_uuid:
        :return:
        """
        # TODO: Implement
        util.SMlog("rbdsr_common.CVDI.clone: sr_uuid=%s, snap_uuid=%s" % (sr_uuid, snap_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(snap_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        is_a_snapshot = self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref)
        label = self.session.xenapi.VDI.get_name_label(vdi_ref)
        description = self.session.xenapi.VDI.get_name_description(vdi_ref)

        if not is_a_snapshot:
            raise util.SMException("Can not make clone not from snapshot %s" % snap_uuid)

        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))
        self.snapshot_of = self.session.xenapi.VDI.get_snapshot_of(vdi_ref)

        clone_uuid = util.gen_uuid()

        vdi_uuid = self.session.xenapi.VDI.get_uuid(self.snapshot_of)
        vdi_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)
        snap_name = "%s@%s%s" % (vdi_name, self.sr.SNAPSHOT_PREFIX, snap_uuid)
        clone_name = "%s%s" % (self.sr.VDI_PREFIX, clone_uuid)

        cloneVDI = self.sr.vdi(clone_uuid)
        cloneVDI.label = "%s (clone)" % label
        cloneVDI.description = description
        cloneVDI.path = self.sr._get_path(snap_uuid)
        cloneVDI.location = cloneVDI.uuid
        cloneVDI.size = self.size
        cloneVDI.utilisation = self.size
        cloneVDI.sm_config = dict()
        for key, val in sm_config.iteritems(): # TODO: Remove rbd-parent from here
            if key not in ["type", "vdi_type", "rbd-parent", "paused", "attached"] and \
                    not key.startswith("host_"):
                cloneVDI.sm_config[key] = val

        if 'attached' in sm_config:
            if 'paused' not in sm_config:
                if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                    raise util.SMException("failed to pause VDI %s" % vdi_uuid)
            self._unmap_rbd(vdi_uuid,self.size, devlinks=False, norefcount=True)
        # ---
        util.pread2(
            ["rbd", "clone", "%s/%s" % (self.sr.CEPH_POOL_NAME, snap_name), clone_name, "--name", self.sr.CEPH_USER])
        cloneVDI.ref = cloneVDI._db_introduce()
        CVDI.update(cloneVDI, sr_uuid, clone_uuid)
        # ---
        if 'attached' in sm_config:
            self._map_rbd(vdi_uuid, self.size, devlinks=False, norefcount=True)
            if 'paused' not in sm_config:
                if not blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None):
                    raise util.SMException("failed to unpause VDI %s" % vdi_uuid)

        return cloneVDI.get_params()

    def snapshot(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        # TODO: Test the method
        util.SMlog("rbdsr_common.CVDI.snapshot: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        is_a_snapshot = self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref)
        label = self.session.xenapi.VDI.get_name_label(vdi_ref)
        description = self.session.xenapi.VDI.get_name_description(vdi_ref)

        if is_a_snapshot:
            raise util.SMException("Can not make snapshot from snapshot %s" % vdi_uuid)

        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))

        snap_uuid = util.gen_uuid()

        vdi_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)
        snapshot_name = "%s@%s%s" % (vdi_name, self.sr.SNAPSHOT_PREFIX, snap_uuid)

        snapVDI = self.sr.vdi(snap_uuid)
        snapVDI.label = "%s (snapshot)" % label #TODO: base or snapshot?
        snapVDI.description = description
        snapVDI.path = self.sr._get_path(snap_uuid)
        snapVDI.location = snapVDI.uuid
        snapVDI.size = self.size
        snapVDI.utilisation = self.size
        snapVDI.sm_config = dict()
        for key, val in sm_config.iteritems(): # TODO: Remove rbd-parent from here
            if key not in ["type", "vdi_type", "rbd-parent", "paused", "attached"] and \
                    not key.startswith("host_"):
                snapVDI.sm_config[key] = val
        snapVDI.read_only = True
        snapVDI.is_a_snapshot = True
        snapVDI.snapshot_of = vdi_ref

        if 'attached' in sm_config:
            if 'paused' not in sm_config:
                if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                    raise util.SMException("failed to pause VDI %s" % vdi_uuid)
            self._unmap_rbd(vdi_uuid,self.size, devlinks=False, norefcount=True)
        # ---
        util.pread2(
            ["rbd", "snap", "create", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        util.pread2(
            ["rbd", "snap", "protect", snapshot_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
        snapVDI.ref = snapVDI._db_introduce()
        CVDI.update(snapVDI, sr_uuid, snap_uuid)
        # ---
        if 'attached' in sm_config:
            self._map_rbd(vdi_uuid, self.size, devlinks=False, norefcount=True)
            if 'paused' not in sm_config:
                if not blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None):
                    raise util.SMException("failed to unpause VDI %s" % vdi_uuid)

        return snapVDI.get_params()

    def resize(self, sr_uuid, vdi_uuid, size):
        """
        Resize the given VDI to size <size>. Size can be any valid disk size greater than [or smaller than] the current
        value.
        :param sr_uuid:
        :param vdi_uuid:
        :param size:
        :return:
        """
        util.SMlog("rbdsr_common.CVDI.resize: sr_uuid=%s, vdi_uuid=%s, size=%s" % (sr_uuid, vdi_uuid, size))
        # TODO: Test the method
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)

        if self.rbd_info is not None:
            if self.rbd_info[0] == 'image':

                self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))

                if size < OBJECT_SIZE_B:
                    size_M = OBJECT_SIZE_B // 1024 // 1024
                else:
                    size_M = size // 1024 // 1024

                size = size_M * 1024 * 1024

                if size == self.size:
                    return self.get_params(self)
                elif size < self.size:
                    util.SMlog('rbdsr_common.CVDI.resize: shrinking not supported yet: ' +
                               '(current size: %d, new size: %d)' % (self.size, size))
                    raise xs_errors.XenError('VDIResize', opterr='shrinking not allowed')

                if not self.sr.isSpaceAvailable(size - self.size):
                    util.SMlog('rbdsr_common.VDI.create: vdi size is too big: (vdi size: %d, sr free space size: %d)' %
                               (size, self.sr.RBDPOOLs[sr_uuid]['stats']['max_avail']))
                    raise xs_errors.XenError('VDIResize', opterr='size is too big: vdi size: %d, sr free space size: %d'
                                                            % (size, self.sr.RBDPOOLs[sr_uuid]['stats']['max_avail']))
                ####
                if vdi_uuid == 'MGT':
                    rbd_name = vdi_uuid
                else:
                    rbd_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)

                util.pread2(["rbd", "resize", "--size", str(size_M), "--allow-shrink", rbd_name, "--pool",
                             self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])

                self.sr._updateStats(self.sr.uuid, size - self.size)
                self.size = size
                self.session.xenapi.VDI.set_virtual_size(vdi_ref, str(self.size))
                self.session.xenapi.VDI.set_physical_utilisation(vdi_ref, str(self.utilisation))

                return self.get_params()
            else:
                raise xs_errors.XenError('VDIResize', opterr='Could not resize RBD snapshot %s in pool %s' %
                                                                  (vdi_uuid, sr_uuid))
        else:
            raise xs_errors.XenError('VDIUnavailable', opterr='Could not find image %s in pool %s' %
                                                              (vdi_uuid, sr_uuid))

    def resize_online(self, sr_uuid, vdi_uuid, size):
        """
        Resize the given VDI which may have active VBDs, which have been paused for the duration of this call.
        :param sr_uuid:
        :param vdi_uuid:
        :param size:
        :return:
        """
        util.SMlog("rbdsr_common.CVDI.resize_online: sr_uuid=%s, vdi_uuid=%s, size=%s" % (sr_uuid, vdi_uuid, size))

        if self.rbd_info is not None:
            if self.rbd_info[0] == 'image':

                vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
                sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)

                if 'attached' in sm_config:
                    if 'paused' not in sm_config:
                        if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                            raise util.SMException("failed to pause VDI %s" % vdi_uuid)
                    self._unmap_rbd(vdi_uuid, self.size, devlinks=False, norefcount=True)

                retval = CVDI.resize(self, sr_uuid, vdi_uuid, size)

                if 'attached' in sm_config:
                    self._map_rbd(vdi_uuid, self.size, devlinks=False, norefcount=True)
                    if 'paused' not in sm_config:
                        blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None)

                return retval
            else:
                raise xs_errors.XenError('VDIResize', opterr='Could not resize RBD snapshot %s in pool %s' %
                                                                  (vdi_uuid, sr_uuid))

    def compose(self, sr_uuid, vdi1_uuid, vdi2_uuid):
        """
        :param sr_uuid:
        :param vdi1_uuid:
        :param vdi2_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.CVDI.compose: sr_uuid=%s, vdi1_uuid=%s, vdi2_uuid=%s" % (sr_uuid, vdi1_uuid, vdi2_uuid))

        super(CVDI, self).compose(sr_uuid,vdi1_uuid, vdi2_uuid)

    def generate_config(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.CVDI.generate_config: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        dict = {}
        dict['device_config'] = self.sr.dconf
        dict['sr_uuid'] = sr_uuid
        dict['vdi_uuid'] = vdi_uuid
        dict['command'] = 'vdi_attach_from_config'
        # Return the 'config' encoded within a normal XMLRPC response so that
        # we can use the regular response/error parsing code.
        config = xmlrpclib.dumps(tuple([dict]), "vdi_attach_from_config")

        return xmlrpclib.dumps((config,), "", True)

    def attach_from_config(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_common.CVDI.attach_from_config: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        self.sr.attach(sr_uuid)
        self.sr.attach(sr_uuid)
        try:
            _vdi_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)
            _dev_name = "%s/%s" % (self.sr.DEV_ROOT, _vdi_name)
            vdi_name = "%s" % (vdi_uuid)
            dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)
            if self.sr.mode == "kernel":
                cmdout = util.pread2(["rbd", "map", _vdi_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
            elif self.mode == "fuse":
                pass
            elif self.mode == "nbd":
                self._disable_rbd_caching()
                cmdout = util.pread2(["rbd-nbd", "--device", "/dev/nbd0", "--nbds_max", str(NBDS_MAX), "-c",
                                      "/etc/ceph/ceph.conf.nocaching", "map",
                                      "%s/%s" % (self.sr.CEPH_POOL_NAME, _vdi_name), "--name",
                                      self.sr.CEPH_USER]).rstrip('\n')
                util.pread2(["ln", "-f", cmdout, _dev_name])
            util.pread2(["ln", "-sf", cmdout, dev_name])

            self.path = self.sr._get_path(vdi_uuid)
            if not util.pathexists(self.path):
                raise xs_errors.XenError('VDIUnavailable', opterr='Unable to attach the heartbeat disk: %s' % self.path)

            return VDI.VDI.attach(self, sr_uuid, vdi_uuid)
        except:
            util.logException("RBDVDI.attach_from_config")
            raise xs_errors.XenError('SRUnavailable', \
                        opterr='Unable to attach the heartbeat disk')


class RBDMetadataHandler:

    def __init__(self, sr, vdi_uuid):
        """
        :param sr_ref:
        :param vdi_uuid:
        """
        util.SMlog("rbdsr_common.RBDMetadataHandler.__init__: sr_uuid = %s, vdi_uuid=%s" % (sr.uuid, vdi_uuid))

        self.CEPH_VDI_NAME = "%s%s" % (sr.VDI_PREFIX, vdi_uuid)
        self.sr = sr

    def updateMetadata(self, vdi_info):
        """
        :param vdi_info:
        :return:
        """
        util.SMlog("rbdsr_common.RBDMetadataHandler.updateMetadata: vdi_info = %s" % vdi_info)

        for tag, value in vdi_info.iteritems():
            if value != '':
                util.pread2(["rbd", "image-meta", "set", self.CEPH_VDI_NAME, tag, str(value), "--pool",
                             self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
            else:
                try:
                    util.pread2(["rbd", "image-meta", "remove", self.CEPH_VDI_NAME, tag, "--pool",
                                 self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
                except Exception:
                    pass

    def retrieveMetadata(self):
        """
        :return:
        """
        cmdout = util.pread2(["rbd", "image-meta", "list", self.CEPH_VDI_NAME, "--pool", self.sr.CEPH_POOL_NAME,
                              "--format", "json", "--name", self.sr.CEPH_USER])
        if len(cmdout) != 0:
            vdi_info = json.loads(cmdout)
        else:
            vdi_info = {}

        util.SMlog("rbdsr_common.RBDMetadataHandler.retrieveMetadata: vdi_info = %s" % vdi_info)
        return vdi_info
