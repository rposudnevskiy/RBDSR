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

# import util
# import json
# import re
from rbdsr_common import *

VDI_TYPE = 'aio'

DRIVER_TYPE = 'raw'
DRIVER_CLASS_PREFIX[DRIVER_TYPE] = 'RBDRAW'

VDI_PREFIX = "RAW-"
CLONE_PREFIX = "RAW-"
SXM_PREFIX = "SXM-"
SNAPSHOT_PREFIX = "SNAP-"

class RBDRAWSR(CSR):

    def load(self, sr_uuid):
        """
        :param sr_uuid:
        """
        util.SMlog("rbdsr_raw.SR._load: vdi_uuid=%s" % sr_uuid)

        self.vdi_type = VDI_TYPE

        self.VDI_PREFIX = VDI_PREFIX
        self.SXM_PREFIX = SXM_PREFIX
        self.SNAPSHOT_PREFIX = SNAPSHOT_PREFIX

    def _get_snap_uuid(self, rbd_snap_name):
        """
        :param rbd_snap_name:
        :return:
        """
        util.SMlog("rbdsr_raw.SR._get_snap_uuid: rbd_snap_name = %s" % rbd_snap_name)
        regex = re.compile(self.SNAPSHOT_PREFIX)
        return regex.sub('', rbd_snap_name)

    def _get_vdilist(self, rbd_pool_name):
        """
        :param rbd_pool_name:
        :return:
        """
        util.SMlog("Calling cephutils.SR._get_vdilist: pool=%s" % rbd_pool_name)

        rbdvdis = {}

        cmd = ["rbd", "ls", "-l", "--format", "json", "--pool", rbd_pool_name, "--name", self.CEPH_USER]
        cmdout = util.pread2(cmd)
        decoded = json.loads(cmdout)

        for vdi in decoded:
            if vdi['image'].find("SXM") == -1:
                if vdi.has_key('snapshot'):
                    if vdi['snapshot'].startswith(self.SNAPSHOT_PREFIX):
                        snap_uuid = self._get_snap_uuid(vdi['snapshot'])
                        rbdvdis[snap_uuid] = vdi
                elif vdi['image'].startswith(self.VDI_PREFIX) or vdi['image'].startswith(self.CLONE_PREFIX):
                    vdi_uuid = self._get_vdi_uuid(vdi['image'])
                    rbdvdis[vdi_uuid] = vdi

        return rbdvdis

    def _loadvdis(self):
        """
        :return:
        """
        util.SMlog("rbdsr_raw.SR._loadvdis")

        rbdvdis = self._get_vdilist(self.CEPH_POOL_NAME)

class RBDRAWVDI(CVDI):

    def _load(self, vdi_uuid):
        """
        :param vdi_uuid:
        """
        util.SMlog("rbdsr_raw.VDI._load: vdi_uuid=%s" % vdi_uuid)
