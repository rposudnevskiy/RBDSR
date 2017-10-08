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

from rbdsr_vhd import *
from rbdsr_dmp import *
from rbdsr_rbd import *
import SRCommand
import SR
import util
from lock import Lock

CAPABILITIES = ["VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH", "VDI_CLONE", "VDI_SNAPSHOT",
                "VDI_INTRODUCE", "VDI_RESIZE", "VDI_RESIZE_ONLINE", "VDI_UPDATE", "VDI_MIRROR",
                "VDI_RESET_ON_BOOT/2", "VDI_GENERATE_CONFIG", "ATOMIC_PAUSE",
                "SR_SCAN", "SR_UPDATE", "SR_ATTACH", "SR_DETACH", "SR_PROBE", "SR_METADATA"]

CONFIGURATION = [['rbd-mode', 'SR mount mode (optional): kernel, fuse, nbd (default)'],
                 ['driver-type', 'Driver type (optional): vhd (default), dmp, rbd'],
                 ['cephx-id', 'Cephx id to be used (optional): default is admin'],
                 ['use-rbd-meta', 'Store VDI params in rbd metadata (optional): True (default), False'],
                 ['vdi-update-existing', 'Update params of existing VDIs on scan (optional): True, False (default)']]

DRIVER_INFO = {
    'name': 'RBD',
    'description': 'Handles virtual disks on CEPH RBD devices',
    'vendor': 'Roman V. Posudnevskiy',
    'copyright': '(c) 2017 Roman V. Posudnevskiy',
    'driver_version': '2.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
    }

OPS_EXCLUSIVE = ['sr_create', 'sr_delete', 'sr_attach', 'sr_detach', 'sr_scan',
        'sr_update', 'vdi_create', 'vdi_delete', 'vdi_resize', 'vdi_snapshot',
        'vdi_clone']

TYPE = 'rbd'

PROVISIONING_TYPES = ['thin', 'thick']
PROVISIONING_DEFAULT = 'thick'

MODE_TYPES = ['kernel', 'fuse', 'nbd']
MODE_DEFAULT = 'nbd'

VDI_TYPES = ['vhd', 'aio']

DRIVER_TYPES = ['vhd', 'dmp', 'rbd']
DRIVER_TYPE_DEFAULT = 'vhd'

class RBDSR(object):
    """Ceph Block Devices storage repository"""

    def __new__(cls, *args, **kwargs):
        """
        :param args:
        :param kwargs:
        :return:
        """
        util.SMlog("RBDSR.SR.__new__: args = %s, kwargs = %s" % (str(args), str(kwargs)))

        srcmd = args[0]

        if 'driver-type' in srcmd.dconf:
            DRIVER_TYPE = srcmd.dconf['driver-type']
        else:
            DRIVER_TYPE = DRIVER_TYPE_DEFAULT

        subtypeclass = "%sSR" % DRIVER_CLASS_PREFIX[DRIVER_TYPE]
        return object.__new__(type('RBDSR',
                                   (RBDSR, globals()[subtypeclass]) + RBDSR.__bases__,
                                   dict(RBDSR.__dict__)),
                              *args, **kwargs)

    def __init__(self, srcmd, sr_uuid):
        """
        :param srcmd:
        :param sr_uuid:
        """
        util.SMlog("RBDSR.SR.__init__: srcmd = %s, sr_uuid= %s" % (srcmd, sr_uuid))

        if 'driver-type' in srcmd.dconf:
            self.DRIVER_TYPE = srcmd.dconf.get('driver-type')
        else:
            self.DRIVER_TYPE = DRIVER_TYPE_DEFAULT

        if 'cephx-id' in srcmd.dconf:
            self.CEPH_USER = ("client.%s" % srcmd.dconf.get('cephx-id'))

        if 'use-rbd-meta' in srcmd.dconf:
            self.USE_RBD_META = srcmd.dconf['use-rbd-meta']

        if 'vdi-update-existing' in srcmd.dconf:
            self.VDI_UPDATE_EXISTING = srcmd.dconf['vdi-update-existing']

        self.provision = PROVISIONING_DEFAULT
        self.mode = MODE_DEFAULT

        super(RBDSR, self).__init__(srcmd, sr_uuid)

        util.SMlog("RBDSR.SR.__init__: Using cephx id %s" % self.CEPH_USER)

    def load(self, sr_uuid):
        """
        :param sr_uuid:
        """
        util.SMlog("RBDSR.SR.load: sr_uuid= %s" % sr_uuid)

        self.lock = Lock('sr', self.uuid)
        self.ops_exclusive = OPS_EXCLUSIVE
        self.mode = MODE_DEFAULT

        super(RBDSR, self).load(sr_uuid)

    def handles(type):
        """
        :param type:
        :return:
        """
        util.SMlog("RBDSR.SR.handles type = %s" % type)

        if type == TYPE:
            return True
        else:
            return False
    handles = staticmethod(handles)

    def content_type(self, sr_uuid):
        """
        :param sr_uuid:
        :return: content_type XML
        """
        util.SMlog("RBDSR.SR.content_type sr_uuid = %s" % sr_uuid)

        return self.content_type(sr_uuid)

    def vdi(self, vdi_uuid):
        """
        Create a VDI class
        :param vdi_uuid:
        :return:
        """
        util.SMlog("RBDSR.SR.vdi vdi_uuid = %s" % vdi_uuid)

        if vdi_uuid not in self.vdis:
            self.vdis[vdi_uuid] = RBDVDI(self, vdi_uuid)
        return self.vdis[vdi_uuid]

class RBDVDI(object):

    def __new__(cls, *args, **kwargs):
        sr_ref = args[0]
        subtypeclass = "%sVDI" % DRIVER_CLASS_PREFIX[sr_ref.DRIVER_TYPE]
        return object.__new__(type('RBDVDI',
                                   (RBDVDI, globals()[subtypeclass]) + RBDVDI.__bases__,
                                   dict(RBDVDI.__dict__)),
                              *args, **kwargs)


if __name__ == '__main__':
    SRCommand.run(RBDSR, DRIVER_INFO)
else:
    SR.registerSR(RBDSR)
