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
        util.SMlog("RBDSR.RBDSR.__new__: args = %s, kwargs = %s" % (str(args), str(kwargs)))

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
        util.SMlog("RBDSR.RBDSR.__init__: srcmd = %s, sr_uuid= %s" % (srcmd, sr_uuid))

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

        if 'disable-caching' in srcmd.dconf:
            self.disable_caching = srcmd.dconf['disable-caching']

        if 'snapshot-pool' in srcmd.dconf:
            self.CEPH_SNAPSHOT_POOL = srcmd.dconf['snapshot-pool']

        self.provision = PROVISIONING_DEFAULT
        self.mode = MODE_DEFAULT
        self.ops_exclusive = OPS_EXCLUSIVE

        util.SMlog("RBDSR.RBDSR.__init__: Using cephx id %s" % self.CEPH_USER)
        super(RBDSR, self).__init__(srcmd, sr_uuid)

    def load(self, sr_uuid):
        """
        :param sr_uuid:
        """
        util.SMlog("RBDSR.RBDSR.load: sr_uuid= %s" % sr_uuid)

        super(RBDSR, self).load(sr_uuid)

    def handles(type):
        """
        :param type:
        :return:
        """
        util.SMlog("RBDSR.RBDSR.handles type = %s" % type)

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
        util.SMlog("RBDSR.RBDSR.content_type sr_uuid = %s" % sr_uuid)

        return self.content_type(sr_uuid)

    def vdi(self, vdi_uuid):
        """
        Create a VDI class
        :param vdi_uuid:
        :return:
        """
        util.SMlog("RBDSR.RBDSR.vdi vdi_uuid = %s" % vdi_uuid)

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


class RBDSR_GC(object):

    def __new__(cls, *args, **kwargs):
        """
        :param args:
        :param kwargs:
        :return:
        """
        util.SMlog("RBDSR.RBDSR_GC.__new__: args = %s, kwargs = %s" % (str(args), str(kwargs)))

        sr_uuid = args[0]
        xapi = args[1]
        sr_ref = xapi.session.xenapi.SR.get_by_uuid(sr_uuid)

        host_ref = util.get_localhost_uuid(xapi.session)

        _PBD = xapi.session.xenapi.PBD
        pbds = _PBD.get_all_records_where('field "SR" = "%s" and' % sr_ref +
                                          'field "host" = "%s"' % host_ref)
        pbd_ref, pbd = pbds.popitem()
        assert not pbds

        device_config = _PBD.get_device_config(pbd_ref)

        if 'driver-type' in device_config:
            DRIVER_TYPE = device_config['driver-type']
        else:
            DRIVER_TYPE = DRIVER_TYPE_DEFAULT

        subtypeclass = "%sSR_GC" % DRIVER_CLASS_PREFIX[DRIVER_TYPE]
        return object.__new__(type('RBDSR_GC',
                                   (RBDSR_GC, globals()[subtypeclass]) + RBDSR_GC.__bases__,
                                   dict(RBDSR_GC.__dict__)),
                              *args, **kwargs)

    def __init__(self, sr_uuid, xapi, createLock, force):
        """
        :param uuid:
        :param xapi:
        :param createLock:
        :param force:
        """
        util.SMlog("RBDSR.RBDSR_GC.__init__: sr_uuid = %s" % sr_uuid)

        host_ref = util.get_localhost_uuid(self.xapi.session)

        _PBD = xapi.session.xenapi.PBD
        pbds = _PBD.get_all_records_where('field "SR" = "%s" and' % self.ref +
                                          'field "host" = "%s"' % host_ref)
        pbd_ref, pbd = pbds.popitem()
        assert not pbds

        device_config = _PBD.get_device_config(pbd_ref)

        if 'driver-type' in device_config:
            self.DRIVER_TYPE = device_config['driver-type']
        else:
            self.DRIVER_TYPE = DRIVER_TYPE_DEFAULT

        if 'use-rbd-meta' in self.sm_config:
            self.USE_RBD_META = self.sm_config['use-rbd-meta']

        if 'disable-caching' in self.sm_config:
            self.disable_caching = self.sm_config['disable-caching']

        if 'cephx-id' in self.sm_config:
            self.CEPH_USER = "client.%s" % self.sm_config['cephx-id']

        util.SMlog("RBDSR.RBDSR_GC.__init__: Using cephx id %s" % self.CEPH_USER)
        super(RBDSR_GC, self).__init__(sr_uuid, xapi, createLock, force)

    def vdi(self, sr, uuid, raw):
        """
        :param sr:
        :param uuid:
        :param raw:
        :return:
        """
        util.SMlog("RBDSR.RBDSR_GC.vdi uuid = %s" % uuid)

        return RBDVDI_GC(sr, uuid, raw)


class RBDVDI_GC(object):

    def __new__(cls, *args, **kwargs):
        sr_ref = args[0]
        subtypeclass = "%sVDI_GC" % DRIVER_CLASS_PREFIX[sr_ref.DRIVER_TYPE]
        return object.__new__(type('RBDVDI_GC',
                                   (RBDVDI_GC, globals()[subtypeclass]) + RBDVDI_GC.__bases__,
                                   dict(RBDVDI_GC.__dict__)),
                              *args, **kwargs)


if __name__ == '__main__':
    SRCommand.run(RBDSR, DRIVER_INFO)
else:
    SR.registerSR(RBDSR)
