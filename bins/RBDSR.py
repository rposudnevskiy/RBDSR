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
from rbdsr_raw import *
from rbdsr_raw2 import *
import SRCommand
import SR
import util
from lock import Lock

CAPABILITIES = ["VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH", "VDI_CLONE", "VDI_SNAPSHOT",
                "VDI_INTRODUCE", "VDI_RESIZE", "VDI_RESIZE_ONLINE", "VDI_UPDATE", "VDI_MIRROR",
                "VDI_RESET_ON_BOOT/2", "VDI_GENERATE_CONFIG", "ATOMIC_PAUSE",
                "SR_SCAN", "SR_UPDATE", "SR_ATTACH", "SR_DETACH", "SR_PROBE", "SR_METADATA"]

CONFIGURATION = [['rbd-mode', 'SR mount mode (optional): kernel, fuse, nbd (default)'],
                 ['vdi-type', 'Image format (optional): vhd (default), raw'],
                 ['cephx-id', 'Cephx id to be used (optional): default is admin'],
                 ['use-rbd-meta', 'Store VDI params in rbd metadata (optional): True (default), False'],
                 ['vdi-update-existing', 'Update params of existing VDIs on scan (optional): True (default), False']]

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

OPS_EXCLUSIVE = ["sr_create", "sr_delete", "sr_attach", "sr_detach", "sr_scan",
        "sr_update", "vdi_create", "vdi_delete", "vdi_resize", "vdi_snapshot",
        "vdi_clone"]

TYPE = "rbd"

PROVISIONING_TYPES = ["thin", "thick"]
PROVISIONING_DEFAULT = "thick"

MODE_TYPES = ["kernel", "fuse", "nbd"]
MODE_DEFAULT = "nbd"

VDI_TYPES = ["vhd", "raw", "raw2"]
VDI_TYPE_DEFAULT = "vhd"


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

        if 'vdi-type' in srcmd.dconf:
            vditype = srcmd.dconf['vdi-type']
        else:
            vditype = VDI_TYPE_DEFAULT

        subtypeclass = "%sSR" % DRIVER_CLASS_PREFIX[vditype]
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

        if 'vdi-type' in srcmd.dconf:
            self.vdi_type = srcmd.dconf.get('vdi-type')
        else:
            self.vdi_type = VDI_TYPE_DEFAULT

        if 'cephx-id' in srcmd.dconf:
            self.CEPH_USER = ("client.%s" % srcmd.dconf.get('cephx-id'))

        if 'use-rbd-meta' in srcmd.dconf:
            self.USE_RBD_META = srcmd.dconf['use-rbd-meta']
        else:
            self.USE_RBD_META = USE_RBD_META_DEFAULT

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

#    def probe(self):
#        """
#        :return:
#        """
#        util.SMlog("RBDSR.SR.probe for sr_uuid = %s" % self.uuid)
#
#        return super(RBDSR, self).probe()

#    def scan(self, sr_uuid):
#        """
#        :param sr_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.SR.scan: sr_uuid=%s" % sr_uuid)
#
#        super(RBDSR, self).scan(sr_uuid)

#    def attach(self, sr_uuid):
#        """
#        :param sr_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.SR.attach: sr_uuid=%s" % sr_uuid)
#
#        super(RBDSR, self).attach(sr_uuid)

#    def detach(self, sr_uuid):
#        """
#        :param sr_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.SR.detach: sr_uuid=%s" % sr_uuid)
#
#        super(RBDSR, self).detach(sr_uuid)

#    def create(self, sr_uuid, size):
#        """
#        :param sr_uuid:
#        :param size:
#        :return:
#        """
#        util.SMlog("RBDSR.SR.create: sr_uuid=%s, size=%s" % (sr_uuid, size))
#
#        self.attach(sr_uuid)

#    def delete(self, sr_uuid):
#        """
#        :param sr_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.SR.delete: sr_uuid=%s" % sr_uuid)
#
#        self.detach(sr_uuid)

#    def update(self, sr_uuid):
#        """
#        :param sr_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.SR.update: sr_uuid=%s" % sr_uuid)
#
#        super(RBDSR, self).update(sr_uuid)


class RBDVDI(object):

    def __new__(cls, *args, **kwargs):
        sr_ref = args[0]
        subtypeclass = "%sVDI" % DRIVER_CLASS_PREFIX[sr_ref.vdi_type]
        return object.__new__(type('RBDVDI',
                                   (RBDVDI, globals()[subtypeclass]) + RBDVDI.__bases__,
                                   dict(RBDVDI.__dict__)),
                              *args, **kwargs)

#    def __init__(self, sr_ref, vdi_uuid):
#        """
#        :param sr_ref:
#        :param vdi_uuid:
#        """
#        util.SMlog("RBDSR.VDI.__init__: vdi_uuid=%s" % vdi_uuid)
#
#        super(RBDVDI, self).__init__(sr_ref, vdi_uuid)

#    def load(self, vdi_uuid):
#        """
#        :param vdi_uuid:
#        """
#        util.SMlog("RBDSR.VDI.load: vdi_uuid=%s" % vdi_uuid)
#
#        super(RBDVDI, self).load(vdi_uuid)

#    def delete(self, sr_uuid, vdi_uuid):
#        """
#        :param sr_uuid:
#        :param vdi_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.VDI.delete: sr_uuid = %s, vdi_uuid = %s, size = %s" % (sr_uuid, vdi_uuid))
#
#        super(RBDVDI, self).delete(sr_uuid, vdi_uuid)

#    def create(self, sr_uuid, vdi_uuid, size):
#        """
#        :param sr_uuid:
#        :param vdi_uuid:
#        :param size:
#        :return:
#        """
#        util.SMlog("RBDSR.VDI.create: sr_uuid = %s, vdi_uuid = %s, size = %s" % (sr_uuid, vdi_uuid, size))
#
#        return super(RBDVDI, self).create(sr_uuid, vdi_uuid, size)

#    def update(self, sr_uuid, vdi_uuid):
#        """
#        :param sr_uuid:
#        :param vdi_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.VDI.update: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
#
#        super(RBDVDI, self).update(sr_uuid, vdi_uuid)

#    def introduce(self, sr_uuid, vdi_uuid):
#        """
#        Explicitly introduce a particular VDI.
#        :param sr_uuid:
#        :param vdi_uuid:
#        :return:
#        """
#        util.SMlog("RBDVDI.VDI.introduce: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
#
#        return super(RBDVDI, self).update(sr_uuid, vdi_uuid)

#    def attach(self, sr_uuid, vdi_uuid):
#        """
#        :param sr_uuid:
#        :param vdi_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.attach: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
#
#        return super(RBDVDI, self).attach(sr_uuid, vdi_uuid)

#    def detach(self, sr_uuid, vdi_uuid):
#        """
#        :param sr_uuid:
#        :param vdi_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.detach: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
#
#        super(RBDVDI, self).detach(sr_uuid, vdi_uuid)

#    def clone(self, sr_uuid, vdi_uuid):
#        """
#        :param sr_uuid:
#        :param snap_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.clone: sr_uuid=%s, snap_uuid=%s" % (sr_uuid, vdi_uuid))
#
#        return super(RBDVDI, self).clone(sr_uuid, vdi_uuid)

#    def snapshot(self, sr_uuid, vdi_uuid):
#        """
#        :param sr_uuid:
#        :param vdi_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.snapshot: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
#
#        return super(RBDVDI, self).snapshot(sr_uuid, vdi_uuid)

#    def resize(self, sr_uuid, vdi_uuid, size):
#        """
#        Resize the given VDI to size <size>. Size can be any valid disk size greater than [or smaller than] the current
#        value.
#        :param sr_uuid:
#        :param vdi_uuid:
#        :param size:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.resize: sr_uuid=%s, vdi_uuid=%s, size=%s" % (sr_uuid, vdi_uuid, size))
#
#        return super(RBDVDI, self).resize(sr_uuid, vdi_uuid, size)

#    def resize_online(self, sr_uuid, vdi_uuid, size):
#        """
#        Resize the given VDI which may have active VBDs, which have been paused for the duration of this call.
#        :param sr_uuid:
#        :param vdi_uuid:
#        :param size:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.resize_online: sr_uuid=%s, vdi_uuid=%s, size=%s" % (sr_uuid, vdi_uuid, size))
#
#        return super(RBDVDI, self).resize_online(sr_uuid, vdi_uuid, size)

#    def compose(self, sr_uuid, vdi1_uuid, vdi2_uuid):
#        """
#        :param sr_uuid:
#        :param vdi1_uuid:
#        :param vdi2_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.compose: sr_uuid=%s, vdi1_uuid=%s, vdi2_uuid=%s" % (sr_uuid, vdi1_uuid, vdi2_uuid))
#
#        super(RBDVDI, self).compose(sr_uuid, vdi1_uuid, vdi2_uuid)

#    def generate_config(self, sr_uuid, vdi_uuid):
#        """
#        :param sr_uuid:
#        :param vdi_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.generate_config: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
#
#        return super(RBDVDI, self).generate_config(sr_uuid, vdi_uuid)

#    def attach_from_config(self, sr_uuid, vdi_uuid):
#        """
#        :param sr_uuid:
#        :param vdi_uuid:
#        :return:
#        """
#        util.SMlog("RBDSR.RBDVDI.attach_from_config: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
#
#        return super(RBDVDI, self).attach_from_config(sr_uuid, vdi_uuid)

if __name__ == '__main__':
    SRCommand.run(RBDSR, DRIVER_INFO)
else:
    SR.registerSR(RBDSR)
