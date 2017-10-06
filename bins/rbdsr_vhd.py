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

import vhdutil
import lvhdutil
import inventory
from rbdsr_common import *

VDI_TYPE = 'vhd'

DRIVER_TYPE = 'vhd'
DRIVER_CLASS_PREFIX[DRIVER_TYPE] = 'RBDVHD'

VDI_PREFIX = 'VHD-'


class RBDVHDSR(CSR):

    def load(self, sr_uuid):
        """
        :param sr_uuid:
        """
        util.SMlog("rbdsr_vhd.RBDVHDSR._load: sr_uuid=%s" % sr_uuid)

        self.VDI_PREFIX = VDI_PREFIX
        self.vdi_type = VDI_TYPE

        super(RBDVHDSR, self).load(sr_uuid)

    def vdi(self, vdi_uuid):
        """
        Create a VDI class
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_vhd.SR.vdi vdi_uuid = %s" % vdi_uuid)

        if vdi_uuid not in self.vdis:
            self.vdis[vdi_uuid] = RBDVHDVDI(self, vdi_uuid)
        return self.vdis[vdi_uuid]


class RBDVHDVDI(CVDI):

    def _create_vhd_over_rbd(self, vdi_uuid, size, rbd_size):
        """
        :param vdi_uuid:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_vhd.RBDVHDVDI._create_vhd_over_rbd: vdi_name = %s, size = %s, rbd_size = %s" % (vdi_uuid, size, rbd_size))
        self._map_rbd(vdi_uuid, rbd_size, norefcount=True)
        # util.pread2(["/usr/bin/vhd-util", "create", "-n", self.path, "-s", str(image_size_M), "-S", str(VHD_MSIZE_MB)])
        ## print "---- self.path = %s, long(size) = %s, False = %s , lvhdutil.MSIZE_MB = %s" % (self.path, long(size), False, lvhdutil.MSIZE_MB)
        vhdutil.create(self.path, long(size), False, lvhdutil.MSIZE_MB)
        self._unmap_rbd(vdi_uuid, rbd_size, norefcount=True)

    def _map_vhd_chain(self, sr_uuid, vdi_uuid, rbd_size, host_uuid=None, read_only=None, dmmode='None', devlinks=True, norefcount=False):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param host_uuid:
        :return:
        """
        util.SMlog("rbdsr_vhd.RBDVHDVDI._map_vhd_chain sr_uuid=%s, vdi_uuid=%s, host_uuid=%s" % (sr_uuid, vdi_uuid, host_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)

        i = 0
        vhd_chain=[]
        #vhd_chain.append(vdi_uuid)
        #i+=1
        parent_sm_config = sm_config
        while 'vhd-parent' in parent_sm_config:
            parent_uuid = parent_sm_config['vhd-parent']
            i+=1
            vhd_chain.append(parent_uuid)
            parent_ref = self.session.xenapi.VDI.get_by_uuid(parent_uuid)
            parent_sm_config = self.session.xenapi.VDI.get_sm_config(parent_ref)
        i_max = i

        try:
            for uuid_to_map in reversed(vhd_chain):
                self._map_rbd(uuid_to_map, rbd_size , host_uuid, read_only, dmmode, devlinks, norefcount)
                i -= 1
        except:
            for k in range(i, i_max):
                self._unmap_rbd(vhd_chain[k], rbd_size, host_uuid, devlinks, norefcount)

            raise xs_errors.XenError('VDIUnavailable', opterr="Could not map: %s" % vhd_chain[i])

    def _unmap_vhd_chain(self, sr_uuid, vdi_uuid, rbd_size, host_uuid=None, devlinks=True, norefcount=False):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param host_uuid:
        :return:
        """
        util.SMlog("rbdsr_vhd.RBDVHDVDI._unmap_vhd_chain sr_uuid=%s, vdi_uuid=%s, host_uuid=%s" % (sr_uuid, vdi_uuid, host_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)

        #self._unmap_rbd(vdi_uuid, size, host_uuid, read_only, dmmode)

        parent_sm_config = sm_config
        while 'vhd-parent' in parent_sm_config:
            parent_uuid = parent_sm_config['vhd-parent']
            parent_ref = self.session.xenapi.VDI.get_by_uuid(parent_uuid)
            parent_sm_config = self.session.xenapi.VDI.get_sm_config(parent_ref)

            self._unmap_rbd(parent_uuid, rbd_size, host_uuid, devlinks, norefcount)

    def create(self, sr_uuid, vdi_uuid, size):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param size:
        :return:
        """
        util.SMlog("rbdsr_vhd.RBDVHDVDI.create: sr_uuid = %s, vdi_uuid = %s, size = %s" % (sr_uuid, vdi_uuid, size))
        # TODO: Checked

        size = vhdutil.validate_and_round_vhd_size(long(size))

        if self.sr.provision == "thin":
            rbd_size = lvhdutil.calcSizeVHDLV(long(size))
        elif self.sr.provision == "thick":
            rbd_size = lvhdutil.calcSizeVHDLV(long(size))

        retval = super(RBDVHDVDI, self).create(sr_uuid, vdi_uuid, rbd_size)
        self._create_vhd_over_rbd(vdi_uuid, size, rbd_size)

        self.size = size
        self.session.xenapi.VDI.set_virtual_size(self.ref, str(size))
        self.session.xenapi.VDI.set_physical_utilisation(self.ref, str(size))

        return retval

    def attach(self, sr_uuid, vdi_uuid, host_uuid=None, dmmode='None'):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param host_uuid:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_vhd.RBDVHDVDI.attach: sr_uuid=%s, vdi_uuid=%s, host_uuid=%s" % (sr_uuid, vdi_uuid, host_uuid))

        self._map_vhd_chain(sr_uuid, vdi_uuid, self.rbd_info[1]['size'], host_uuid=host_uuid, dmmode=dmmode)

        return super(RBDVHDVDI, self).attach(sr_uuid, vdi_uuid, host_uuid=host_uuid, dmmode=dmmode)

    def detach(self, sr_uuid, vdi_uuid, host_uuid=None):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_vhd.RBDVHDVDI.detach: sr_uuid=%s, vdi_uuid=%s, host_uuid=%s" % (sr_uuid, vdi_uuid, host_uuid))

        super(RBDVHDVDI, self).detach(sr_uuid, vdi_uuid, host_uuid=host_uuid)

        self._unmap_vhd_chain(sr_uuid, vdi_uuid, self.rbd_info[1]['size'], host_uuid=host_uuid)

    def snapshot(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param snap_uuid:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_vhd.RBDVHDVDI.snapshot: sr_uuid=%s, snap_uuid=%s" % (sr_uuid, vdi_uuid))

        return self.clone(sr_uuid, vdi_uuid, mode='snapshot')

    def clone(self, sr_uuid, vdi_uuid, mode='clone'):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        # TODO: Test the method
        util.SMlog("rbdsr_vhd.RBDVHDVDI.clone: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        is_a_snapshot = self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref)
        label = self.session.xenapi.VDI.get_name_label(vdi_ref)
        description = self.session.xenapi.VDI.get_name_description(vdi_ref)

        local_host_uuid = inventory.get_localhost_uuid()

        if mode == 'snapshot' and is_a_snapshot:
            raise util.SMException("Can not make snapshot form snapshot %s" % vdi_uuid)

        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))

        if not is_a_snapshot:
            base_uuid = util.gen_uuid()
        else:
            base_uuid = sm_config["vhd-parent"]
        clone_uuid = util.gen_uuid()

        util.SMlog("rbdsr_vhd.RBDVHDVDI.clone: Pepare CloneVDI: sr_uuid=%s, clone_uuid=%s" % (sr_uuid, clone_uuid))
        cloneVDI = self.sr.vdi(clone_uuid)
        cloneVDI.label = "%s (%s)" % (label, mode)
        cloneVDI.description = description
        cloneVDI.path = self.sr._get_path(clone_uuid)
        cloneVDI.location = cloneVDI.uuid
        cloneVDI.size = self.size
        cloneVDI.utilisation = self.size
        cloneVDI.sm_config = dict()
        for key, val in sm_config.iteritems():
            if key not in ["type", "vdi_type", "vhd-parent", "paused", "attached"] and \
                    not key.startswith("host_"):
                cloneVDI.sm_config[key] = val
        if mode == 'snapshot':
            cloneVDI.is_a_snapshot = True
            cloneVDI.snapshot_of = vdi_ref

        retval_clone = RBDVHDVDI.create(cloneVDI, sr_uuid, clone_uuid, cloneVDI.size)
        clone_ref = self.session.xenapi.VDI.get_by_uuid(clone_uuid)

        if not is_a_snapshot:
            util.SMlog("rbdsr_vhd.RBDVHDVDI.clone: Pepare BaseVDI: sr_uuid=%s, base_uuid=%s" % (sr_uuid, base_uuid))
            baseVDI = self.sr.vdi(base_uuid)
            baseVDI.label = "%s (base)" % label
            baseVDI.description = description
            baseVDI.path = self.sr._get_path(base_uuid)
            baseVDI.location = baseVDI.uuid
            baseVDI.managed = False
            baseVDI.size = self.size
            baseVDI.utilisation = self.size
            baseVDI.sm_config = dict()

            retval_base = RBDVHDVDI.create(baseVDI,sr_uuid, base_uuid, baseVDI.size)
            base_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
        else:
            base_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
            baseVDI = self.sr.vdi(base_uuid)
            baseVDI.path = self.sr._get_path(base_uuid)
            baseVDI.sm_config = self.session.xenapi.VDI.get_sm_config(base_ref)

        if not is_a_snapshot:
            if 'attached' in sm_config:
                util.SMlog("rbdsr_vhd.RBDVHDVDI.clone: Unmap VDI as it's mapped: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
                if 'paused' not in sm_config:
                    if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                        raise util.SMException("failed to pause VDI %s" % vdi_uuid)
                self._unmap_rbd(vdi_uuid, self.size, devlinks=False, norefcount=True)
            util.SMlog(
                "rbdsr_vhd.RBDVHDVDI.clone: Swap Base and VDI: sr_uuid=%s, vdi_uuid=%s, base_uuid=%s" % (sr_uuid, vdi_uuid, base_uuid))
            tmp_uuid = "temporary"  # util.gen_uuid()
            self._rename_rbd(vdi_uuid, tmp_uuid)
            self._rename_rbd(base_uuid, vdi_uuid)
            self._rename_rbd(tmp_uuid, base_uuid)

        if not is_a_snapshot:
            if 'attached' in sm_config:
                self._map_rbd(vdi_uuid, self.size, devlinks=False, norefcount=True)
                base_hostRefs = self._get_vdi_hostRefs(vdi_uuid)
                if local_host_uuid not in base_hostRefs:
                    self.attach(sr_uuid, vdi_uuid, host_uuid=local_host_uuid)
            else:
                self.attach(sr_uuid, vdi_uuid, host_uuid=local_host_uuid)
                base_hostRefs = {}
        else:
            if 'attached' not in baseVDI.sm_config:
                RBDVHDVDI.attach(baseVDI, sr_uuid, base_uuid, host_uuid=local_host_uuid)
                base_hostRefs = {}
            else:
                base_hostRefs = self._get_vdi_hostRefs(base_uuid)
                if local_host_uuid not in base_hostRefs:
                    RBDVHDVDI.attach(baseVDI, sr_uuid, base_uuid, host_uuid=local_host_uuid)

        if is_a_snapshot:
            RBDVHDVDI.attach(cloneVDI, sr_uuid, clone_uuid, host_uuid=local_host_uuid)

            vhdutil.snapshot(cloneVDI.path, baseVDI.path, False, lvhdutil.MSIZE_MB)

            RBDVHDVDI.detach(cloneVDI, sr_uuid, clone_uuid, host_uuid=local_host_uuid)
        else:
            for host_uuid in base_hostRefs.iterkeys():
                RBDVHDVDI.attach(baseVDI, sr_uuid, base_uuid, host_uuid=host_uuid)
            if local_host_uuid not in base_hostRefs:
                RBDVHDVDI.attach(baseVDI, sr_uuid, base_uuid, host_uuid=local_host_uuid)
            RBDVHDVDI.attach(cloneVDI, sr_uuid, clone_uuid, host_uuid=local_host_uuid)

            vhdutil.snapshot(cloneVDI.path, baseVDI.path, False, lvhdutil.MSIZE_MB)
            vhdutil.snapshot(self.path, baseVDI.path, False, lvhdutil.MSIZE_MB)
            vhdutil.setHidden(baseVDI.path)

            RBDVHDVDI.detach(cloneVDI, sr_uuid, clone_uuid, host_uuid=local_host_uuid)
            if local_host_uuid not in base_hostRefs:
                RBDVHDVDI.detach(baseVDI, sr_uuid, base_uuid, host_uuid=local_host_uuid)

            baseVDI.read_only = True
            self.session.xenapi.VDI.set_read_only(base_ref, True)

        if mode == 'snapshot':
            cloneVDI.read_only = True
            self.session.xenapi.VDI.set_read_only(clone_ref, True)

        cloneVDI.sm_config["vhd-parent"] = base_uuid
        self.session.xenapi.VDI.add_to_sm_config(cloneVDI.ref, 'vhd-parent', base_uuid)
        RBDVHDVDI.update(cloneVDI, sr_uuid, clone_uuid)

        if not is_a_snapshot:
            if 'vhd-parent' in sm_config:
                baseVDI.sm_config['vhd-parent'] = sm_config['vhd-parent']
                self.session.xenapi.VDI.add_to_sm_config(baseVDI.ref, 'vhd-parent', sm_config['vhd-parent'])
                RBDVHDVDI.update(baseVDI, sr_uuid, base_uuid)
                self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'vhd-parent')
            else:
                RBDVHDVDI.update(baseVDI, sr_uuid, base_uuid)
            self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'vhd-parent', base_uuid)
            self.sm_config['vhd-parent'] = base_uuid
            self.update(sr_uuid, vdi_uuid)

        if not is_a_snapshot:
            if 'attached' in sm_config:
                if 'paused' not in sm_config:
                    if not blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None):
                        raise util.SMException("failed to unpause VDI %s" % vdi_uuid)
            if local_host_uuid not in base_hostRefs:
                self.detach(sr_uuid, vdi_uuid, host_uuid=local_host_uuid)
        else:
            if local_host_uuid not in base_hostRefs:
                RBDVHDVDI.detach(baseVDI, sr_uuid, base_uuid, host_uuid=local_host_uuid)

        return retval_clone

    def resize(self, sr_uuid, vdi_uuid, size, online=False):
        """
        Resize the given VDI to size <size>. Size can be any valid disk size greater than [or smaller than] the current
        value.
        :param sr_uuid:
        :param vdi_uuid:
        :param size:
        :param online:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_vhd.RBDVHDVDI.resize: sr_uuid=%s, vdi_uuid=%s, size=%s" % (sr_uuid, vdi_uuid, size))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        vdi_hostRefs = self._get_vdi_hostRefs(vdi_uuid)
        local_host_uuid = inventory.get_localhost_uuid()

        if 'attached' in sm_config and online is False:
            online = True
            raise xs_errors.XenError('VDIResize', opterr='Online resize is not supported in VHD mode')

        size = vhdutil.validate_and_round_vhd_size(long(size))

        if self.sr.provision == "thin":
            rbdSizeNew = lvhdutil.calcSizeVHDLV(size)
        elif self.sr.provision == "thick":
            rbdSizeNew = lvhdutil.calcSizeVHDLV(size)

        if online:
            retval = super(RBDVHDVDI, self).resize_online(sr_uuid, vdi_uuid, rbdSizeNew)
        else:
            retval = super(RBDVHDVDI, self).resize(sr_uuid, vdi_uuid, rbdSizeNew)

        if not online:
            #self._map_rbd(vdi_uuid, rbdSizeNew, norefcount=True)
            self.attach(sr_uuid, vdi_uuid, host_uuid=local_host_uuid)
        else:
            if local_host_uuid not in vdi_hostRefs:
                self.attach(sr_uuid, vdi_uuid, host_uuid=local_host_uuid)

        vhdutil.setSizePhys(self.path, size, False)
        vhdutil.setSizeVirtFast(self.path, size)

        if online:
            if not blktap2.VDI.tap_refresh(self.session, self.sr.uuid, vdi_uuid, True):
                raise util.SMException("failed to refresh VDI %s" % vdi_uuid)

        if not online:
            #self._unmap_rbd(vdi_uuid, rbdSizeNew, norefcount=True)
            self.detach(sr_uuid, vdi_uuid, host_uuid=local_host_uuid)
        else:
            if local_host_uuid not in vdi_hostRefs:
                self.detach(sr_uuid, vdi_uuid, host_uuid=local_host_uuid)

        self.size = size
        self.session.xenapi.VDI.set_virtual_size(vdi_ref, str(size))
        self.session.xenapi.VDI.set_physical_utilisation(vdi_ref, str(size))

        return retval

    def resize_online(self, sr_uuid, vdi_uuid, size):
        """
        Resize the given VDI which may have active VBDs, which have been paused for the duration of this call.
        :param sr_uuid:
        :param vdi_uuid:
        :param size:
        :return:
        """
        # TODO: Checked. We only support offline resize. If we try "vhd-util resize" on online image it returns ok but VHD will not be resized
        util.SMlog("rbdsr_vhd.RBDVHDVDI.resize_online: sr_uuid=%s, vdi_uuid=%s, size=%s" % (sr_uuid, vdi_uuid, size))

        return self.resize(sr_uuid, vdi_uuid, size, online=True)

    def compose(self, sr_uuid, vdi1_uuid, vdi2_uuid):
        """
        :param sr_uuid:
        :param vdi1_uuid:
        :param vdi2_uuid:
        :return:
        """
        util.SMlog("rbdsr_vhd.RBDVHDVDI.compose: sr_uuid=%s, vdi1_uuid=%s, vdi2_uuid=%s" % (sr_uuid, vdi1_uuid, vdi2_uuid))
        # TODO: Test the method

        base_uuid = vdi1_uuid
        mirror_uuid = vdi2_uuid

        base_path = self.sr._get_path(base_uuid)
        mirror_path = self.sr._get_path(mirror_uuid)

        base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
        mirror_vdi_ref = self.session.xenapi.VDI.get_by_uuid(mirror_uuid)

        base_sm_config = self.session.xenapi.VDI.get_sm_config(base_vdi_ref)
        mirror_sm_config = self.session.xenapi.VDI.get_sm_config(mirror_vdi_ref)

        mirror_hostRefs = self._get_vdi_hostRefs(mirror_uuid)
        local_host_uuid = inventory.get_localhost_uuid()

        for host_uuid in mirror_hostRefs.iterkeys():
            self.attach(sr_uuid, base_uuid, host_uuid=host_uuid)
        if local_host_uuid not in mirror_hostRefs:
            self.attach(sr_uuid, base_uuid, host_uuid=local_host_uuid)

        vhdutil.setParent(mirror_path, base_path, False)
        vhdutil.setHidden(base_path)
        self.sr.session.xenapi.VDI.set_managed(base_vdi_ref, False)

        if 'vhd-parent' in mirror_sm_config:
            self.session.xenapi.VDI.remove_from_sm_config(mirror_vdi_ref, 'vhd-parent')
        self.session.xenapi.VDI.add_to_sm_config(mirror_vdi_ref, 'vhd-parent', base_uuid)
        self.sm_config['vhd-parent'] = base_uuid

        if local_host_uuid not in mirror_hostRefs:
            self.detach(sr_uuid, base_uuid, host_uuid=local_host_uuid)

        if not blktap2.VDI.tap_refresh(self.session, self.sr.uuid, mirror_uuid, True):
            raise util.SMException("failed to refresh VDI %s" % mirror_uuid)

        util.SMlog("Compose done")