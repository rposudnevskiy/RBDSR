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

from rbdsr_common import *

VDI_TYPE = 'aio'

DRIVER_TYPE = 'dmp'
DRIVER_CLASS_PREFIX[DRIVER_TYPE] = 'RBDDMP'

VDI_PREFIX = 'DMP-'


class RBDDMPSR(CSR):

    def load(self, sr_uuid):
        """
        :param sr_uuid:
        """
        util.SMlog("rbdsr_dmp.RBDDMPSR._load: sr_uuid=%s" % sr_uuid)

        self.VDI_PREFIX = VDI_PREFIX
        self.vdi_type = VDI_TYPE

        super(RBDDMPSR, self).load(sr_uuid)

    def vdi(self, vdi_uuid):
        """
        Create a VDI class
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_dmp.SR.vdi vdi_uuid = %s" % vdi_uuid)

        if vdi_uuid not in self.vdis:
            self.vdis[vdi_uuid] = RBDDMPVDI(self, vdi_uuid)
        return self.vdis[vdi_uuid]


class RBDDMPVDI(CVDI):

    def _map_dmp_chain(self, sr_uuid, vdi_uuid, rbd_size, host_uuid=None, read_only=None, devlinks=True, norefcount=False):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param rbd_size:
        :param host_uuid:
        :param read_only:
        :param dmmode:
        :param devlinks:
        :param norefcount:
        :return:
        """
        util.SMlog("rbdsr_dmp.RBDDMPVDI._map_dmp_chain sr_uuid=%s, vdi_uuid=%s, host_uuid=%s" % (sr_uuid, vdi_uuid, host_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)

        i = 0
        dmp_chain=[]
        #dmp_chain.append(vdi_uuid)
        #i+=1
        parent_sm_config = sm_config
        while 'dmp-parent' in parent_sm_config:
            parent_uuid = parent_sm_config['dmp-parent']
            i+=1
            dmp_chain.append(parent_uuid)
            parent_ref = self.session.xenapi.VDI.get_by_uuid(parent_uuid)
            parent_sm_config = self.session.xenapi.VDI.get_sm_config(parent_ref)
        i_max = i

        is_first_base_mounted = False
        try:
            for uuid_to_map in reversed(dmp_chain):
                if not is_first_base_mounted:
                    self._map_rbd(uuid_to_map, rbd_size , host_uuid, read_only, 'base', devlinks, norefcount)
                    is_first_base_mounted = True
                else:
                    self._map_rbd(uuid_to_map, rbd_size, host_uuid, read_only, 'cow2base', devlinks, norefcount)
                i -= 1
        except:
            for k in range(i, i_max):
                self._unmap_rbd(dmp_chain[k], rbd_size, host_uuid, devlinks, norefcount)

            raise xs_errors.XenError('VDIUnavailable', opterr="Could not map: %s" % dmp_chain[i])

    def _unmap_dmp_chain(self, sr_uuid, vdi_uuid, rbd_size, host_uuid=None, devlinks=True, norefcount=False):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param rbd_size:
        :param host_uuid:
        :param devlinks:
        :param norefcount:
        :return:
        """
        util.SMlog("rbdsr_dmp.RBDDMPVDI._unmap_dmp_chain sr_uuid=%s, vdi_uuid=%s, host_uuid=%s" % (sr_uuid, vdi_uuid, host_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)

        #self._unmap_rbd(vdi_uuid, size, host_uuid, read_only, dmmode)

        parent_sm_config = sm_config
        while 'dmp-parent' in parent_sm_config:
            parent_uuid = parent_sm_config['dmp-parent']
            parent_ref = self.session.xenapi.VDI.get_by_uuid(parent_uuid)
            parent_sm_config = self.session.xenapi.VDI.get_sm_config(parent_ref)

            self._unmap_rbd(parent_uuid, rbd_size, host_uuid, devlinks, norefcount)

    def attach(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        # TODO: Checked. Need to check for 'base_mirror'
        util.SMlog("rbdsr_dmp.RBDDMPVDI.attach: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        is_a_snapshot = self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref)

        if 'base_mirror' in sm_config: # check if VDI is SXM created vdi
            if is_a_snapshot:
                # it's a mirror vdi of storage migrating VM
                # it's attached first
                return super(RBDDMPVDI, self).attach(sr_uuid, vdi_uuid, 'mirror')
            else:
                # it's a base vdi of storage migrating VM
                # it's attached after mirror VDI and mirror snapshot VDI has been created
                return super(RBDDMPVDI, self).attach(sr_uuid, vdi_uuid, 'linear')
        else:
            self._map_dmp_chain(sr_uuid, vdi_uuid, self.rbd_info[1]['size'])

            if 'dmp-parent' in sm_config:
                return super(RBDDMPVDI, self).attach(sr_uuid, vdi_uuid, 'cow')
            else:
                return super(RBDDMPVDI, self).attach(sr_uuid, vdi_uuid, 'linear')

    def detach(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_dmp.RBDDMPVDI.detach: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        super(RBDDMPVDI, self).detach(sr_uuid, vdi_uuid)

        self._unmap_dmp_chain(sr_uuid, vdi_uuid, self.rbd_info[1]['size'])

    def snapshot(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param snap_uuid:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_dmp.RBDDMPVDI.snapshot: sr_uuid=%s, snap_uuid=%s" % (sr_uuid, vdi_uuid))

        return self.clone(sr_uuid, vdi_uuid, mode='snapshot')

    def clone(self, sr_uuid, vdi_uuid, mode='clone'):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        # TODO: Checked
        util.SMlog("rbdsr_dmp.RBDDMPVDI.clone: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        is_a_snapshot = self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref)
        label = self.session.xenapi.VDI.get_name_label(vdi_ref)
        description = self.session.xenapi.VDI.get_name_description(vdi_ref)

        local_host_uuid = inventory.get_localhost_uuid()

        if mode == 'snapshot' and is_a_snapshot:
            raise util.SMException("Can not make snapshot from snapshot %s" % vdi_uuid)

        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))

        if not is_a_snapshot:
            base_uuid = util.gen_uuid()
        else:
            base_uuid = sm_config["dmp-parent"]
        clone_uuid = util.gen_uuid()

        util.SMlog("rbdsr_vhd.RBDDMPVDI.clone: Pepare CloneVDI: sr_uuid=%s, clone_uuid=%s" % (sr_uuid, clone_uuid))
        cloneVDI = self.sr.vdi(clone_uuid)
        cloneVDI.label = "%s (%s)" % (label, mode)
        cloneVDI.description = description
        cloneVDI.path = self.sr._get_path(clone_uuid)
        cloneVDI.location = cloneVDI.uuid
        cloneVDI.size = self.size
        cloneVDI.utilisation = self.size
        cloneVDI.sm_config = dict()
        for key, val in sm_config.iteritems():
            if key not in ["type", "vdi_type", "dmp-parent", "paused", "attached"] and \
                    not key.startswith("host_"):
                cloneVDI.sm_config[key] = val
        if mode == 'snapshot':
            cloneVDI.read_only = True
            cloneVDI.is_a_snapshot = True
            cloneVDI.snapshot_of = vdi_ref

        retval_clone = RBDDMPVDI.create(cloneVDI, sr_uuid, clone_uuid, cloneVDI.size)
        #retval_clone = cloneVDI.create(sr_uuid, clone_uuid, self.rbd_info[1]['size'])
        clone_ref = self.session.xenapi.VDI.get_by_uuid(clone_uuid)

        if not is_a_snapshot:
            util.SMlog("rbdsr_vhd.RBDDMPVDI.clone: Pepare BaseVDI: sr_uuid=%s, base_uuid=%s" % (sr_uuid, base_uuid))
            baseVDI = self.sr.vdi(base_uuid)
            baseVDI.label = "%s (base)" % label
            baseVDI.description = description
            baseVDI.path = self.sr._get_path(base_uuid)
            baseVDI.location = baseVDI.uuid
            baseVDI.managed = False
            baseVDI.size = self.size
            baseVDI.utilisation = self.size
            baseVDI.sm_config = dict()
            baseVDI.read_only = True

            retval_base = RBDDMPVDI.create(baseVDI,sr_uuid, base_uuid, baseVDI.size)
            #retval_base = baseVDI.create(sr_uuid, base_uuid, self.rbd_info[1]['size'])
            base_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
        else:
            base_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
            baseVDI = self.sr.vdi(base_uuid)
            baseVDI.path = self.sr._get_path(base_uuid)
            baseVDI.sm_config = self.session.xenapi.VDI.get_sm_config(base_ref)

        if not is_a_snapshot:
            if 'attached' in sm_config:
                util.SMlog("rbdsr_vhd.RBDDMPVDI.clone: Unmap VDI as it's mapped: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
                if 'paused' not in sm_config:
                    if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                        raise util.SMException("failed to pause VDI %s" % vdi_uuid)
                self._unmap_rbd(vdi_uuid, self.size, norefcount=True)
                base_hostRefs = self._get_vdi_hostRefs(vdi_uuid)
            util.SMlog(
                "rbdsr_vhd.RBDDMPVDI.clone: Swap Base and VDI: sr_uuid=%s, vdi_uuid=%s, base_uuid=%s" % (sr_uuid, vdi_uuid, base_uuid))
            tmp_uuid = "temporary"  # util.gen_uuid()
            self._rename_rbd(vdi_uuid, tmp_uuid)
            self._rename_rbd(base_uuid, vdi_uuid)
            self._rename_rbd(tmp_uuid, base_uuid)

        cloneVDI.sm_config["dmp-parent"] = base_uuid
        self.session.xenapi.VDI.add_to_sm_config(cloneVDI.ref, 'dmp-parent', base_uuid)
        RBDDMPVDI.update(cloneVDI, sr_uuid, clone_uuid)

        if not is_a_snapshot:
            if 'dmp-parent' in sm_config:
                baseVDI.sm_config['dmp-parent'] = sm_config['dmp-parent']
                self.session.xenapi.VDI.add_to_sm_config(baseVDI.ref, 'dmp-parent', sm_config['dmp-parent'])
                RBDDMPVDI.update(baseVDI, sr_uuid, base_uuid)
                self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'dmp-parent')
            else:
                RBDDMPVDI.update(baseVDI, sr_uuid, base_uuid)
            self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'dmp-parent', base_uuid)
            self.sm_config['dmp-parent'] = base_uuid
            self.update(sr_uuid, vdi_uuid)

        if not is_a_snapshot:
            if 'attached' in sm_config:
                for host_uuid in base_hostRefs.iterkeys():
                    if 'dmp-parent' in baseVDI.sm_config:
                        self._map_rbd(base_uuid, self.size, host_uuid=host_uuid, dmmode='cow2base')
                    else:
                        self._map_rbd(base_uuid, self.size, host_uuid=host_uuid, dmmode='base')
                self._map_rbd(vdi_uuid, self.size, dmmode='cow', norefcount=True)
                if 'paused' not in sm_config:
                    if not blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None):
                        raise util.SMException("failed to unpause VDI %s" % vdi_uuid)

        return retval_clone

    def compose(self, sr_uuid, vdi1_uuid, vdi2_uuid):
        """
        :param sr_uuid:
        :param vdi1_uuid:
        :param vdi2_uuid:
        :return:
        """
        util.SMlog("rbdsr_dmp.RBDDMPVDI.compose: sr_uuid=%s, vdi1_uuid=%s, vdi2_uuid=%s" % (sr_uuid, vdi1_uuid, vdi2_uuid))
        # TODO: Test the method

        base_uuid = vdi1_uuid
        mirror_uuid = vdi2_uuid

        base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
        mirror_vdi_ref = self.session.xenapi.VDI.get_by_uuid(mirror_uuid)

        base_sm_config = self.session.xenapi.VDI.get_sm_config(base_vdi_ref)
        mirror_sm_config = self.session.xenapi.VDI.get_sm_config(mirror_vdi_ref)

        if 'attached' in mirror_sm_config:
            if 'paused' not in mirror_sm_config:
                if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, mirror_uuid):
                    raise util.SMException("failed to pause VDI %s" % mirror_uuid)

        self.detach(sr_uuid, mirror_uuid)

        if 'dmp-parent' in mirror_sm_config:
            self.session.xenapi.VDI.remove_from_sm_config(mirror_vdi_ref, 'dmp-parent')
        self.session.xenapi.VDI.add_to_sm_config(mirror_vdi_ref, 'dmp-parent', base_uuid)
        self.sm_config['dmp-parent'] = base_uuid

        self.attach(sr_uuid, mirror_uuid)

        if 'attached' in mirror_sm_config:
            if 'paused' not in mirror_sm_config:
                if not blktap2.VDI.tap_unpause(self.session, self.sr.uuid, mirror_sm_config, None):
                    raise util.SMException("failed to unpause VDI %s" % mirror_sm_config)

        self.sr.session.xenapi.VDI.set_managed(base_vdi_ref, False)

        util.SMlog("Compose done")

    def resize(self, sr_uuid, vdi_uuid, size):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param size:
        :return:
        """
        raise xs_errors.XenError('Unimplemented')

    def resize_online(self, sr_uuid, vdi_uuid, size):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param size:
        :return:
        """
        raise xs_errors.XenError('Unimplemented')