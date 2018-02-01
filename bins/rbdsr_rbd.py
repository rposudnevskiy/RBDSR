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
import inventory

VDI_TYPE = 'aio'

DRIVER_TYPE = 'rbd'
DRIVER_CLASS_PREFIX[DRIVER_TYPE] = 'RBDRBD'

VDI_PREFIX = 'RBD-'
#SNAPSHOT_PREFIX = 'SNAP-'
#SXM_PREFIX = 'SXM-'


class RBDRBDSR(CSR):

    def load(self, sr_uuid):
        """
        :param sr_uuid:
        """
        util.SMlog("rbdsr_rbd.RBDRBDSR._load: sr_uuid=%s" % sr_uuid)

        self.VDI_PREFIX = VDI_PREFIX
        #self.SNAPSHOT_PREFIX = SNAPSHOT_PREFIX
        self.vdi_type = VDI_TYPE

        super(RBDRBDSR, self).load(sr_uuid)

    def vdi(self, vdi_uuid):
        """
        Create a VDI class
        :param vdi_uuid:
        :return:
        """
        util.SMlog("rbdsr_rbd.SR.vdi vdi_uuid = %s" % vdi_uuid)

        if vdi_uuid not in self.vdis:
            self.vdis[vdi_uuid] = RBDRBDVDI(self, vdi_uuid)
        return self.vdis[vdi_uuid]


class RBDRBDVDI(CVDI):

    def attach(self, sr_uuid, vdi_uuid, host_uuid=None, dmmode='None'):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :param host_uuid:
        :param dmmode:
        :return:
        """
        # TODO: Test the method
        util.SMlog("rbdsr_dmp.RBDRBDVDI.attach: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if 'base_mirror' in sm_config:
            if 'dmp-parent' in sm_config:
                sxm_vdi_type = 'mirror'
            else:
                sxm_vdi_type = 'base'
        else:
            sxm_vdi_type = 'none'

        if 'base_mirror' in sm_config:  # check if VDI is SXM created vdi
            # ######### SXM VDIs
            if dmmode == 'None':
                if sxm_vdi_type == 'mirror':
                    # it's a mirror vdi of storage migrating VM
                    # it's attached first
                    return super(RBDRBDVDI, self).attach(sr_uuid, vdi_uuid, host_uuid=host_uuid, dmmode='mirror')
                elif sxm_vdi_type == 'base':
                    # it's a base vdi of storage migrating VM
                    # it's attached after mirror VDI and mirror snapshot VDI has been created
                    return super(RBDRBDVDI, self).attach(sr_uuid, vdi_uuid, host_uuid=host_uuid, dmmode='linear')
            else:
                return super(RBDRBDVDI, self).attach(sr_uuid, vdi_uuid, host_uuid=host_uuid, dmmode=dmmode)
        else:
            ########## not SXM VDIs
            return super(RBDRBDVDI, self).attach(sr_uuid, vdi_uuid, host_uuid=host_uuid, dmmode=dmmode)

    def snapshot(self, sr_uuid, vdi_uuid):
        """
        :param sr_uuid:
        :param snap_uuid:
        :return:
        """
        # TODO: Test the method
        util.SMlog("rbdsr_dmp.RBDRBDVDI.snapshot: sr_uuid=%s, snap_uuid=%s" % (sr_uuid, vdi_uuid))

        return self.clone(sr_uuid, vdi_uuid, mode='snapshot')

    def clone(self, sr_uuid, vdi_uuid, mode='clone'):
        """
        :param sr_uuid:
        :param vdi_uuid:
        :return:
        """
        # TODO: Test the method
        util.SMlog("rbdsr_dmp.RBDRBDVDI.clone: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))

        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        is_a_snapshot = self.session.xenapi.VDI.get_is_a_snapshot(vdi_ref)
        label = self.session.xenapi.VDI.get_name_label(vdi_ref)
        description = self.session.xenapi.VDI.get_name_description(vdi_ref)

        # TODO: Should be implemented in 'vhd' and 'dmp' too because clone can be made not only from snap byt from base too
        if 'is_a_base' in sm_config:
            is_a_base = True
        else:
            is_a_base = False

        if mode == 'snapshot' and is_a_snapshot:
            raise util.SMException("Can not make snapshot from snapshot %s" % vdi_uuid)

        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))

        if not is_a_snapshot and not is_a_base:
            base_uuid = util.gen_uuid()
        elif is_a_base:
            base_uuid = vdi_uuid
        else:
            base_uuid = sm_config["rbd-parent"]
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

        cloneVDI.ref = cloneVDI._db_introduce()

        if not is_a_snapshot and not is_a_base:
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

            baseVDI.ref = baseVDI._db_introduce()
        else:
            base_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
            baseVDI = self.sr.vdi(base_uuid)
            baseVDI.path = self.sr._get_path(base_uuid)
            baseVDI.sm_config = self.session.xenapi.VDI.get_sm_config(base_ref)

        if not is_a_snapshot and not is_a_base:
            if 'attached' in sm_config:
                util.SMlog("rbdsr_vhd.RBDDMPVDI.clone: Unmap VDI as it's mapped: sr_uuid=%s, vdi_uuid=%s" % (sr_uuid, vdi_uuid))
                if 'paused' not in sm_config:
                    if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, vdi_uuid):
                        raise util.SMException("failed to pause VDI %s" % vdi_uuid)
                self._unmap_rbd(vdi_uuid, self.size, norefcount=True)
            util.SMlog(
                "rbdsr_vhd.RBDDMPVDI.clone: Transform VDI to BASE and clone it to VDI: sr_uuid=%s, vdi_uuid=%s, \
                base_uuid=%s" % (sr_uuid, vdi_uuid, base_uuid))

            self._rename_rbd(vdi_uuid, base_uuid)
            base_name = "%s%s@BASE" % (self.sr.VDI_PREFIX, base_uuid)
            vdi_name = "%s%s" % (self.sr.VDI_PREFIX, vdi_uuid)
            # Transform VDI to BASE
            util.pread2(
                ["rbd", "snap", "create", base_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
            util.pread2(
                ["rbd", "snap", "protect", base_name, "--pool", self.sr.CEPH_POOL_NAME, "--name",
                 self.sr.CEPH_USER])
            util.pread2(["rbd", "clone", "%s/%s" % (self.sr.CEPH_POOL_NAME, base_name), vdi_name , "--name",
                         self.sr.CEPH_USER])

            if 'rbd-parent' in sm_config:
                baseVDI.sm_config['rbd-parent'] = sm_config['rbd-parent']
                self.session.xenapi.VDI.add_to_sm_config(baseVDI.ref, 'rbd-parent', sm_config['rbd-parent'])
                RBDRBDVDI.update(baseVDI, sr_uuid, base_uuid)
                self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'rbd-parent')
            else:
                RBDRBDVDI.update(baseVDI, sr_uuid, base_uuid)
            self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'rbd-parent', base_uuid)
            self.sm_config['rbd-parent'] = base_uuid
            self.update(sr_uuid, vdi_uuid)

            if 'attached' in sm_config:
                self._map_rbd(vdi_uuid, self.size, norefcount=True)
                if 'paused' not in sm_config:
                    if not blktap2.VDI.tap_unpause(self.session, self.sr.uuid, vdi_uuid, None):
                        raise util.SMException("failed to unpause VDI %s" % vdi_uuid)

        else:
            base_name = "%s%s@BASE" % (self.sr.VDI_PREFIX, base_uuid)

        clone_name = "%s%s" % (self.sr.VDI_PREFIX, clone_uuid)
        util.pread2(["rbd", "clone", "%s/%s" % (self.sr.CEPH_POOL_NAME, base_name), clone_name, "--name",
                     self.sr.CEPH_USER])

        cloneVDI.sm_config["rbd-parent"] = base_uuid
        self.session.xenapi.VDI.add_to_sm_config(cloneVDI.ref, 'rbd-parent', base_uuid)
        RBDRBDVDI.update(cloneVDI, sr_uuid, clone_uuid)

        return cloneVDI.get_params()

    def compose(self, sr_uuid, vdi1_uuid, vdi2_uuid):
        """
        :param sr_uuid:
        :param vdi1_uuid:
        :param vdi2_uuid:
        :return:
        """
        util.SMlog("rbdsr_dmp.RBDRBDVDI.compose: sr_uuid=%s, vdi1_uuid=%s, vdi2_uuid=%s" % (sr_uuid, vdi1_uuid, vdi2_uuid))
        # TODO: Test the method

        base_uuid = vdi1_uuid
        mirror_uuid = vdi2_uuid
        mirror_vdi_ref = self.session.xenapi.VDI.get_by_uuid(mirror_uuid)
        mirror_sm_config = self.session.xenapi.VDI.get_sm_config(mirror_vdi_ref)
        local_host_uuid = inventory.get_localhost_uuid()

        if 'attached' in mirror_sm_config:
            if 'paused' not in mirror_sm_config:
                if not blktap2.VDI.tap_pause(self.session, self.sr.uuid, mirror_uuid):
                    raise util.SMException("failed to pause VDI %s" % mirror_uuid)
            self._unmap_rbd(mirror_uuid, self.rbd_info[1]['size'], devlinks=True, norefcount=True)

        self._map_rbd(mirror_uuid, self.rbd_info[1]['size'], host_uuid=local_host_uuid, dmmode='None', devlinks=True,
                      norefcount=True)
        self._map_rbd(base_uuid, self.rbd_info[1]['size'], host_uuid=local_host_uuid, dmmode='base', devlinks=True,
                      norefcount=True)
        ######## Execute merging dm snapshot to base
        try:
            _mirror_vdi_name = "%s%s" % (VDI_PREFIX, mirror_uuid)
            _mirror_dev_name = "%s/%s" % (self.sr.DEV_ROOT, _mirror_vdi_name)
            _base_vdi_name = "%s%s" % (VDI_PREFIX, base_uuid)
            _base_dev_name = "%s/%s" % (self.sr.DEV_ROOT, _base_vdi_name)
            _base_dm_name = "%s-%s-base" % (self.sr.CEPH_POOL_NAME, _base_vdi_name)

            util.pread2(["dmsetup", "suspend", _base_dm_name])
            util.pread2(["dmsetup", "reload", _base_dm_name, "--table",
                         "0 %s snapshot-merge %s %s P 1" % (str(int(self.rbd_info[1]['size']) / 512), _base_dev_name, _mirror_dev_name)])
            util.pread2(["dmsetup", "resume", _base_dm_name])
            # we should wait until the merge is completed
            util.pread2(["waitdmmerging.sh", _base_dm_name])
        except Exception:
            self._unmap_rbd(mirror_uuid, self.rbd_info[1]['size'], host_uuid=local_host_uuid, devlinks=True,
                          norefcount=True)
            self._unmap_rbd(base_uuid, self.rbd_info[1]['size'], host_uuid=local_host_uuid, devlinks=True,
                          norefcount=True)
            if 'attached' in mirror_sm_config:
                self._map_rbd(mirror_uuid, self.rbd_info[1]['size'], devlinks=True, norefcount=True)
                if 'paused' not in mirror_sm_config:
                    if not blktap2.VDI.tap_unpause(self.session, self.sr.uuid, mirror_uuid, None):
                        raise util.SMException("failed to unpause VDI %s" % mirror_uuid)
        ########
        self._unmap_rbd(base_uuid, self.rbd_info[1]['size'], host_uuid=local_host_uuid, devlinks=True,
                        norefcount=True)
        self._unmap_rbd(mirror_uuid, self.rbd_info[1]['size'], host_uuid=local_host_uuid, devlinks=True,
                        norefcount=True)
        ######## Swap snapshot and base
        tmp_uuid = "temporary"  # util.gen_uuid()
        self._rename_rbd(mirror_uuid, tmp_uuid)
        self._rename_rbd(base_uuid, mirror_uuid)
        self._rename_rbd(tmp_uuid, base_uuid)
        ########

        if 'attached' in mirror_sm_config:
            self._unmap_rbd(mirror_uuid, self.rbd_info[1]['size'], dmmode='None', devlinks=True, norefcount=True)
            if 'paused' not in mirror_sm_config:
                if not blktap2.VDI.tap_unpause(self.session, self.sr.uuid, mirror_uuid, None):
                    raise util.SMException("failed to unpause VDI %s" % mirror_sm_config)

        util.SMlog("Compose done")


class RBDRBDSR_GC(CSR_GC):

    def __init__(self, sr_uuid, xapi, createLock, force):
        """
        :param uuid:
        :param xapi:
        :param createLock:
        :param force:
        """
        util.SMlog("rbdsr_rbd.RBDRBDSR_GC.__init__: sr_uuid = %s" % sr_uuid)

        super(RBDRBDSR_GC, self).__init__(sr_uuid, xapi, createLock, force)

        self.VDI_PREFIX = VDI_PREFIX
        self.vdi_type = VDI_TYPE

    def vdi(self, sr, uuid, raw):
        """
        :param sr:
        :param uuid:
        :param raw:
        :return:
        """
        util.SMlog("rbdsr_rbd.RBDRBDSR_GC.vdi uuid = %s" % uuid)

        return RBDRBDVDI_GC(self, sr, uuid, raw)

class RBDRBDVDI_GC(CVDI_GC):

    def __init__(self, sr, uuid, raw):
        """
        :param sr:
        :param uuid:
        :param raw:
        """
        util.SMlog("rbdsr_rbd.RBDRBDVDI_GC.__init__: uuid = %s" % uuid)

        super(RBDRBDVDI_GC, self).__init__(sr, uuid, raw)