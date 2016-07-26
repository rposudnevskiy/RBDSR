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

from datetime import datetime
from xmlrpclib import DateTime
import time
import SR, VDI, SRCommand, util
import os, re
import xs_errors
import xmlrpclib
import string
import cephutils
import scsiutil
import xml.dom.minidom
import blktap2

CAPABILITIES = ["VDI_CREATE","VDI_DELETE","VDI_ATTACH","VDI_DETACH","VDI_CLONE","VDI_SNAPSHOT", "VDI_RESIZE", "VDI_RESIZE_ONLINE", "ATOMIC_PAUSE", "VDI_UPDATE"
                "SR_SCAN","SR_UPDATE","SR_ATTACH","SR_DETACH","SR_PROBE"]
CONFIGURATION = []
DRIVER_INFO = {
    'name': 'RBD',
    'description': 'Handles virtual disks on CEPH RBD devices',
    'vendor': 'Roman V. Posudnevskiy',
    'copyright': '(c) 2016 Roman V. Posudnevskiy',
    'driver_version': '1.0',
    'required_api_version': '1.0',
    'capabilities': CAPABILITIES,
    'configuration': CONFIGURATION
    }

TYPE = "rbd"

PROVISIONING_TYPES = ["thin", "thick"]
PROVISIONING_DEFAULT = "thick"

MODE_TYPES = ["kernel", "fuse", "nbd"]
MODE_DEFAULT = "fuse"

class RBDSR(SR.SR):
    """Ceph Block Devices storage repository"""
    
    def _loadvdis(self):
        
        if self.vdis:
            return
        
        RBDPOOLs = cephutils.scan_srlist()
        RBDVDIs = cephutils.scan_vdilist(RBDPOOLs[self.uuid])
        
        #xapi_session = self.session.xenapi
        #sm_config = xapi_session.SR.get_sm_config(self.sr_ref)
        vdis = self.session.xenapi.SR.get_VDIs(self.sr_ref)
        vdi_uuids = set([])
        for vdi in vdis:
            vdi_uuids.add(self.session.xenapi.VDI.get_uuid(vdi))

        for vdi_uuid in RBDVDIs.keys():
            #name = RBDVDIs[vdi_uuid]['image']
            if RBDVDIs[vdi_uuid].has_key('snapshot'):
                parent_vdi_uuid = cephutils._get_vdi_uuid(RBDVDIs[vdi_uuid]['image'])
                parent_vdi_info = cephutils._get_vdi_info(self.uuid,parent_vdi_uuid)
                if parent_vdi_info.has_key('VDI_LABEL'):
                    label = parent_vdi_info['VDI_LABEL']
                else:
                    label = ''
                if parent_vdi_info.has_key('VDI_DESCRIPTION'):
                    description = parent_vdi_info['VDI_DESCRIPTION']
                else:
                    description = ''
                if vdi_uuid not in vdi_uuids:
                    #VDI doesn't exist
                    self.vdis[vdi_uuid] = RBDVDI(self, vdi_uuid, label)
                    self.vdis[vdi_uuid].size = str(RBDVDIs[parent_vdi_uuid]['size'])
                    base_vdi_uuid=cephutils._get_vdi_uuid(RBDVDIs[vdi_uuid]['image'])
                    self.vdis[vdi_uuid].is_a_snapshot = True
                    self.vdis[vdi_uuid].description = description
                    if base_vdi_uuid not in vdi_uuids:
                        if self.vdis.has_key(base_vdi_uuid):
                            self.vdis[vdi_uuid].snapshot_of = self.vdis[base_vdi_uuid]
                        else:
                            self.vdis[base_vdi_uuid] = RBDVDI(self, base_vdi_uuid, label)
                            self.vdis[base_vdi_uuid].size = str(RBDVDIs[base_vdi_uuid]['size'])
                            self.vdis[base_vdi_uuid].sm_config["vdi_type"] = 'aio'
                    else:
                        base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(base_vdi_uuid)
                        self.vdis[vdi_uuid].snapshot_of = base_vdi_ref
                    SNAPSHOT_NAME = "%s%s" % (cephutils.SNAPSHOT_PREFIX, vdi_uuid)
                    if parent_vdi_info.has_key(SNAPSHOT_NAME):
                        self.vdis[vdi_uuid].snapshot_time = parent_vdi_info[SNAPSHOT_NAME]
                    self.vdis[vdi_uuid].read_only = True
                    self.vdis[vdi_uuid].sm_config['snapshot-of'] = base_vdi_uuid
                    self.vdis[vdi_uuid].sm_config["vdi_type"] = 'aio'
                    if self.mode == "kernel":
                        self.vdis[vdi_uuid].path = cephutils.get_snap_path_kernel(self.uuid, base_vdi_uuid, vdi_uuid)
                    elif self.mode == "fuse":
                        self.vdis[vdi_uuid].path = cephutils.get_snap_path_fuse(self.uuid, base_vdi_uuid, vdi_uuid)
                    elif self.mode == "nbd":
                        self.vdis[vdi_uuid].path = cephutils.get_snap_path_nbd(self.uuid, base_vdi_uuid, vdi_uuid)
                else:
                    #VDI exists
                    vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
                    self.session.xenapi.VDI.set_virtual_size(vdi_ref, str(RBDVDIs[parent_vdi_uuid]['size']))
                    self.session.xenapi.VDI.set_physical_utilisation(vdi_ref, str(RBDVDIs[parent_vdi_uuid]['size']))
                    base_vdi_uuid=cephutils._get_vdi_uuid(RBDVDIs[vdi_uuid]['image'])
                    base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(base_vdi_uuid)
                    self.session.xenapi.VDI.set_is_a_snapshot(vdi_ref, True)
                    self.session.xenapi.VDI.set_name_description(vdi_ref, description)
                    self.session.xenapi.VDI.set_snapshot_of(vdi_ref, base_vdi_ref)
                    SNAPSHOT_NAME = "%s%s" % (cephutils.SNAPSHOT_PREFIX, vdi_uuid)
                    if parent_vdi_info.has_key(SNAPSHOT_NAME):
                        self.session.xenapi.VDI.set_snapshot_time(vdi_ref, parent_vdi_info[SNAPSHOT_NAME])
                    self.session.xenapi.VDI.set_read_only(vdi_ref, True)
                    self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'snapshot-of')
                    self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'snapshot-of', base_vdi_uuid)
                    #self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'vdi_type', 'aio')
            else:
                vdi_info = cephutils._get_vdi_info(self.uuid,vdi_uuid)
                if vdi_info.has_key('VDI_LABEL'):
                    label = vdi_info['VDI_LABEL']
                else:
                    label = ''
                if vdi_info.has_key('VDI_DESCRIPTION'):
                    description = vdi_info['VDI_DESCRIPTION']
                else:
                    description = ''
                if vdi_uuid not in vdi_uuids:
                    #VDI doesn't exist
                    if not self.vdis.has_key(vdi_uuid):
                        self.vdis[vdi_uuid] = RBDVDI(self, vdi_uuid, label)
                        self.vdis[vdi_uuid].description = description
                        self.vdis[vdi_uuid].size = str(RBDVDIs[vdi_uuid]['size'])
                        self.vdis[vdi_uuid].sm_config["vdi_type"] = 'aio'
                else:
                    #VDI exists
                    vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
                    self.session.xenapi.VDI.set_virtual_size(vdi_ref, str(RBDVDIs[vdi_uuid]['size']))
                    self.session.xenapi.VDI.set_physical_utilisation(vdi_ref, str(RBDVDIs[vdi_uuid]['size']))
                    self.session.xenapi.VDI.set_name_description(vdi_ref, description)
                    #self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'vdi_type', 'aio')
    
    def handles(type):
        """Do we handle this type?"""
        if type == TYPE:
            return True
        return False
    handles = staticmethod(handles)
    
    def content_type(self, sr_uuid):
        """Returns the content_type XML""" 
        return super(RBDSR, self).content_type(sr_uuid)
        #return 'rbd'
    
    def vdi(self, uuid):
        """Create a VDI class"""
        if not self.vdis.has_key(uuid):
            self.vdis[uuid] = RBDVDI(self, uuid, '')
        return self.vdis[uuid]
    
    def probe(self):
        util.SMlog("RBDSR.probe for %s" % self.uuid)
        return cephutils.srlist_toxml(cephutils.scan_srlist())
    
    def load(self, sr_uuid):
        """Initialises the SR"""
        
        self.sr_vditype = 'rbd'
        self.provision = PROVISIONING_DEFAULT
        self.mode = MODE_DEFAULT
        self.uuid = sr_uuid
        
    
    def attach(self, sr_uuid):
        """Std. attach"""
        util.SMlog("RBDSR.attach for %s" % self.uuid)
        
        if not cephutils.check_uuid(sr_uuid):
            raise xs_errors.XenError('SRUnavailable', \
                    opterr='no pool with uuid: %s' % sr_uuid)

        if self.mode == "fuse":
            cephutils.fuse_pool_mount(sr_uuid)
        elif self.mode == "nbd":
            cephutils.nbd_pool_mount(sr_uuid)
    
    def update(self, sr_uuid):
        self.scan(sr_uuid)
    
    def detach(self, sr_uuid):
        """Std. detach"""
        if self.mode == "fuse":
            cephutils.fuse_pool_umount(sr_uuid)
        elif self.mode == "nbd":
            cephutils.nbd_pool_umount(sr_uuid)
    
    def scan(self, sr_uuid):
        """Scan"""
        self.sr_vditype = 'rbd'
        self.provision = PROVISIONING_DEFAULT
        RBDPOOLs = cephutils.scan_srlist()
        self.physical_size = cephutils._get_pool_info(RBDPOOLs[sr_uuid],'size')
        self.physical_utilisation = cephutils._get_pool_info(RBDPOOLs[sr_uuid],'used')
        RBDVDIs = cephutils.scan_vdilist(RBDPOOLs[self.uuid])
        self.virtual_allocation = cephutils.get_allocated_size(RBDVDIs)
        self._loadvdis()
        self._db_update()
        scanrecord = SR.ScanRecord(self)
        scanrecord.synchronise_existing()
        scanrecord.synchronise_new()
    
    def create(self, sr_uuid, size):
        self.attach(sr_uuid)
        self.detach(sr_uuid)
    
    def delete(self, sr_uuid):
        pass
    
    def _updateStats(self, sr_uuid, virtAllocDelta):
        valloc = int(self.session.xenapi.SR.get_virtual_allocation(self.sr_ref))
        self.virtual_allocation = valloc + int(virtAllocDelta)
        self.session.xenapi.SR.set_virtual_allocation(self.sr_ref, str(self.virtual_allocation))
        RBDPOOLs = cephutils.scan_srlist()
        self.session.xenapi.SR.set_physical_utilisation(self.sr_ref, str(cephutils._get_pool_info(RBDPOOLs[sr_uuid],'used')))
    
    def _isSpaceAvailable(self, sr_uuid, size):
        RBDPOOLs = cephutils.scan_srlist()
        sr_free_space = cephutils._get_pool_info(RBDPOOLs[sr_uuid],'size') - cephutils._get_pool_info(RBDPOOLs[sr_uuid],'used')
        if size > sr_free_space:
            return False
        else:
            return True
    

class RBDVDI(VDI.VDI):
    def load(self, vdi_uuid):
        self.loaded   = False
        self.vdi_type = 'aio'
        self.uuid     = vdi_uuid
        self.location = vdi_uuid
        self.mode = self.sr.mode
        if self.mode == "kernel":
            self.path = cephutils.get_blkdev_path(self.sr.uuid, vdi_uuid)
        elif self.mode == "fuse":
            self.path = cephutils.get_fuse_path(self.sr.uuid, vdi_uuid)
        elif self.mode == "nbd":
            self.path = cephutils.get_nbddev_path(self.sr.uuid, vdi_uuid)
        
        self.exists = False
        try:
            RBDVDIs = cephutils.scan_vdilist(RBDPOOLs[self.sr])
            self.size = int(RBDVDIs[vdi_uuid]['size'])
        except:
            pass
    
    def __init__(self, mysr, uuid, label):
        self.uuid = uuid
        VDI.VDI.__init__(self, mysr, uuid)
        self.label = label
        self.vdi_type = 'aio'
        self.read_only = False
        self.shareable = False
        self.issnap = False
        self.hidden = False
        self.sm_config = {}
    
    def create(self, sr_uuid, vdi_uuid, size):
        util.SMlog("RBDVDI.create for %s" % self.uuid)
        if self.exists:
            raise xs_errors.XenError('VDIExists')
        
        if not self.sr._isSpaceAvailable(sr_uuid, size):
            util.SMlog('vdi_resize: vdi size is too big: ' + \
                    '(vdi size: %d, sr free space size: %d)' % (size, sr_free_space))
            raise xs_errors.XenError('VDISize', opterr='vdi size is too big: vdi size: %d, sr free space size: %d'  % (size, sr_free_space))
        
        cephutils.create_vdi(sr_uuid, vdi_uuid, self.label, self.description, size)
        
        self.size = size
        self.utilisation = size
        self.sm_config["vdi_type"] = 'aio'
        
        self.ref = self._db_introduce()
        self.sr._updateStats(self.sr.uuid, self.size)
        
        return VDI.VDI.get_params(self)
    
    def delete(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDVDI.delete for %s" % self.uuid)
        
        vdis = self.session.xenapi.SR.get_VDIs(self.sr.sr_ref)
        clones_uuids = set([])
        has_a_snapshot = False
        has_a_clone = False
        
        for tmp_vdi in vdis:
            tmp_vdi_uuid = self.session.xenapi.VDI.get_uuid(tmp_vdi)
            tmp_sm_config = self.session.xenapi.VDI.get_sm_config(tmp_vdi)
            if tmp_sm_config.has_key("snapshot-of"):
                if tmp_sm_config["snapshot-of"] == vdi_uuid:
                    has_a_snapshot = True
            elif tmp_sm_config.has_key("clone-of"):
                if tmp_sm_config["clone-of"] == vdi_uuid:
                    has_a_clone = True
                    clones_uuids.add(tmp_vdi_uuid)
        
        if has_a_snapshot == True:
            # reverting of VM snapshot
            self_vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
            self.uuid = util.gen_uuid()
            self.location = self.uuid
            if self.mode == "kernel":
                self.path = cephutils.get_blkdev_path(self.sr.uuid, self.uuid)
            elif self.mode == "fuse":
                self.path = cephutils.get_fuse_path(self.sr.uuid, self.uuid)
            elif self.mode == "nbd":
                self.path = cephutils.get_nbddev_path(self.sr.uuid, self.uuid)
            new_vdi_ref = self._db_introduce()
            self.session.xenapi.VDI.set_sm_config(new_vdi_ref, self.session.xenapi.VDI.get_sm_config(self_vdi_ref))
            self.session.xenapi.VDI.add_to_sm_config(new_vdi_ref, 'rollback', 'true')
            self.session.xenapi.VDI.add_to_sm_config(new_vdi_ref, 'orig_uuid', vdi_uuid)
            self.session.xenapi.VDI.set_name_label(new_vdi_ref, self.session.xenapi.VDI.get_name_label(self_vdi_ref))
            self.session.xenapi.VDI.set_is_a_snapshot(new_vdi_ref, self.session.xenapi.VDI.get_is_a_snapshot(self_vdi_ref))
            self.session.xenapi.VDI.set_name_description(new_vdi_ref, self.session.xenapi.VDI.get_name_description(self_vdi_ref))
            self.session.xenapi.VDI.set_snapshot_of(new_vdi_ref, self.session.xenapi.VDI.get_snapshot_of(self_vdi_ref))
            self.session.xenapi.VDI.set_snapshot_time(new_vdi_ref, self.session.xenapi.VDI.get_snapshot_time(self_vdi_ref))
            self.session.xenapi.VDI.set_read_only(new_vdi_ref, self.session.xenapi.VDI.get_read_only(self_vdi_ref))
            self.session.xenapi.VDI.set_metadata_of_pool(new_vdi_ref, self.session.xenapi.VDI.get_metadata_of_pool(self_vdi_ref))
            self.session.xenapi.VDI.set_managed(new_vdi_ref, False)
            self.session.xenapi.VDI.set_physical_utilisation(new_vdi_ref, self.session.xenapi.VDI.get_physical_utilisation(self_vdi_ref))
            self.session.xenapi.VDI.set_virtual_size(new_vdi_ref, self.session.xenapi.VDI.get_virtual_size(self_vdi_ref))
        else:
            # deleting of VDI
            self_vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
            self_sm_config = self.session.xenapi.VDI.get_sm_config(self_vdi_ref)
            if self_sm_config.has_key("snapshot-of"):
                if has_a_clone == True:
                    for clone_uuid in clones_uuids:
                        clone_vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
                        self.session.xenapi.VDI.remove_from_sm_config(clone_vdi_ref, "clone-of")
                        cephutils.flatten_clone(sr_uuid, clone_uuid)
                if self_sm_config.has_key("compose"):
                    cephutils.delete_snapshot(sr_uuid, self_sm_config["compose_vdi1"], vdi_uuid)
                    if self.mode == "kernel":
                        cephutils.delete_vdi_kernel(sr_uuid, self_sm_config["compose_vdi1"])
                    elif self.mode == "fuse":
                        cephutils.delete_vdi_fuse(sr_uuid, self_sm_config["compose_vdi1"])
                    elif self.mode == "nbd":
                        cephutils.delete_vdi_kernel(sr_uuid, self_sm_config["compose_vdi1"])
                    self.sr.forget_vdi(self_sm_config["compose_vdi1"])
                else:
                    cephutils.delete_snapshot(sr_uuid, self_sm_config["snapshot-of"], vdi_uuid)
            else:
                if self.mode == "kernel":
                    cephutils.delete_vdi_kernel(sr_uuid, vdi_uuid)
                elif self.mode == "fuse":
                    cephutils.delete_vdi_fuse(sr_uuid, vdi_uuid)
                elif self.mode == "nbd":
                    cephutils.delete_vdi_kernel(sr_uuid, vdi_uuid)
            self.size = int(self.session.xenapi.VDI.get_virtual_size(self_vdi_ref))
            self.sr._updateStats(self.sr.uuid, -self.size)
            self._db_forget()
    
    def attach(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDVDI.attach for %s" % self.uuid)
        
        vdi_ref = self.sr.srcmd.params['vdi_ref']
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        
        if not hasattr(self,'xenstore_data'):
            self.xenstore_data = {}
        
        self.xenstore_data.update(scsiutil.update_XS_SCSIdata(self.uuid, \
                                                                  scsiutil.gen_synthetic_page_data(self.uuid)))
        
        self.xenstore_data['storage-type']='rbd'
        self.xenstore_data['vdi-type']=self.vdi_type

        self.attached = True
        self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'attached', 'true')
        
        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))
        
        ##########
        vdis = self.session.xenapi.SR.get_VDIs(self.sr.sr_ref)
        has_a_snapshot = False
        for tmp_vdi in vdis:
            tmp_vdi_uuid = self.session.xenapi.VDI.get_uuid(tmp_vdi)
            tmp_sm_config = self.session.xenapi.VDI.get_sm_config(tmp_vdi)
            if tmp_sm_config.has_key("snapshot-of"):
                if tmp_sm_config["snapshot-of"] == vdi_uuid:
                    has_a_snapshot = True
            if tmp_sm_config.has_key("sxm_mirror"):
                    sxm_mirror_vdi = vdi_uuid
        ########## SXM VDIs
        if sm_config.has_key("base_mirror"):
            if has_a_snapshot:
                # it's a mirror vdi of storage migrating VM
                # it's attached first
                self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'sxm_mirror', 'true')
                # creating dm snapshot dev
                if self.mode == "kernel":
                    cephutils.dm_setup_mirror_kernel(sr_uuid, vdi_uuid, self.size)
                elif self.mode == "fuse":
                    cephutils.dm_setup_mirror_fuse(sr_uuid, vdi_uuid, self.size)
                elif self.mode == "nbd":
                    cephutils.dm_setup_mirror_nbd(sr_uuid, vdi_uuid, self.size)
            else:
                # it's a base vdi of storage migrating VM
                # it's attached after mirror VDI and mirror snapshot VDI has been created
                if self.mode == "kernel":
                    cephutils.map_rbd_blkdev(sr_uuid, vdi_uuid)
                elif self.mode == "nbd":
                    cephutils.map_nbd_blkdev(sr_uuid, vdi_uuid)
        ########## not SXM VDIs
        else:
            # it's not SXM VDI, just attach it
            if self.mode == "kernel":
                cephutils.map_rbd_blkdev(sr_uuid, vdi_uuid)
            elif self.mode == "nbd":
                cephutils.map_nbd_blkdev(sr_uuid, vdi_uuid)
        
        if not util.pathexists(self.path):
            raise xs_errors.XenError('VDIUnavailable', \
                    opterr='Could not find: %s' % self.path)
        
        return VDI.VDI.attach(self, self.sr.uuid, self.uuid)
    
    def detach(self, sr_uuid, vdi_uuid):
        if self.mode == "kernel":
            cephutils.unmap_rbd_blkdev(sr_uuid, vdi_uuid)
        elif self.mode == "nbd":
            cephutils.unmap_nbd_blkdev(sr_uuid, vdi_uuid)
        self.attached = False
        vdi_ref = self.sr.srcmd.params['vdi_ref']
        self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'attached')
    
    def clone(self, sr_uuid, snap_uuid):
        util.SMlog("RBDVDI.clone for %s snapshot"% (snap_uuid))

        is_VM_copying = False
        
        snap_vdi_ref = self.session.xenapi.VDI.get_by_uuid(snap_uuid)
        if self.session.xenapi.VDI.get_sharable(snap_vdi_ref):
            return snap_vdi_ref.get_params()

        snap_sm_config = self.session.xenapi.VDI.get_sm_config(snap_vdi_ref)
        if snap_sm_config.has_key("snapshot-of"):
            old_base_uuid = snap_sm_config["snapshot-of"]
        else:
            snapVDI = self._snapshot(sr_uuid, snap_uuid)
            old_base_uuid = snap_uuid
            snap_uuid = snapVDI.uuid
            self.sr.scan(self.sr.uuid)

        base_uuid = None
        
        vdis = self.session.xenapi.SR.get_VDIs(self.sr.sr_ref)
        for tmp_vdi in vdis:
            tmp_vdi_uuid = self.session.xenapi.VDI.get_uuid(tmp_vdi)
            tmp_sm_config = self.session.xenapi.VDI.get_sm_config(tmp_vdi)
            if tmp_sm_config.has_key("orig_uuid"):
                if tmp_sm_config["orig_uuid"] == old_base_uuid:
                    base_uuid = tmp_vdi_uuid
        if not base_uuid:
            base_uuid = old_base_uuid
        
        base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
        base_sm_config = self.session.xenapi.VDI.get_sm_config(base_vdi_ref)
        
        if base_sm_config.has_key("rollback"):
            if base_sm_config["rollback"] == 'true':
                # renaming base image
                cephutils.rename_image(sr_uuid, old_base_uuid, base_uuid)
                # executing rollback of snapshot (reverting VM to snapshot)
                cephutils.rollback_snapshot(sr_uuid, base_uuid, snap_uuid)
                self.session.xenapi.VDI.remove_from_sm_config(base_vdi_ref, 'rollback')
                self.session.xenapi.VDI.remove_from_sm_config(base_vdi_ref, 'orig_uuid')
                self.session.xenapi.VDI.set_managed(base_vdi_ref, True)
                self.session.xenapi.VDI.set_snapshot_of(snap_vdi_ref, base_vdi_ref)
                self.session.xenapi.VDI.remove_from_sm_config(snap_vdi_ref, 'snapshot-of')
                self.session.xenapi.VDI.add_to_sm_config(snap_vdi_ref, 'snapshot-of', base_uuid)
                struct = { 'location': base_uuid,
                           'uuid': base_uuid }
                return xmlrpclib.dumps((struct,), "", True)
        else:
            base_vdi_info = cephutils._get_vdi_info(sr_uuid, base_uuid)
            if base_vdi_info.has_key('VDI_LABEL'):
                base_vdi_label = base_vdi_info['VDI_LABEL']
            else:
                base_vdi_label = ''
            
            clone_uuid = util.gen_uuid()
            
            cloneVDI = RBDVDI(self.sr, clone_uuid, base_vdi_label)
            cephutils.do_clone(self.sr.uuid, base_uuid, snap_uuid, clone_uuid, base_vdi_label)
            
            if self.mode == "kernel":
                cloneVDI.path = cephutils.get_blkdev_path(sr_uuid, clone_uuid)
            elif self.mode == "fuse":
                cloneVDI.path = cephutils.get_fuse_path(sr_uuid, clone_uuid)
            elif self.mode == "nbd":
                cloneVDI.path = cephutils.get_nbddev_path(sr_uuid, clone_uuid)
            cloneVDI.location = cloneVDI.uuid
            cloneVDI.sm_config["vdi_type"] = 'aio'
            cloneVDI.sm_config["clone-of"] = snap_uuid
            
            clone_vdi_ref = cloneVDI._db_introduce()
            self.session.xenapi.VDI.set_physical_utilisation(clone_vdi_ref, self.session.xenapi.VDI.get_physical_utilisation(base_vdi_ref))
            self.session.xenapi.VDI.set_virtual_size(clone_vdi_ref, self.session.xenapi.VDI.get_virtual_size(base_vdi_ref))
            self.sr._updateStats(self.sr.uuid, self.session.xenapi.VDI.get_virtual_size(base_vdi_ref))
            
            return cloneVDI.get_params()

    def snapshot(self, sr_uuid, vdi_uuid):
        return self._snapshot(self, sr_uuid, vdi_uuid).get_params()
    
    def _snapshot(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDVDI.snapshot for %s" % (vdi_uuid))
        
        secondary = None
        
        if not blktap2.VDI.tap_pause(self.session, sr_uuid, vdi_uuid):
            raise util.SMException("failed to pause VDI %s" % vdi_uuid)
        
        vdi_ref = self.sr.srcmd.params['vdi_ref']
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        
        base_uuid = vdi_uuid
        snap_uuid = util.gen_uuid()
        
        vdi_info = cephutils._get_vdi_info(sr_uuid, vdi_uuid)
        if vdi_info.has_key('VDI_LABEL'):
            orig_label = vdi_info['VDI_LABEL']
        else:
            orig_label = ''
        
        snapVDI = RBDVDI(self.sr, snap_uuid, "%s%s" % (orig_label, " (snapshot)"))
        cephutils.do_snapshot(self.sr.uuid, base_uuid, snap_uuid)
        
        if self.mode == "kernel":
            snapVDI.path = cephutils.get_snap_path_kernel(self.sr.uuid, base_uuid, snap_uuid)
        elif self.mode == "fuse":
            snapVDI.path = cephutils.get_snap_path_fuse(self.sr.uuid, base_uuid, snap_uuid)
        elif self.mode == "nbd":
            snapVDI.path = cephutils.get_snap_path_nbd(self.sr.uuid, base_uuid, snap_uuid)
        snapVDI.issnap = True
        snapVDI.read_only = True
        snapVDI.location = snapVDI.uuid
        snapVDI.size = self.size
        snapVDI.utilisation = self.utilisation
        snapVDI.sm_config["vdi_type"] = 'aio'
        snapVDI.sm_config["snapshot-of"] = base_uuid
        
        snap_vdi_ref = snapVDI._db_introduce()
        
        self.session.xenapi.VDI.set_physical_utilisation(snap_vdi_ref, self.session.xenapi.VDI.get_physical_utilisation(vdi_ref))
        self.session.xenapi.VDI.set_virtual_size(snap_vdi_ref, self.session.xenapi.VDI.get_virtual_size(vdi_ref))
        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))
        self.sr._updateStats(self.sr.uuid, self.size)
        
        blktap2.VDI.tap_unpause(self.session, sr_uuid, vdi_uuid, secondary)
        
        return snapVDI
    
    def resize(self, sr_uuid, vdi_uuid, size):
        """Resize the given VDI to size <size> MB. Size can
        be any valid disk size greater than [or smaller than]
        the current value."""
        util.SMlog("RBDVDI.resize for %s" % self.uuid)
        
        if not self.sr._isSpaceAvailable(sr_uuid, size):
            util.SMlog('vdi_resize: vdi size is too big: ' + \
                    '(vdi size: %d, sr free space size: %d)' % (size, sr_free_space))
            raise xs_errors.XenError('VDISize', opterr='vdi size is too big')
        
        if size < self.size:
            util.SMlog('vdi_resize: shrinking not supported yet: ' + \
                    '(current size: %d, new size: %d)' % (self.size, size))
            raise xs_errors.XenError('VDISize', opterr='shrinking not allowed')
        
        if size == self.size:
            return VDI.VDI.get_params(self)
        
        oldSize = self.size
        cephutils.resize(sr_uuid, vdi_uuid, size)
        
        self.size = size
        self.utilisation = self.size
        
        vdi_ref = self.sr.srcmd.params['vdi_ref']
        self.session.xenapi.VDI.set_virtual_size(vdi_ref, str(self.size))
        self.session.xenapi.VDI.set_physical_utilisation(vdi_ref,
                str(self.utilisation))
        self.sr._updateStats(self.sr.uuid, self.size - oldSize)
        return VDI.VDI.get_params(self)        
    
    def resize_online(self, sr_uuid, vdi_uuid, size):
        """Resize the given VDI which may have active VBDs, which have
        been paused for the duration of this call."""
        return resize(sr_uuid, vdi_uuid, size)
    
    def compose(self, sr_uuid, vdi1_uuid, vdi2_uuid):
        util.SMlog("RBDSR.compose for %s -> %s" % (vdi2_uuid, vdi1_uuid))
        
        if not blktap2.VDI.tap_pause(self.session, sr_uuid, vdi2_uuid):
            raise util.SMException("failed to pause VDI %s" % vdi2_uuid)
        
        vdis = self.session.xenapi.SR.get_VDIs(self.sr.sr_ref)
        for tmp_vdi in vdis:
            tmp_vdi_uuid = self.session.xenapi.VDI.get_uuid(tmp_vdi)
            tmp_sm_config = self.session.xenapi.VDI.get_sm_config(tmp_vdi)
            if tmp_sm_config.has_key("snapshot-of"):
                if tmp_sm_config["snapshot-of"] == vdi2_uuid:
                    snap_vdi_ref = self.session.xenapi.VDI.get_by_uuid(tmp_vdi_uuid)
                    snap_uuid = tmp_vdi_uuid
        
        self.session.xenapi.VDI.add_to_sm_config(snap_vdi_ref, 'compose', 'true')
        self.session.xenapi.VDI.add_to_sm_config(snap_vdi_ref, 'compose_vdi1', vdi1_uuid)
        self.session.xenapi.VDI.add_to_sm_config(snap_vdi_ref, 'compose_vdi2', vdi2_uuid)
        
        vdi1_ref = self.session.xenapi.VDI.get_by_uuid(vdi1_uuid)
        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi1_ref))
        
        if self.mode == "kernel":
            cephutils.merge_diff_kernel(sr_uuid, vdi2_uuid, snap_uuid, vdi1_uuid, self.size)
        elif self.mode == "fuse":
            cephutils.merge_diff_fuse(sr_uuid, vdi2_uuid, snap_uuid, vdi1_uuid, self.size)
        elif self.mode == "nbd":
            cephutils.merge_diff_nbd(sr_uuid, vdi2_uuid, snap_uuid, vdi1_uuid, self.size)
        
        self.session.xenapi.VDI.remove_from_sm_config(snap_vdi_ref, 'snapshot-of')
        self.session.xenapi.VDI.add_to_sm_config(snap_vdi_ref, 'snapshot-of', vdi1_uuid)
        
        blktap2.VDI.tap_unpause(self.session, sr_uuid, vdi2_uuid, None)
    
    def update(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDSR.update for %s" % vdi_uuid)
        
        self_vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        
        if not self.session.xenapi.VDI.get_is_a_snapshot(self_vdi_ref):
            vdis = self.session.xenapi.SR.get_VDIs(self.sr.sr_ref)
            snapshots = {}
            has_snapshots = False
            
            for tmp_vdi_ref in vdis:
                tmp_vdi_uuid = self.session.xenapi.VDI.get_uuid(tmp_vdi_ref)
                tmp_sm_config = self.session.xenapi.VDI.get_sm_config(tmp_vdi_ref)
                if tmp_sm_config.has_key("snapshot-of"):
                    if tmp_sm_config["snapshot-of"] == vdi_uuid:
                        has_snapshots = True
                        snapshots[tmp_vdi_uuid]=self.session.xenapi.VDI.get_snapshot_time(tmp_vdi_ref)
            
            label=self.session.xenapi.VDI.get_name_label(self_vdi_ref)
            description=self.session.xenapi.VDI.get_name_description(self_vdi_ref)
            
            cephutils.update_vdi(sr_uuid, vdi_uuid, label, description, snapshots)
            if has_snapshots == True:
                for snapshot_uuid in snapshots.keys():
                    snapshot_vdi_ref = self.session.xenapi.VDI.get_by_uuid(snapshot_uuid)
                    self.session.xenapi.VDI.set_name_label(snapshot_vdi_ref, self.session.xenapi.VDI.get_name_label(self_vdi_ref))
                    self.session.xenapi.VDI.set_name_description(snapshot_vdi_ref, self.session.xenapi.VDI.get_name_description(self_vdi_ref))
        else:
            self_vdi_sm_config = self.session.xenapi.VDI.get_sm_config(self_vdi_ref)
            base_vdi_uuid = self_vdi_sm_config["snapshot-of"]
            base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(base_vdi_uuid)
            label=self.session.xenapi.VDI.get_name_label(base_vdi_ref)
            description=self.session.xenapi.VDI.get_name_description(base_vdi_ref)
            snapshots = {}
            snapshots[vdi_uuid]=self.session.xenapi.VDI.get_snapshot_time(self_vdi_ref)
            cephutils.update_vdi(sr_uuid, base_vdi_uuid, label, description, snapshots)

if __name__ == '__main__':
    SRCommand.run(RBDSR, DRIVER_INFO)
else:
    SR.registerSR(RBDSR)
