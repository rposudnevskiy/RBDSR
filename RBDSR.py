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
import vhdutil

CAPABILITIES = ["VDI_CREATE", "VDI_DELETE", "VDI_ATTACH", "VDI_DETACH", "VDI_CLONE",
                "VDI_SNAPSHOT", "VDI_INTRODUCE", "VDI_RESIZE", "VDI_RESIZE_ONLINE",
                "ATOMIC_PAUSE", "VDI_UPDATE", "SR_SCAN", "SR_UPDATE", "SR_ATTACH",
                "SR_DETACH", "SR_PROBE", "SR_METADATA", "VDI_GENERATE_CONFIG"]

CONFIGURATION = [['rbd-mode', 'SR mount mode (optional): kernel, fuse, nbd (default)'],
                 ['cephx-id', 'Cephx id to be used (optional): default is admin'],
                ]

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
MODE_DEFAULT = "nbd"

DEFAULT_CEPH_USER = 'admin'

class RBDSR(SR.SR, cephutils.SR):
    """Ceph Block Devices storage repository"""
    
    def handles(type):
        """Do we handle this type?"""
        util.SMlog("RBDSR.handles type %s" % type)
        if type == "rbd":
            return True
        else:
            return False
    handles = staticmethod(handles)
    
    def _loadvdis(self):
        
        if self.vdis:
            return
        
        RBDVDIs = self._get_vdilist(self.CEPH_POOL_NAME)
        
        #xapi_session = self.session.xenapi
        #sm_config = xapi_session.SR.get_sm_config(self.sr_ref)
        vdis = self.session.xenapi.SR.get_VDIs(self.sr_ref)
        vdi_uuids = set([])
        for vdi in vdis:
            vdi_uuids.add(self.session.xenapi.VDI.get_uuid(vdi))
        
        for vdi_uuid in RBDVDIs.keys():
            #name = RBDVDIs[vdi_uuid]['image']
            if RBDVDIs[vdi_uuid].has_key('snapshot'):
                parent_vdi_uuid = self._get_vdi_uuid(RBDVDIs[vdi_uuid]['image'])
                parent_vdi_info = self._get_vdi_info(parent_vdi_uuid)
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
                    self.vdis[vdi_uuid].is_a_snapshot = True
                    self.vdis[vdi_uuid].description = description
                    if parent_vdi_uuid not in vdi_uuids:
                        if self.vdis.has_key(parent_vdi_uuid):
                            self.vdis[vdi_uuid].snapshot_of = self.vdis[parent_vdi_uuid]
                        else:
                            self.vdis[parent_vdi_uuid] = RBDVDI(self, parent_vdi_uuid, label)
                            self.vdis[parent_vdi_uuid].size = str(RBDVDIs[parent_vdi_uuid]['size'])
                            self.vdis[parent_vdi_uuid].sm_config["vdi_type"] = 'aio'
                    else:
                        base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(parent_vdi_uuid)
                        self.vdis[vdi_uuid].snapshot_of = base_vdi_ref
                    if parent_vdi_info.has_key(RBDVDIs[vdi_uuid]['snapshot']):
                        self.vdis[vdi_uuid].snapshot_time = str(parent_vdi_info[RBDVDIs[vdi_uuid]['snapshot']])
                    self.vdis[vdi_uuid].read_only = True
                    self.vdis[vdi_uuid].sm_config['snapshot-of'] = parent_vdi_uuid
                    self.vdis[vdi_uuid].sm_config["vdi_type"] = 'aio'
                    self.vdis[vdi_uuid].path = self._get_snap_path(parent_vdi_uuid, vdi_uuid)
                else:
                    #VDI exists
                    vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
                    self.session.xenapi.VDI.set_virtual_size(vdi_ref, str(RBDVDIs[parent_vdi_uuid]['size']))
                    self.session.xenapi.VDI.set_physical_utilisation(vdi_ref, str(RBDVDIs[parent_vdi_uuid]['size']))
                    if parent_vdi_uuid not in vdi_uuids:
                        self.vdis[parent_vdi_uuid] = RBDVDI(self, parent_vdi_uuid, label)
                        self.vdis[parent_vdi_uuid].description = description
                        self.vdis[parent_vdi_uuid].size = str(RBDVDIs[parent_vdi_uuid]['size'])
                        self.vdis[parent_vdi_uuid].sm_config["vdi_type"] = 'aio'
                        try:
                            parent_vdi_ref = self.vdis[parent_vdi_uuid]._db_introduce()
                        except Exception:
                            continue
                    else:
                        parent_vdi_ref = self.session.xenapi.VDI.get_by_uuid(parent_vdi_uuid)
                    self.session.xenapi.VDI.set_is_a_snapshot(vdi_ref, True)
                    self.session.xenapi.VDI.set_name_description(vdi_ref, description)
                    self.session.xenapi.VDI.set_snapshot_of(vdi_ref, parent_vdi_ref)
                    if parent_vdi_info.has_key(RBDVDIs[vdi_uuid]['snapshot']):
                        self.session.xenapi.VDI.set_snapshot_time(vdi_ref, str(parent_vdi_info[RBDVDIs[vdi_uuid]['snapshot']]))
                    self.session.xenapi.VDI.set_read_only(vdi_ref, True)
                    self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'snapshot-of')
                    self.session.xenapi.VDI.add_to_sm_config(vdi_ref, 'snapshot-of', parent_vdi_uuid)
            else:
                vdi_info = self._get_vdi_info(vdi_uuid)
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
    
    def content_type(self, sr_uuid):
        """Returns the content_type XML""" 
        return SR.SR.content_type(self, sr_uuid)
    
    def vdi(self, uuid):
        """Create a VDI class"""
        if not self.vdis.has_key(uuid):
            self.vdis[uuid] = RBDVDI(self, uuid, '')
        return self.vdis[uuid]
    
    def probe(self):
        util.SMlog("RBDSR.probe for %s" % self.uuid)
        return self._srlist_toxml()

    def load(self, sr_uuid):
        """Initialises the SR"""
        self.provision = PROVISIONING_DEFAULT
        self.mode = MODE_DEFAULT
        self.uuid = sr_uuid
        ceph_user = DEFAULT_CEPH_USER
        if self.dconf.has_key('cephx-id'):
            ceph_user = self.dconf.get('cephx-id')
            util.SMlog("RBDSR.load using cephx id %s" % ceph_user)
        
        if self.dconf.has_key('rbd-mode'):
            self.mode = self.dconf['rbd-mode']
        
        cephutils.SR.load(self,sr_uuid, ceph_user)
    
    def attach(self, sr_uuid):
        """Std. attach"""
        util.SMlog("RBDSR.attach for %s" % self.uuid)
        
        if not self.RBDPOOLs.has_key(self.uuid):
            raise xs_errors.XenError('SRUnavailable',opterr='no pool with uuid: %s' % sr_uuid)
        
        cephutils.SR.attach(self, sr_uuid)
    
    def update(self, sr_uuid):
        self.scan(sr_uuid)
    
    def detach(self, sr_uuid):
        """Std. detach"""
        cephutils.SR.detach(self, sr_uuid)
    
    def scan(self, sr_uuid):
        """Scan"""
        cephutils.SR.scan(self, sr_uuid)
        
        self.physical_size = self.RBDPOOLs[sr_uuid]['stats']['max_avail'] + self.RBDPOOLs[sr_uuid]['stats']['bytes_used']
        self.physical_utilisation = self.RBDPOOLs[sr_uuid]['stats']['bytes_used']
        
        self.virtual_allocation = self._get_allocated_size()
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
        
        self.session.xenapi.SR.set_physical_utilisation(self.sr_ref, str(self.RBDPOOLs[sr_uuid]['stats']['bytes_used']))

class RBDVDI(VDI.VDI, cephutils.VDI):

    def load(self, vdi_uuid):
        self.loaded   = False
        self.vdi_type = 'aio'
        self.uuid     = vdi_uuid
        self.location = vdi_uuid
        self.mode = self.sr.mode
        self.exists = False
        #---
        RBDVDIs=self.sr._get_vdilist(self.sr.CEPH_POOL_NAME)
        if RBDVDIs.has_key(vdi_uuid):
            if RBDVDIs[vdi_uuid].has_key("snapshot"):
                base_image_name=RBDVDIs[vdi_uuid]["image"]
                base_uuid=self.sr._get_vdi_uuid(base_image_name)
                # it's a snapshot VDI
                self.path = self.sr._get_snap_path(base_uuid, vdi_uuid)
            else:
                self.path = self.sr._get_path(vdi_uuid)
        #---
        cephutils.VDI.load(self, vdi_uuid)
    
    def __init__(self, mysr, uuid, label):
        self.uuid = uuid
        self.location = uuid
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
        
        if not self.sr._isSpaceAvailable(size):
            util.SMlog('RBDVDI.create: vdi size is too big: ' + \
                    '(vdi size: %d, sr free space size: %d)' % (size, self.sr.RBDPOOLs[sr_uuid]['stats']['max_avail']))
            raise xs_errors.XenError('VDISize', opterr='vdi size is too big: vdi size: %d, sr free space size: %d'  % (size, self.sr.RBDPOOLs[sr_uuid]['stats']['max_avail']))
        
        cephutils.VDI.create(self, sr_uuid, vdi_uuid, size)
        
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
            new_uuid = util.gen_uuid()
            self.snaps = self.session.xenapi.VDI.get_snapshots(self_vdi_ref)
            # renaming base image
            self._rename_image(vdi_uuid, new_uuid)
            for snap in self.snaps:
                util.SMlog("RBDVDI.delete set rollback for %s" % self.session.xenapi.VDI.get_uuid(snap))
                self.session.xenapi.VDI.add_to_sm_config(snap, 'new_uuid', new_uuid)
                self.session.xenapi.VDI.add_to_sm_config(snap, 'rollback', 'true')
        else:
            # deleting of VDI
            self_vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
            self_sm_config = self.session.xenapi.VDI.get_sm_config(self_vdi_ref)
            if self_sm_config.has_key("snapshot-of"):
                if has_a_clone == True:
                    for clone_uuid in clones_uuids:
                        clone_vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
                        self.session.xenapi.VDI.remove_from_sm_config(clone_vdi_ref, "clone-of")
                        self._flatten_clone(clone_uuid)
                if self_sm_config.has_key("compose"):
                    self._delete_snapshot(self_sm_config["compose_vdi1"], vdi_uuid)
                    self._delete_vdi(self_sm_config["compose_vdi1"])
                    self.sr.forget_vdi(self_sm_config["compose_vdi1"])
                else:
                    self._delete_snapshot(self_sm_config["snapshot-of"], vdi_uuid)
            else:
                self._delete_vdi(vdi_uuid)
            self.size = int(self.session.xenapi.VDI.get_virtual_size(self_vdi_ref))
            self.sr._updateStats(self.sr.uuid, -self.size)
            self._db_forget()
    
    def attach(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDVDI.attach for %s" % self.uuid)
        
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        
        if sm_config.has_key("snapshot-of"):
            base_uuid = sm_config["snapshot-of"]
            # it's a snapshot VDI
            self.path = self.sr._get_snap_path(base_uuid, vdi_uuid)
        else:
            self.path = self.sr._get_path(vdi_uuid)
        
        if not hasattr(self,'xenstore_data'):
            self.xenstore_data = {}
        
        self.xenstore_data.update(scsiutil.update_XS_SCSIdata(self.uuid, scsiutil.gen_synthetic_page_data(self.uuid)))
        
        self.xenstore_data['storage-type']='rbd'
        self.xenstore_data['vdi-type']=self.vdi_type
        
        self.attached = True
        self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'attached')
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
                self._setup_mirror(vdi_uuid, self.size)
            else:
                # it's a base vdi of storage migrating VM
                # it's attached after mirror VDI and mirror snapshot VDI has been created
                self._map_VHD(vdi_uuid)
        ########## not SXM VDIs
        elif sm_config.has_key("snapshot-of"):
            base_uuid = sm_config["snapshot-of"]
            # it's a snapshot VDI, attach it as snapshot
            self._map_SNAP(base_uuid, vdi_uuid)
        else:
            # it's not SXM VDI, just attach it
            self._map_VHD(vdi_uuid)
        
        if not util.pathexists(self.path):
            raise xs_errors.XenError('VDIUnavailable', opterr='Could not find: %s' % self.path)
        
        return VDI.VDI.attach(self, self.sr.uuid, self.uuid)
    
    def detach(self, sr_uuid, vdi_uuid):
        vdi_ref = self.sr.srcmd.params['vdi_ref']
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        if sm_config.has_key("snapshot-of"):
            base_uuid = sm_config["snapshot-of"]
            # it's a snapshot VDI, detach it as snapshot
            self._unmap_SNAP(base_uuid, vdi_uuid)
        else:
            self._unmap_VHD(vdi_uuid)
        self.attached = False
        self.session.xenapi.VDI.remove_from_sm_config(vdi_ref, 'attached')
    
    def clone(self, sr_uuid, snap_uuid):
        util.SMlog("RBDVDI.clone for %s snapshot"% (snap_uuid))
        
        snap_vdi_ref = self.session.xenapi.VDI.get_by_uuid(snap_uuid)
        if self.session.xenapi.VDI.get_sharable(snap_vdi_ref):
            return snap_vdi_ref.get_params()
        
        snap_sm_config = self.session.xenapi.VDI.get_sm_config(snap_vdi_ref)
        if snap_sm_config.has_key("snapshot-of"):
            base_uuid = snap_sm_config["snapshot-of"]
        else:
            snapVDI = self._snapshot(sr_uuid, snap_uuid)
            base_uuid = snap_uuid
            snap_uuid = snapVDI.uuid
            self.sr.scan(self.sr.uuid)
        
        util.SMlog("RBDVDI.clone base_uuid = %s"% (base_uuid))
        
        if snap_sm_config.has_key("rollback"):
            if snap_sm_config["rollback"] == 'true':
                util.SMlog("RBDVDI.clone reverting %s to %s"% (snap_uuid, base_uuid))
                # executing rollback of snapshot (reverting VM to snapshot)
                new_uuid = snap_sm_config["new_uuid"]
                self._rollback_snapshot(new_uuid, snap_uuid)
                
                baseVDI = RBDVDI(self.sr, new_uuid, self.session.xenapi.VDI.get_name_label(snap_vdi_ref))
                baseVDI.path = self.sr._get_path(new_uuid)
                baseVDI.location = baseVDI.uuid
                baseVDI.size = self.session.xenapi.VDI.get_virtual_size(snap_vdi_ref)
                baseVDI.sm_config["vdi_type"] = 'aio'
                baseVDI.sm_config["reverted"] = 'true'
                base_vdi_ref = baseVDI._db_introduce()
                
                vdis = self.session.xenapi.SR.get_VDIs(self.sr.sr_ref)
                for tmp_vdi in vdis:
                    tmp_vdi_uuid = self.session.xenapi.VDI.get_uuid(tmp_vdi)
                    tmp_sm_config = self.session.xenapi.VDI.get_sm_config(tmp_vdi)
                    if tmp_sm_config.has_key("rollback"):
                        if tmp_sm_config.has_key("new_uuid"):
                            if tmp_sm_config["new_uuid"] == new_uuid:
                                sm_config = self.session.xenapi.VDI.get_sm_config(tmp_vdi)
                                del sm_config['snapshot-of']
                                sm_config['snapshot-of'] = new_uuid
                                del sm_config['rollback']
                                del sm_config['new_uuid']
                                self.session.xenapi.VDI.set_sm_config(tmp_vdi, sm_config)
                
                return baseVDI.get_params()
        else:
            base_vdi_info = self._get_vdi_info(base_uuid)
            base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(base_uuid)
            if base_vdi_info.has_key('VDI_LABEL'):
                base_vdi_label = base_vdi_info['VDI_LABEL']
            else:
                base_vdi_label = ''
            
            clone_uuid = util.gen_uuid()
            
            cloneVDI = RBDVDI(self.sr, clone_uuid, base_vdi_label)
            self._do_clone(base_uuid, snap_uuid, clone_uuid, base_vdi_label)
            
            cloneVDI.path = self.sr._get_path(clone_uuid)
            cloneVDI.location = cloneVDI.uuid
            cloneVDI.sm_config["vdi_type"] = 'aio'
            cloneVDI.sm_config["clone-of"] = snap_uuid
            
            clone_vdi_ref = cloneVDI._db_introduce()
            self.session.xenapi.VDI.set_physical_utilisation(clone_vdi_ref, self.session.xenapi.VDI.get_physical_utilisation(base_vdi_ref))
            self.session.xenapi.VDI.set_virtual_size(clone_vdi_ref, self.session.xenapi.VDI.get_virtual_size(base_vdi_ref))
            self.sr._updateStats(self.sr.uuid, self.session.xenapi.VDI.get_virtual_size(base_vdi_ref))
            
            return cloneVDI.get_params()
    
    def snapshot(self, sr_uuid, vdi_uuid):
        return self._snapshot(sr_uuid, vdi_uuid).get_params()
    
    def _snapshot(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDVDI.snapshot for %s" % (vdi_uuid))
        
        #secondary = None
        
        #if not blktap2.VDI.tap_pause(self.session, sr_uuid, vdi_uuid):
        #    raise util.SMException("failed to pause VDI %s" % vdi_uuid)
        
        vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        sm_config = self.session.xenapi.VDI.get_sm_config(vdi_ref)
        
        base_uuid = vdi_uuid
        snap_uuid = util.gen_uuid()
        
        vdi_info = self._get_vdi_info(vdi_uuid)
        if vdi_info.has_key('VDI_LABEL'):
            orig_label = vdi_info['VDI_LABEL']
        else:
            orig_label = ''
        
        snapVDI = RBDVDI(self.sr, snap_uuid, "%s%s" % (orig_label, " (snapshot)"))
        self._do_snapshot(base_uuid, snap_uuid)
        
        snapVDI.path = self.sr._get_snap_path(base_uuid, snap_uuid)
        snapVDI.issnap = True
        snapVDI.read_only = True
        snapVDI.location = snapVDI.uuid
        snapVDI.snapshot_of = vdi_ref
        snapVDI.size = self.session.xenapi.VDI.get_virtual_size(vdi_ref)
        snapVDI.sm_config["vdi_type"] = 'aio'
        snapVDI.sm_config["snapshot-of"] = base_uuid
        
        snap_vdi_ref = snapVDI._db_introduce()
        
        self.session.xenapi.VDI.set_physical_utilisation(snap_vdi_ref, self.session.xenapi.VDI.get_physical_utilisation(vdi_ref))
        self.session.xenapi.VDI.set_virtual_size(snap_vdi_ref, self.session.xenapi.VDI.get_virtual_size(vdi_ref))
        self.size = int(self.session.xenapi.VDI.get_virtual_size(vdi_ref))
        self.sr._updateStats(self.sr.uuid, self.size)
        
        #blktap2.VDI.tap_unpause(self.session, sr_uuid, vdi_uuid, secondary)
        
        return snapVDI
    
    def resize(self, sr_uuid, vdi_uuid, size):
        """Resize the given VDI to size <size> MB. Size can
        be any valid disk size greater than [or smaller than]
        the current value."""
        util.SMlog("RBDVDI.resize for %s" % self.uuid)
        
        if not self.sr._isSpaceAvailable(size):
            util.SMlog('vdi_resize: vdi size is too big: ' + \
                    '(vdi size: %d, sr free space size: %d)' % (size, self.sr.RBDPOOLs[sr_uuid]['stats']['max_avail']))
            raise xs_errors.XenError('VDISize', opterr='vdi size is too big')
        
        if size < self.size:
            util.SMlog('vdi_resize: shrinking not supported yet: ' + \
                    '(current size: %d, new size: %d)' % (self.size, size))
            raise xs_errors.XenError('VDISize', opterr='shrinking not allowed')
        
        if size == self.size:
            return VDI.VDI.get_params(self)
        
        oldSize = self.size
        cephutils.VDI.resize(self, sr_uuid, vdi_uuid, size)
        
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
        
        #if not blktap2.VDI.tap_pause(self.session, sr_uuid, vdi2_uuid):
        #    raise util.SMException("failed to pause VDI %s" % vdi2_uuid)
        
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
        
        self._merge_diffs(vdi2_uuid, snap_uuid, vdi1_uuid, self.size)
        
        self.session.xenapi.VDI.remove_from_sm_config(snap_vdi_ref, 'snapshot-of')
        self.session.xenapi.VDI.add_to_sm_config(snap_vdi_ref, 'snapshot-of', vdi1_uuid)
        
        #blktap2.VDI.tap_unpause(self.session, sr_uuid, vdi2_uuid, None)
    
    def update(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDSR.update for %s" % vdi_uuid)
        
        self_vdi_ref = self.session.xenapi.VDI.get_by_uuid(vdi_uuid)
        
        if not self.session.xenapi.VDI.get_is_a_snapshot(self_vdi_ref):
            vdis = self.session.xenapi.SR.get_VDIs(self.sr.sr_ref)
            self.snaps = {}
            has_snapshots = False
            
            for tmp_vdi_ref in vdis:
                tmp_vdi_uuid = self.session.xenapi.VDI.get_uuid(tmp_vdi_ref)
                tmp_sm_config = self.session.xenapi.VDI.get_sm_config(tmp_vdi_ref)
                if tmp_sm_config.has_key("snapshot-of"):
                    if tmp_sm_config["snapshot-of"] == vdi_uuid:
                        has_snapshots = True
                        self.snaps[tmp_vdi_uuid]=self.session.xenapi.VDI.get_snapshot_time(tmp_vdi_ref)
            
            self.label = self.session.xenapi.VDI.get_name_label(self_vdi_ref)
            self.description = self.session.xenapi.VDI.get_name_description(self_vdi_ref)
            
            cephutils.VDI.update(self, sr_uuid, vdi_uuid)
            
            sm_config = self.session.xenapi.VDI.get_sm_config(self_vdi_ref)
            if sm_config.has_key('reverted'):
                 if sm_config['reverted'] == 'true':
                     del sm_config['reverted']
                     self.session.xenapi.VDI.set_sm_config(self_vdi_ref, sm_config)
            else:
                if has_snapshots == True:
                    for snapshot_uuid in self.snaps.keys():
                        util.SMlog("RBDVDI.update start setting snapshots")
                        snapshot_vdi_ref = self.session.xenapi.VDI.get_by_uuid(snapshot_uuid)
                        self.session.xenapi.VDI.set_name_label(snapshot_vdi_ref, self.session.xenapi.VDI.get_name_label(self_vdi_ref))
                        self.session.xenapi.VDI.set_name_description(snapshot_vdi_ref, self.session.xenapi.VDI.get_name_description(self_vdi_ref))
                        util.SMlog("RBDVDI.update finish setting snapshots")
        else:
            self_vdi_sm_config = self.session.xenapi.VDI.get_sm_config(self_vdi_ref)
            if self_vdi_sm_config.has_key("new_uuid"):
                base_vdi_uuid = self_vdi_sm_config["new_uuid"]
                del self_vdi_sm_config['rollback']
                del self_vdi_sm_config['new_uuid']
                self.session.xenapi.VDI.set_sm_config(self_vdi_ref, self_vdi_sm_config)
            else:
                base_vdi_uuid = self_vdi_sm_config["snapshot-of"]
            base_vdi_ref = self.session.xenapi.VDI.get_by_uuid(base_vdi_uuid)
            self.label=self.session.xenapi.VDI.get_name_label(base_vdi_ref)
            self.description=self.session.xenapi.VDI.get_name_description(base_vdi_ref)
            self.snaps = {}
            self.snaps[vdi_uuid]=self.session.xenapi.VDI.get_snapshot_time(self_vdi_ref)
            cephutils.VDI.update(self, sr_uuid, base_vdi_uuid)

    def generate_config(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDVDI.generate_config")
        #if not util.pathexists(self.path):
        #    raise xs_errors.XenError('VDIUnavailable', opterr='Could not find: %s' % self.path)
        dict = {}
        #self.sr.dconf['multipathing'] = self.sr.mpath
        #self.sr.dconf['multipathhandle'] = self.sr.mpathhandle
        dict['device_config'] = self.sr.dconf
        dict['sr_uuid'] = sr_uuid
        dict['vdi_uuid'] = vdi_uuid
        #dict['allocation'] =  self.sr.sm_config['allocation']
        dict['command'] = 'vdi_attach_from_config'
        # Return the 'config' encoded within a normal XMLRPC response so that
        # we can use the regular response/error parsing code.
        config = xmlrpclib.dumps(tuple([dict]), "vdi_attach_from_config")
        return xmlrpclib.dumps((config,), "", True)

    def attach_from_config(self, sr_uuid, vdi_uuid):
        util.SMlog("RBDVDI.attach_from_config")
        self.sr.attach(sr_uuid)
        try:
            vdi_name = "%s%s" % (cephutils.VDI_PREFIX, vdi_uuid)
            dev_name = "%s/%s" % (self.sr.SR_ROOT, vdi_name)
            self.path = self.sr._get_path(vdi_uuid)
            if self.mode == "kernel":
                util.pread2(["rbd", "map", vdi_name, "--pool", self.sr.CEPH_POOL_NAME, "--name", self.sr.CEPH_USER])
            elif self.mode == "fuse":
                pass
            elif self.mode == "nbd":
                cmdout = util.pread2(["rbd-nbd", "--nbds_max", str(cephutils.NBDS_MAX), "map", "%s/%s" % (self.sr.CEPH_POOL_NAME, vdi_name), "--name", self.sr.CEPH_USER]).rstrip('\n')
                util.pread2(["ln", "-s", cmdout, dev_name])
            return VDI.VDI.attach(self, sr_uuid, vdi_uuid)
        except:
            util.logException("RBDVDI.attach_from_config")
            raise xs_errors.XenError('SRUnavailable', \
                        opterr='Unable to attach the heartbeat disk')

if __name__ == '__main__':
    SRCommand.run(RBDSR, DRIVER_INFO)
else:
    SR.registerSR(RBDSR)
