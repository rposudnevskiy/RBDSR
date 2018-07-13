#!/usr/bin/env python

import os
import sys
import uuid
import copy
import xapi.storage.api.volume
from xapi.storage import log

import utils
import ceph_utils
import rbd_utils

class Implementation(xapi.storage.api.volume.Volume_skeleton):

    def create(self, dbg, sr, name, description, size):
        log.debug("%s: Volume.create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        vdi_uuid = str(uuid.uuid4())
        vdi_uri = "%s/%s" % (sr, vdi_uuid)
        vsize = size
        psize = 0
        #size_MB = size / 1024 / 1024
        if description == '':
            description = ' '

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, vdi_uuid)

        image_meta = {
                'key': vdi_uuid,
                'uuid': vdi_uuid,
                'name': name,
                'description': description,
                'read_write': True,
                'virtual_size': vsize,
                'physical_utilisation': psize,
                'uri': [vdi_uri],
                'sharable': False,
                'keys': {}
            }

        try:
            rbd_utils.create(dbg, ceph_cluster, image_name, vsize)
            rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
            return image_meta
        except Exception:
            try:
                rbd_utils.remove(dbg, ceph_cluster, image_name)
            except Exception:
                pass
            finally:
                raise xapi.storage.api.volume.Volume_does_not_exist(vdi_uuid)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def clone(self, dbg, sr, key, mode='clone'):
        log.debug("%s: Volume.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        base_uuid = str(uuid.uuid4())
        clone_uuid = str(uuid.uuid4())

        ceph_cluster = ceph_utils.connect(dbg, sr)

        orig_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, key)
        clone_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, clone_uuid)
        base_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, base_uuid)

        orig_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, orig_name)
        base_meta = copy.deepcopy(orig_meta)
        clone_meta = copy.deepcopy(orig_meta)

        try:
            rbd_utils.rename(dbg, ceph_cluster, orig_name, base_name)
            rbd_utils.snapshot(dbg, ceph_cluster, base_name, 'base')
            rbd_utils.clone(dbg, ceph_cluster, base_name, 'base', orig_name)
            rbd_utils.clone(dbg, ceph_cluster, base_name, 'base', clone_name)

            base_meta[rbd_utils.NAME_TAG] = "(base) %s" % base_meta[rbd_utils.NAME_TAG]
            base_meta[rbd_utils.KEY_TAG] = base_uuid
            base_meta[rbd_utils.UUID_TAG] = base_uuid
            base_meta[rbd_utils.URI_TAG] = ["%s/%s" % (sr, base_uuid)]
            base_meta[rbd_utils.READ_WRITE_TAG] = False

            clone_meta[rbd_utils.KEY_TAG] = clone_uuid
            clone_meta[rbd_utils.UUID_TAG] = clone_uuid
            clone_meta[rbd_utils.URI_TAG] = ["%s/%s" % (sr, clone_uuid)]
            if mode is 'snapshot':
                clone_meta[rbd_utils.READ_WRITE_TAG] = False
            elif mode is 'clone':
                clone_meta[rbd_utils.READ_WRITE_TAG] = True

            rbd_utils.updateMetadata(dbg, ceph_cluster, base_name, base_meta)
            rbd_utils.updateMetadata(dbg, ceph_cluster, clone_name, clone_meta)
            rbd_utils.updateMetadata(dbg, ceph_cluster, orig_name, orig_meta)

            return clone_meta
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def snapshot(self, dbg, sr, key):
        return self.clone(dbg,sr,key, mode='snapshot')

    def destroy(self, dbg, sr, key):
        log.debug("%s: Volume.destroy: SR: %s Key: %s"
                  % (dbg, sr, key))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, key)

        try:
            rbd_utils.remove(dbg, ceph_cluster, image_name)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: Volume.set_name: SR: %s Key: %s New_name: %s"
                  % (dbg, sr, key, new_name))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, key)

        image_meta = {
            'name': new_name,
        }

        try:
            rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: Volume.set_description: SR: %s Key: %s New_description: %s"
                  % (dbg, sr, key, new_description))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, key)

        image_meta = {
            'description': new_description,
        }

        try:
            rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: Volume.set: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sr, key, k, v))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, key)

        try:
            image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
            image_meta['keys'][k]=v
            rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def unset(self, dbg, sr, key, k):
        log.debug("%s: Volume.unset: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sr, key, k))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, key)

        try:
            image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
            image_meta['keys'].pop(k, None)
            rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: Volume.resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        new_vsize = new_size

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, key)

        image_meta = {
                'virtual_size': new_vsize,
            }

        try:
            rbd_utils.resize(dbg, ceph_cluster, image_name, new_vsize)
            rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def stat(self, dbg, sr, key):
        log.debug("%s: Volume.stat: SR: %s Key: %s"
                  % (dbg, sr, key))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, sr), utils.VDI_PREFIX, key)

        try:
            image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
            image_meta[rbd_utils.PHYSICAL_UTILISATION_TAG] = rbd_utils.getPhysicalUtilisation(dbg,
                                                                                              ceph_cluster,
                                                                                              image_name)
            rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
            log.debug("%s: Volume.stat: SR: %s Key: %s Metadata: %s"
                       % (dbg, sr, key, image_meta))
            return image_meta
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

##   def compare(self, dbg, sr, key, key2):

##   def similar_content(self, dbg, sr, key):

##    def enable_cbt(self, dbg, sr, key):

##    def disable_cbt(self, dbg, sr, key):

##    def data_destroy(self, dbg, sr, key):

##   def list_changed_blocks(self, dbg, sr, key, key2, offset, length):

if __name__ == "__main__":
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    cmd = xapi.storage.api.volume.Volume_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Volume.create":
        cmd.create()
    elif base == "Volume.snapshot":
        cmd.snapshot()
    elif base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.set_description":
        cmd.set_description()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.unset":
        cmd.unset()
    elif base == "Volume.resize":
        cmd.resize()
    elif base == "Volume.stat":
        cmd.stat()
##    elif base == "Volume.compare":
##        cmd.compare()
##    elif base == "Volume.similar_content":
##        cmd.similar_content()
##    elif base == "Volume.enable_cbt":
##        cmd.enable_cbt()
##    elif base == "Volume.disable_cbt":
##        cmd.disable_cbt()
##    elif base == "Volume.data_destroy":
##        cmd.data_destroy()
##    elif base == "Volume.list_changed_blocks":
##        cmd.list_changed_blocks()
    else:
        raise xapi.storage.api.volume.Unimplemented(base)