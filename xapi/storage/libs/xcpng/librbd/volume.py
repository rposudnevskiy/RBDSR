#!/usr/bin/env python

from xapi.storage import log
from xapi.storage.libs.xcpng.meta import IMAGE_UUID_TAG
from xapi.storage.libs.xcpng.utils import get_cluster_name_by_uri, get_sr_name_by_uri, get_vdi_name_by_uri, \
                                          roundup, VDI_PREFIXES, get_vdi_type_by_uri
from xapi.storage.libs.xcpng.volume import VolumeOperations as _VolumeOperations_
from xapi.storage.libs.xcpng.librbd.rbd_utils import VOLBLOCKSIZE, ceph_cluster, rbd_create, rbd_remove, rbd_resize, \
                                                     rbd_utilization


class VolumeOperations(_VolumeOperations_):

    def create(self, dbg, uri, size):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.create: uri: %s size: %s" % (dbg, uri, size))

        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            rbd_create(dbg,
                       cluster,
                       get_sr_name_by_uri(dbg, uri),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], volume_meta[IMAGE_UUID_TAG]), size)
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.create: Failed to create volume: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.destroy: uri: %s" % (dbg, uri))

        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            rbd_remove(dbg,
                       cluster,
                       get_sr_name_by_uri(dbg, uri),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], volume_meta[IMAGE_UUID_TAG]))
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.create: Failed to create volume: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def resize(self, dbg, uri, new_size):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.resize: uri: %s new_size: %s" % (dbg, uri, new_size))

        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            rbd_resize(dbg,
                       cluster,
                       get_sr_name_by_uri(dbg, uri),
                       "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], volume_meta[IMAGE_UUID_TAG]),
                       new_size)
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.resize: Failed to create volume: uri: %s new_size: %s"
                      % dbg, uri, new_size)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def get_phisical_utilization(self, dbg, uri):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.get_phisical_utilization: uri: %s" % (dbg, uri))

        volume_meta = self.MetadataHandler.get_vdi_meta(dbg, uri)
        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            return rbd_utilization(dbg,
                                   cluster,
                                   get_sr_name_by_uri(dbg, uri),
                                   "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], volume_meta[IMAGE_UUID_TAG]))
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.resize: Failed to create volume: uri: %s new_size: %s"
                      % dbg, uri. new_size)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def roundup_size(self, dbg, size):
        return roundup(VOLBLOCKSIZE, size)
