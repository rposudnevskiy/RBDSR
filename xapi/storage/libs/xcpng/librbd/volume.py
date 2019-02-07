#!/usr/bin/env python

import re
import uuid
from os import system

from xapi.storage.libs.xcpng.utils import get_cluster_name_by_uri, get_sr_name_by_uri, get_vdi_name_by_uri
from xapi.storage.libs.xcpng.utils import roundup

from xapi.storage.libs.xcpng.volume import VOLUME_TYPES, Implementation
from xapi.storage.libs.xcpng.volume import VolumeOperations as _VolumeOperations_
from xapi.storage.libs.xcpng.volume import RAWVolume as _RAWVolume_
from xapi.storage.libs.xcpng.volume import QCOW2Volume as _QCOW2Volume_

from xapi.storage.libs.xcpng.librbd.rbd_utils import VOLBLOCKSIZE, ceph_cluster, rbd_create, rbd_remove, rbd_resize, \
                                                     rbd_utilization, rbd_rename
from xapi.storage.libs.xcpng.librbd.meta import MetadataHandler
from xapi.storage.libs.xcpng.librbd.datapath import DATAPATHES

from xapi.storage import log


class VolumeOperations(_VolumeOperations_):

    def __init__(self):
        super(VolumeOperations, self).__init__()
        self.MetadataHandler = MetadataHandler()

    def create(self, dbg, uri, size):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.create: uri: %s size: %s" % (dbg, uri, size))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            rbd_create(dbg, cluster, get_sr_name_by_uri(dbg, uri), get_vdi_name_by_uri(dbg, uri), size)
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.create: Failed to create volume: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.destroy: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            rbd_remove(dbg, cluster, get_sr_name_by_uri(dbg, uri), get_vdi_name_by_uri(dbg, uri))
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.create: Failed to create volume: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def resize(self, dbg, uri, new_size):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.resize: uri: %s new_size: %s" % (dbg, uri, new_size))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            rbd_resize(dbg, cluster, get_sr_name_by_uri(dbg, uri), get_vdi_name_by_uri(dbg, uri), new_size)
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.resize: Failed to create volume: uri: %s new_size: %s"
                      % dbg, uri, new_size)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def swap(self, dbg, uri1, uri2):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.swap: uri1: %s uri2: %s" % (dbg, uri1, uri2))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri1))

        try:
            cluster.connect()
            rbd_rename(dbg, cluster, get_sr_name_by_uri(dbg, uri1), get_vdi_name_by_uri(dbg, uri1), 'tmp')
            rbd_rename(dbg, cluster, get_sr_name_by_uri(dbg, uri1), get_vdi_name_by_uri(dbg, uri2), get_vdi_name_by_uri(dbg, uri1))
            rbd_rename(dbg, cluster, get_sr_name_by_uri(dbg, uri1), 'tmp', get_vdi_name_by_uri(dbg, uri2))
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.resize: Failed to swap volumes: uri1: %s uri2: %s"
                      % dbg, uri1, uri2)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def get_phisical_utilization(self, dbg, uri):
        log.debug("%s: xcpng.librbd.volume.VolumeOperations.get_phisical_utilization: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            return rbd_utilization(dbg, cluster, get_sr_name_by_uri(dbg, uri), get_vdi_name_by_uri(dbg, uri))
        except Exception as e:
            log.debug("%s: xcpng.librbd.volume.VolumeOperations.resize: Failed to create volume: uri: %s new_size: %s"
                      % dbg, uri. new_size)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def roundup_size(self, dbg, size):
        return roundup(VOLBLOCKSIZE, size)


class RAWVolume(_RAWVolume_):

    def __init__(self):
        super(RAWVolume, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = VolumeOperations()
        self.Datapathes = {}
        for k, v in DATAPATHES.iteritems():
            self.Datapathes[k] = v()


class QCOW2Volume(_QCOW2Volume_):

    def __init__(self):
        super(QCOW2Volume, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.VolOpsHendler = VolumeOperations()
        self.Datapathes = {}
        for k, v in DATAPATHES.iteritems():
            self.Datapathes[k] = v()


VOLUME_TYPES['raw'] = RAWVolume
VOLUME_TYPES['qcow2'] = QCOW2Volume
