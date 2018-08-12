#!/usr/bin/env python

from xapi.storage.libs.librbd import utils, ceph_utils, rbd_utils

from xapi.storage import log
from xapi.storage.api.v4.volume import Volume_does_not_exist

# define tags for metadata
UUID_TAG = 'uuid'
SR_UUID_TAG = 'sr_uuid'
TYPE_TAG = 'vdi_type'
KEY_TAG = 'key'
NAME_TAG = 'name'
DESCRIPTION_TAG = 'description'
CONFIGURATION_TAG = 'configuration'
READ_WRITE_TAG = 'read_write'
VIRTUAL_SIZE_TAG = 'virtual_size'
PHYSICAL_UTILISATION_TAG = 'physical_utilisation'
URI_TAG = 'uri'
CUSTOM_KEYS_TAG = 'keys'
SHARABLE_TAG = 'sharable'
NON_PERSISTENT_TAG = 'nonpersistent'
QEMU_PID_TAG = 'qemu_pid'
QEMU_QMP_SOCK_TAG = 'qemu_qmp_sock'
QEMU_NBD_SOCK_TAG = 'qemu_nbd_sock'
QEMU_QMP_LOG_TAG = 'qemu_qmp_log'
ACTIVE_ON_TAG = 'active_on'
SNAPSHOT_OF_TAG = 'snapshot_of'
IMAGE_FORMAT_TAG = 'image-format'
CEPH_CLUSTER_TAG = 'cluster'

# define tag types
TAG_TYPES = {
    UUID_TAG: str,
    SR_UUID_TAG: str,
    TYPE_TAG: str,
    KEY_TAG: str,
    NAME_TAG: str,
    DESCRIPTION_TAG: str,
    CONFIGURATION_TAG: eval, # dict
    READ_WRITE_TAG: eval, # boolean
    VIRTUAL_SIZE_TAG: int,
    PHYSICAL_UTILISATION_TAG: int,
    URI_TAG: eval, # string list
    CUSTOM_KEYS_TAG: eval, # dict
    SHARABLE_TAG: eval, # boolean
    NON_PERSISTENT_TAG: eval,
    QEMU_PID_TAG: int,
    QEMU_QMP_SOCK_TAG: str,
    QEMU_NBD_SOCK_TAG: str,
    QEMU_QMP_LOG_TAG: str,
    ACTIVE_ON_TAG: str,
    SNAPSHOT_OF_TAG: str,
    IMAGE_FORMAT_TAG: str,
    CEPH_CLUSTER_TAG: str
}

class MetadataHandler(object):

    @staticmethod
    def _load(dbg, uri, use_image_prefix=True):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @staticmethod
    def _update(dbg, uri, image_meta, use_image_prefix=True):
        raise NotImplementedError('Override in MetadataHandler specifc class')

    @classmethod
    def load(cls, dbg, uri, use_image_prefix=True):
        log.debug("%s: meta.MetadataHandler.load: uri: %s "
                  % (dbg, uri))

        return cls._load(dbg, uri, use_image_prefix)

    @classmethod
    def update(cls, dbg, uri, image_meta, use_image_prefix=True):
        log.debug("%s: meta.MetadataHandler.update: uri: %s "
                  % (dbg, uri))

        cls._update(dbg, uri, image_meta, use_image_prefix)

class RBDMetadataHandler(MetadataHandler):

    @staticmethod
    def _load(dbg, uri, use_image_prefix=True):
        log.debug("%s: meta.RBDMetadataHandler._load: uri: %s"
                  % (dbg, uri))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        if use_image_prefix:
            image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                        utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                        utils.get_vdi_uuid_by_uri(dbg, uri))
        else:
            image_name = "%s%s/%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                      utils.get_vdi_uuid_by_uri(dbg, uri))

        image_meta = {}

        try:
            image_meta_list = rbd_utils.retrieveImageMetadata(dbg, ceph_cluster, image_name)

            for tag, value in image_meta_list:
                image_meta[tag] = TAG_TYPES[tag](value)

            log.debug("%s: meta.RBDMetadataHandler._load: Image: %s Metadata: %s "
                      % (dbg, image_name, image_meta))
        except Exception:
            raise Volume_does_not_exist(uri)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        return image_meta

    @staticmethod
    def _update(dbg, uri, image_meta, use_image_prefix=True):
        log.debug("%s: meta.RBDMetadataHandler._update_meta: uri: %s image_meta: %s"
                  % (dbg, uri, image_meta))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        if use_image_prefix:
            image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                        utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                        utils.get_vdi_uuid_by_uri(dbg, uri))
        else:
            image_name = "%s%s/%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                      utils.get_vdi_uuid_by_uri(dbg, uri))

        try:
            rbd_utils.updateImageMetadata(dbg, ceph_cluster, image_name, image_meta)
        except Exception:
            raise Volume_does_not_exist(uri)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)