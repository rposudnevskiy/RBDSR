#!/usr/bin/env python

import uuid

from xapi.storage.libs.librbd import utils, ceph_utils, rbd_utils, meta
import copy

from xapi.storage import log

import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_does_not_exist, Activated_on_another_host
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import Volume_does_not_exist, Activated_on_another_host

from xapi.storage.libs.util import get_current_host_uuid, call

# TODO: We should import correct datapath depend on image uri
from xapi.storage.libs.librbd.datapath import QdiskDatapath as Datapath


class Volume(object):

    @classmethod
    def _create(cls, dbg, sr, name, description, size, sharable, image_meta):
        # Override in Volume specifc class
        return image_meta

    @classmethod
    def create(cls, dbg, sr, name, description, size, sharable):
        log.debug("%s: librbd.Volume.create: SR: %s Name: %s Description: %s Size: %s, Sharable: %s"
                  % (dbg, sr, name, description, size, sharable))

        vdi_uuid = str(uuid.uuid4())
        vdi_uri = "%s/%s" % (sr, vdi_uuid)
        vsize = size
        psize = 0
        # size_MB = size / 1024 / 1024
        if description == '':
            description = ' '

        image_meta = {
            meta.KEY_TAG: vdi_uuid,
            meta.UUID_TAG: vdi_uuid,
            meta.NAME_TAG: name,
            meta.DESCRIPTION_TAG: description,
            meta.READ_WRITE_TAG: True,
            meta.VIRTUAL_SIZE_TAG: vsize,
            meta.PHYSICAL_UTILISATION_TAG: psize,
            meta.URI_TAG: [vdi_uri],
            meta.SHARABLE_TAG: sharable, # False,
            meta.CUSTOM_KEYS_TAG: {}
        }

        return cls._create(dbg, sr, name, description, size, sharable, image_meta)

    @classmethod
    def _stat(cls, dbg, sr, key, image_meta):
        # Override in Volume specifc class
        return image_meta

    @classmethod
    def stat(cls, dbg, sr, key):
        log.debug("%s: librbd.Volume.stat: SR: %s Key: %s"
                  % (dbg, sr, key))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        uri = "%s/%s" % (sr, key)
        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                    utils.get_sr_uuid_by_uri(dbg, sr),
                                    utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                    key)

        try:
            image_meta = meta.RBDMetadataHandler.load(dbg, uri)
            image_meta[meta.PHYSICAL_UTILISATION_TAG] = rbd_utils.getPhysicalUtilisation(dbg,
                                                                                         ceph_cluster,
                                                                                         image_name)
            #meta.RBDMetadataHandler.update(dbg, uri, image_meta)
            log.debug("%s: librbd.Volume.stat: SR: %s Key: %s Metadata: %s"
                      % (dbg, sr, key, image_meta))
            return cls._stat(dbg, sr, key, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    @classmethod
    def _resize(cls, dbg, sr, key, new_size, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def resize(cls, dbg, sr, key, new_size):
        log.debug("%s: librbd.Volume.resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        image_meta = {
            'virtual_size': new_size,
        }

        uri = "%s/%s" % (sr, key)

        try:
            cls._resize(dbg, sr, key, new_size, image_meta)
            meta.RBDMetadataHandler.update(dbg, uri, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _unset(cls, dbg, sr, key, k, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def unset(cls, dbg, sr, key, k):
        log.debug("%s: librbd.Volume.unset: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sr, key, k))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = meta.RBDMetadataHandler.load(dbg, uri)
            image_meta['keys'].pop(k, None)
            meta.RBDMetadataHandler.update(dbg, uri, image_meta)
            cls._unset(dbg, sr, key, k, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _set(cls, dbg, sr, key, k, v, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def set(cls, dbg, sr, key, k, v):
        log.debug("%s: librbd.Volume.set: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sr, key, k, v))

        uri = "%s/%s" % (sr, key)

        try:
            image_meta = meta.RBDMetadataHandler.load(dbg, uri)
            image_meta['keys'][k] = v
            meta.RBDMetadataHandler.update(dbg, uri, image_meta)
            cls._set(dbg, sr, key, k, v, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _set_description(cls, dbg, sr, key, new_description, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def set_description(cls, dbg, sr, key, new_description):
        log.debug("%s: librbd.Volume.set_description: SR: %s Key: %s New_description: %s"
                  % (dbg, sr, key, new_description))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'description': new_description,
        }

        try:
            meta.RBDMetadataHandler.update(dbg, uri, image_meta)
            cls._set_description(dbg, sr, key, new_description, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _set_name(cls, dbg, sr, key, new_name, image_meta):
        # Override in Volume specifc class
        pass

    @classmethod
    def set_name(cls, dbg, sr, key, new_name):
        log.debug("%s: librbd.Volume.set_name: SR: %s Key: %s New_name: %s"
                  % (dbg, sr, key, new_name))

        uri = "%s/%s" % (sr, key)

        image_meta = {
            'name': new_name,
        }

        try:
            meta.RBDMetadataHandler.update(dbg, uri, image_meta)
            cls._set_name(dbg, sr, key, new_name, image_meta)
        except Exception:
            raise Volume_does_not_exist(key)

    @classmethod
    def _destroy(cls, dbg, sr, key):
        # Override in Volume specifc class
        pass

    @classmethod
    def destroy(cls, dbg, sr, key):
        log.debug("%s: Volume.destroy: SR: %s Key: %s"
                  % (dbg, sr, key))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        uri = "%s/%s" % (sr, key)
        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                    utils.get_sr_uuid_by_uri(dbg, sr),
                                    utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                    key)

        try:
            rbd_utils.remove(dbg, ceph_cluster, image_name)
            cls._destroy(dbg, sr, key)
        except Exception:
           raise Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    @classmethod
    def _clone(cls, dbg, sr, key, mode, base_meta):
        raise NotImplementedError('Override in Volume specifc class')

    @classmethod
    def clone(cls, dbg, sr, key, mode):
        log.debug("%s: librbd.Volume.clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        orig_uri = "%s/%s" % (sr, key)

        try:
            orig_meta = meta.RBDMetadataHandler.load(dbg, orig_uri)
        except Exception:
            raise Volume_does_not_exist(key)

        if meta.SNAPSHOT_OF_TAG in orig_meta:
            base_uri = "%s/%s" % (sr, orig_meta[meta.SNAPSHOT_OF_TAG])
            try:
                base_meta = meta.RBDMetadataHandler.load(dbg, base_uri)
            except Exception:
                raise Volume_does_not_exist(key)
        else:
            base_meta = copy.deepcopy(orig_meta)

        if meta.ACTIVE_ON_TAG in base_meta:
            current_host = get_current_host_uuid()
            if base_meta[meta.ACTIVE_ON_TAG] != current_host:
                log.debug("%s: librbd.Volume.clone: SR: %s Key: %s Can not snapshot on %s as VDI already active on %s"
                          % (dbg, sr, base_meta[meta.UUID_TAG],
                             current_host, base_meta[meta.ACTIVE_ON_TAG]))
                raise Activated_on_another_host(base_meta[meta.ACTIVE_ON_TAG])

        return cls._clone(dbg, sr, key, mode, base_meta)


class RAWVolume(Volume):

    @classmethod
    def _clone(cls, dbg, sr, key, mode, base_meta):
        log.debug("%s: librbd.RAWVolume.clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        clone_uuid = str(uuid.uuid4())
        clone_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                    utils.get_sr_uuid_by_uri(dbg, sr),
                                    utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, sr)],
                                    clone_uuid)

        try:
            if base_meta[meta.KEY_TAG] == key:
                base_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                           utils.get_sr_uuid_by_uri(dbg, sr),
                                           utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, sr)],
                                           key)

                new_base_uuid = str(uuid.uuid4())
                new_base_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                               utils.get_sr_uuid_by_uri(dbg, sr),
                                               utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, sr)],
                                               new_base_uuid)

                if meta.ACTIVE_ON_TAG in base_meta:
                    Datapath.suspend(dbg,base_meta[meta.URI_TAG][0], 0)

                rbd_utils.rename(dbg, ceph_cluster, base_name, new_base_name)
                rbd_utils.snapshot(dbg, ceph_cluster, new_base_name, 'base')
                rbd_utils.clone(dbg, ceph_cluster, new_base_name, 'base', base_name)
                rbd_utils.clone(dbg, ceph_cluster, new_base_name, 'base', clone_name)

                if meta.ACTIVE_ON_TAG in base_meta:
                    Datapath.resume(dbg,base_meta[meta.URI_TAG][0], 0)

                new_base_meta = copy.deepcopy(base_meta)
                new_base_meta[meta.NAME_TAG] = "(base) %s" % new_base_meta[meta.NAME_TAG]
                new_base_meta[meta.KEY_TAG] = new_base_uuid
                new_base_meta[meta.UUID_TAG] = new_base_uuid
                new_base_meta[meta.URI_TAG] = ["%s/%s" % (sr, new_base_uuid)]
                new_base_meta[meta.READ_WRITE_TAG] = False

                if meta.ACTIVE_ON_TAG in new_base_meta:
                    new_base_meta[meta.ACTIVE_ON_TAG] = None
                    new_base_meta[meta.QEMU_PID_TAG] = None
                    new_base_meta[meta.QEMU_NBD_SOCK_TAG] = None
                    new_base_meta[meta.QEMU_QMP_SOCK_TAG] = None
                    new_base_meta[meta.QEMU_QMP_LOG_TAG] = None

                meta.RBDMetadataHandler.update(dbg, new_base_meta[meta.URI_TAG][0], new_base_meta)
                meta.RBDMetadataHandler.update(dbg, base_meta[meta.URI_TAG][0], base_meta)

            else:
                base_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                           utils.get_sr_uuid_by_uri(dbg, sr),
                                           utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, sr)],
                                           base_meta[meta.UUID_TAG])
                rbd_utils.clone(dbg, ceph_cluster, base_name, 'base', clone_name)

            clone_meta = copy.deepcopy(base_meta)
            clone_meta[meta.KEY_TAG] = clone_uuid
            clone_meta[meta.UUID_TAG] = clone_uuid
            clone_meta[meta.URI_TAG] = ["%s/%s" % (sr, clone_uuid)]

            if meta.ACTIVE_ON_TAG in clone_meta:
                clone_meta[meta.ACTIVE_ON_TAG] = None
                clone_meta[meta.QEMU_PID_TAG] = None
                clone_meta[meta.QEMU_NBD_SOCK_TAG] = None
                clone_meta[meta.QEMU_QMP_SOCK_TAG] = None
                clone_meta[meta.QEMU_QMP_LOG_TAG] = None

            if mode is 'snapshot':
                clone_meta[meta.READ_WRITE_TAG] = False
                clone_meta[meta.SNAPSHOT_OF_TAG] = new_base_meta[meta.UUID_TAG]
            elif mode is 'clone':
                clone_meta[meta.READ_WRITE_TAG] = True

            meta.RBDMetadataHandler.update(dbg, clone_meta[meta.URI_TAG][0], clone_meta)
            return clone_meta
        except Exception:
            raise Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    @classmethod
    def _create(cls, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: librbd.RAWVolume.create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        image_meta[meta.TYPE_TAG] = utils.get_vdi_type_by_uri(dbg, image_meta[meta.URI_TAG][0])

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                    utils.get_sr_uuid_by_uri(dbg, sr),
                                    utils.VDI_PREFIXES[image_meta[meta.TYPE_TAG]],
                                    image_meta[meta.UUID_TAG])

        try:
            rbd_utils.create(dbg, ceph_cluster, image_name, image_meta[meta.VIRTUAL_SIZE_TAG])
            meta.RBDMetadataHandler.update(dbg, image_meta[meta.URI_TAG][0], image_meta)
        except Exception:
            try:
                rbd_utils.remove(dbg, ceph_cluster, image_name)
            except Exception:
                pass
            finally:
                raise Volume_does_not_exist(image_meta[meta.UUID_TAG])
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        return image_meta

    @classmethod
    def _resize(cls, dbg, sr, key, new_size, image_meta):
        log.debug("%s: librbd.RAWVolume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        uri = "%s/%s" % (sr, key)
        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                    utils.get_sr_uuid_by_uri(dbg, sr),
                                    utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                    key)

        try:
            rbd_utils.resize(dbg, ceph_cluster, image_name, new_size)
        except Exception:
            raise Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)


class QCOW2Volume(Volume):

    @classmethod
    def _clone(cls, dbg, sr, key, mode, base_meta):
        log.debug("%s: librbd.QCOW2Volume.clone: SR: %s Key: %s Mode: %s"
                  % (dbg, sr, key, mode))

        # TODO: Implement overhead calculation for QCOW2 format
        size = utils.validate_and_round_vhd_size(base_meta[meta.VIRTUAL_SIZE_TAG])
        rbd_size = utils.fullSizeVHD(size)

        ceph_cluster = ceph_utils.connect(dbg, sr)

        clone_uuid = str(uuid.uuid4())
        clone_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                    utils.get_sr_uuid_by_uri(dbg, sr),
                                    utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, sr)],
                                    clone_uuid)

        try:
            if base_meta[meta.KEY_TAG] == key:
                base_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                           utils.get_sr_uuid_by_uri(dbg, sr),
                                           utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, sr)],
                                           key)

                new_base_uuid = str(uuid.uuid4())
                new_base_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                               utils.get_sr_uuid_by_uri(dbg, sr),
                                               utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, sr)],
                                               new_base_uuid)

                rbd_utils.rename(dbg, ceph_cluster, base_name, new_base_name)
                rbd_utils.create(dbg, ceph_cluster, base_name, rbd_size)
                rbd_utils.create(dbg, ceph_cluster, clone_name, rbd_size)

                base_nbd_device = call(dbg, ["/usr/bin/rbd",
                                             "nbd",
                                             "map",
                                             base_name]).rstrip('\n')

                clone_nbd_device = call(dbg, ["/usr/bin/rbd",
                                              "nbd",
                                              "map",
                                              clone_name]).rstrip('\n')

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "create",
                           "-f", base_meta[meta.TYPE_TAG],
                           "-b", "rbd:%s" % new_base_name,
                           base_nbd_device])

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "create",
                           "-f", base_meta[meta.TYPE_TAG],
                           "-b", "rbd:%s" % new_base_name,
                           clone_nbd_device])

                call(dbg, ["/usr/bin/rbd",
                           "nbd",
                           "unmap",
                           clone_nbd_device])

                call(dbg, ["/usr/bin/rbd",
                           "nbd",
                           "unmap",
                           base_nbd_device])

                new_base_meta = copy.deepcopy(base_meta)
                new_base_meta[meta.NAME_TAG] = "(base) %s" % new_base_meta[meta.NAME_TAG]
                new_base_meta[meta.KEY_TAG] = new_base_uuid
                new_base_meta[meta.UUID_TAG] = new_base_uuid
                new_base_meta[meta.URI_TAG] = ["%s/%s" % (sr, new_base_uuid)]
                new_base_meta[meta.READ_WRITE_TAG] = False

                if meta.ACTIVE_ON_TAG in new_base_meta:
                    Datapath.snapshot(dbg,new_base_meta[meta.URI_TAG][0], base_meta[meta.URI_TAG][0], 0)

                if meta.ACTIVE_ON_TAG in new_base_meta:
                    new_base_meta[meta.ACTIVE_ON_TAG] = None
                    new_base_meta[meta.QEMU_PID_TAG] = None
                    new_base_meta[meta.QEMU_NBD_SOCK_TAG] = None
                    new_base_meta[meta.QEMU_QMP_SOCK_TAG] = None
                    new_base_meta[meta.QEMU_QMP_LOG_TAG] = None

                meta.RBDMetadataHandler.update(dbg, new_base_meta[meta.URI_TAG][0], new_base_meta)
                meta.RBDMetadataHandler.update(dbg, base_meta[meta.URI_TAG][0], base_meta)

            else:
                base_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                           utils.get_sr_uuid_by_uri(dbg, sr),
                                           utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, sr)],
                                           base_meta[meta.UUID_TAG])

                rbd_utils.create(dbg, ceph_cluster, clone_name, rbd_size)

                clone_nbd_device = call(dbg, ["/usr/bin/rbd",
                                              "nbd",
                                              "map",
                                              clone_name]).rstrip('\n')

                call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                           "create",
                           "-f", base_meta[meta.TYPE_TAG],
                           "-b", "rbd:%s" % base_name,
                           clone_nbd_device])

                call(dbg, ["/usr/bin/rbd",
                           "nbd",
                           "unmap",
                           clone_nbd_device])

            clone_meta = copy.deepcopy(base_meta)
            clone_meta[meta.KEY_TAG] = clone_uuid
            clone_meta[meta.UUID_TAG] = clone_uuid
            clone_meta[meta.URI_TAG] = ["%s/%s" % (sr, clone_uuid)]

            if meta.ACTIVE_ON_TAG in clone_meta:
                clone_meta.pop(meta.ACTIVE_ON_TAG, None)
                clone_meta.pop(meta.QEMU_PID_TAG, None)
                clone_meta.pop(meta.QEMU_NBD_SOCK_TAG, None)
                clone_meta.pop(meta.QEMU_QMP_SOCK_TAG, None)
                clone_meta.pop(meta.QEMU_QMP_LOG_TAG, None)

            if mode is 'snapshot':
                clone_meta[meta.READ_WRITE_TAG] = False
                clone_meta[meta.SNAPSHOT_OF_TAG] = new_base_meta[meta.UUID_TAG]
            elif mode is 'clone':
                clone_meta[meta.READ_WRITE_TAG] = True

            meta.RBDMetadataHandler.update(dbg, clone_meta[meta.URI_TAG][0], clone_meta)

            return clone_meta
        except Exception:
            raise Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    @classmethod
    def _resize(cls, dbg, sr, key, new_size, image_meta):
        log.debug("%s: librbd.QCOW2Volume._resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        # TODO: Implement overhead calculation for QCOW2 format
        new_size = utils.validate_and_round_vhd_size(new_size)
        new_rbd_size = utils.fullSizeVHD(new_size)

        ceph_cluster = ceph_utils.connect(dbg, sr)

        uri = "%s/%s" % (sr, key)
        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                    utils.get_sr_uuid_by_uri(dbg, sr),
                                    utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)],
                                    key)

        try:
            rbd_utils.resize(dbg, ceph_cluster, image_name, new_rbd_size)
        except Exception:
            raise Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        #nbd_device = call(dbg, ["/usr/bin/rbd",
        #                        "nbd",
        #                        "map",
        #                        image_name]).rstrip('\n')

        call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                   "resize",
                   "rbd:%s" % image_name,
                   str(new_size)])

        #call(dbg, ["/usr/bin/rbd",
        #           "nbd",
        #           "unmap",
        #           nbd_device])

    @classmethod
    def _create(cls, dbg, sr, name, description, size, sharable, image_meta):
        log.debug("%s: librbd.QCOW2Volume._create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))

        image_meta[meta.TYPE_TAG] = utils.get_vdi_type_by_uri(dbg, image_meta[meta.URI_TAG][0])

        ceph_cluster = ceph_utils.connect(dbg, sr)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX,
                                    utils.get_sr_uuid_by_uri(dbg, sr),
                                    utils.VDI_PREFIXES[image_meta[meta.TYPE_TAG]],
                                    image_meta[meta.UUID_TAG])

        # TODO: Implement overhead calculation for QCOW2 format
        size = utils.validate_and_round_vhd_size(size)
        rbd_size = utils.fullSizeVHD(size)

        try:
            rbd_utils.create(dbg, ceph_cluster, image_name, rbd_size)
            meta.RBDMetadataHandler.update(dbg, image_meta[meta.URI_TAG][0], image_meta)
        except Exception:
            try:
                rbd_utils.remove(dbg, ceph_cluster, image_name)
            except Exception:
                pass
            finally:
                raise Volume_does_not_exist(image_meta[meta.UUID_TAG])
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        #Datapath.attach(dbg, image_meta[meta.URI_TAG][0], 0)
        #Datapath.activate(dbg, image_meta[meta.URI_TAG][0], 0, 'raw')

        #nbd_device=call(dbg, ["/opt/xensource/libexec/nbd_client_manager.py",
        #                      "connect",
        #                      "--path",
        #                      utils.VAR_RUN_PREFIX + "/qemu-nbd.{}".format(image_meta[meta.UUID_TAG]),
        #                      "--exportname",
        #                      "qemu_node"])

        nbd_device = call(dbg, ["/usr/bin/rbd",
                                "nbd",
                                "map",
                                image_name]).rstrip('\n')

        call(dbg, ["/usr/lib64/qemu-dp/bin/qemu-img",
                   "create",
                   "-f", image_meta[meta.TYPE_TAG],
                   nbd_device,
                   str(size)])

        call(dbg, ["/usr/bin/rbd",
                   "nbd",
                   "unmap",
                   nbd_device])

        #call(dbg, ["/opt/xensource/libexec/nbd_client_manager.py",
        #           "disconnect",
        #               "--device",
        #           nbd_device])

        #Datapath.deactivate(dbg, image_meta[meta.URI_TAG][0], 0)
        #Datapath.detach(dbg, image_meta[meta.URI_TAG][0], 0)

        return image_meta