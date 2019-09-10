#!/usr/bin/env python

from time import time, sleep
from xapi.storage.libs.xcpng.meta import LocksOpsMgr as _LocksOpsMgr_
from xapi.storage.libs.xcpng.librbd.rbd_utils import ceph_cluster, rbd_lock, rbd_unlock, ImageBusy, ImageExists, \
                                                     is_locked
from xapi.storage.libs.xcpng.utils import get_sr_uuid_by_uri, get_vdi_uuid_by_uri, get_cluster_name_by_uri, \
                                          get_sr_name_by_uri, get_vdi_name_by_uri

class LocksOpsMgr(_LocksOpsMgr_):

    def lock(self, dbg, uri, timeout=10):
        log.debug("%s: xcpng.librbd.meta.MetaDBOpeations.lock: uri: %s timeout: %s" % (dbg, uri, timeout))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)
        pool_name = get_sr_name_by_uri(dbg, uri)

        if vdi_uuid is not None:
            lock_uuid = vdi_uuid
            image_name = get_vdi_name_by_uri(dbg, uri)
        else:
            lock_uuid = sr_uuid
            image_name = '__lock__'

        start_time = time()

        lh = [None, None, None]
        lh[0] = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        if is_locked(dbg, lh[0], pool_name, '__lock__'):
            # SR is locked
            raise ImageBusy

        try:
            while True:
                try:
                    if lock_uuid in self.__lhs:
                        raise ImageExists

                    lh[0].connect()
                    lh[1], lh[2] = rbd_lock(dbg,
                                            lh[0],
                                            pool_name,
                                            image_name)
                    self.__lhs[lock_uuid] = lh
                    break
                except Exception as e:
                    if time() - start_time >= timeout:
                        log.error("%s: xcpng.librbd.meta.MetaDBOpeations.lock: Failed to lock: uri: %s" % (dbg, uri))
                        raise Exception(e)
                    sleep(1)
                    pass
        except Exception as e:
            log.error("%s: xcpng.librbd.meta.MetaDBOpeations.lock: Failed to lock: uri: %s"
                      % (dbg, uri))
            lh[0].shutdown()
            raise Exception(e)

    def unlock(self, dbg, uri):
        log.debug("%s: xcpng.librbd.meta.MetaDBOpeations.unlock: uri: %s" % (dbg, uri))

        sr_uuid = get_sr_uuid_by_uri(dbg, uri)
        vdi_uuid = get_vdi_uuid_by_uri(dbg, uri)

        if vdi_uuid is not None:
            lock_uuid = vdi_uuid
        else:
            lock_uuid = sr_uuid

        if lock_uuid in self.__lhs:
            lh = self.__lhs[lock_uuid]
            rbd_unlock(dbg, lh)
            lh[0].shutdown()
            del self.__lhs[lock_uuid]