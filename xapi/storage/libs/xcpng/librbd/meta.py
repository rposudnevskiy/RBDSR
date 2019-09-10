#!/usr/bin/env python

from struct import pack, unpack
from xapi.storage import log
from xapi.storage.libs.xcpng.meta import MetaDBOperations as _MetaDBOperations_
from xapi.storage.libs.xcpng.utils import get_sr_name_by_uri, get_cluster_name_by_uri
from xapi.storage.libs.xcpng.librbd.rbd_utils import ceph_cluster, rbd_create, rbd_write, rbd_read, rbd_remove, \
                                                     rbd_lock, rbd_unlock

CEPH_CLUSTER_TAG = 'cluster'


class MetaDBOperations(_MetaDBOperations_):

    def __init__(self):
        self.lh = None

    def create(self, dbg, uri, db, size=8388608):
        log.debug("%s: xcpng.librbd.meta.MetaDBOpeations.create: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            rbd_create(dbg, cluster, get_sr_name_by_uri(dbg, uri), '__meta__', size)  # default size = 8388608 = 8Mb
            rbd_create(dbg, cluster, get_sr_name_by_uri(dbg, uri), '__lock__', 0)
            length = len(db)
            rbd_write(dbg, cluster, get_sr_name_by_uri(dbg, uri), '__meta__', pack("!I%ss" % length, length, db), 0, length+4)
        except Exception as e:
            log.error("%s: xcpng.librbd.meta.MetaDBOpeations.create: Failed to create MetaDB: uri: %s"
                      % (dbg, uri))
            raise Exception(e)
        finally:
            cluster.shutdown()

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.librbd.meta.MetaDBOpeations.destroy: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            rbd_remove(dbg, cluster, get_sr_name_by_uri(dbg, uri), '__meta__')
            rbd_remove(dbg, cluster, get_sr_name_by_uri(dbg, uri), '__lock__')

        except Exception as e:
            log.error("%s: xcpng.librbd.meta.MetaDBOpeations.destroy: Failed to destroy MetaDB: uri: %s"
                      % (dbg, uri))
            raise Exception(e)
        finally:
            cluster.shutdown()

    def load(self, dbg, uri):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.load: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            length = unpack('!I', rbd_read(dbg, cluster, get_sr_name_by_uri(dbg, uri), '__meta__', 0, 4))[0]
            data = unpack('!%ss' % length, rbd_read(dbg, cluster, get_sr_name_by_uri(dbg, uri), '__meta__', 4, length))[0]
            return data
        except Exception as e:
            log.error("%s: xcpng.librbd.meta.MetaDBOpeations.load: Failed to load MetaDB: uri: %s"
                      % (dbg, uri))
            raise Exception(e)
        finally:
            cluster.shutdown()

    def dump(self, dbg, uri, json):
        log.debug("%s: xcpng.libsbd.meta.MetaDBOpeations.dump: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            length = len(json)
            cluster.connect()
            rbd_write(dbg,
                      cluster,
                      get_sr_name_by_uri(dbg, uri),
                      '__meta__',
                      pack("!I%ss" % length, length, json),
                      0,
                      length + 4)
        except Exception as e:
            log.error("%s: xcpng.librbd.meta.MetaDBOpeations.dump: Failed to dump MetaDB: uri: %s"
                      % (dbg, uri))
            raise Exception(e)
        finally:
            cluster.shutdown()
