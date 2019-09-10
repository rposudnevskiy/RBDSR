#!/usr/bin/env python

from xapi.storage import log
from xapi.storage.libs.xcpng.sr import SROperations as _SROperations_
from xapi.storage.libs.xcpng.utils import POOL_PREFIX, SR_PATH_PREFIX, VDI_PREFIXES, \
                                          get_sr_type_by_uri, get_sr_uuid_by_uri, mkdir_p, get_vdi_type_by_uri, \
                                          get_sr_name_by_uri, get_cluster_name_by_uri, get_sr_uuid_by_name
from xapi.storage.libs.xcpng.librbd.meta import CEPH_CLUSTER_TAG
from xapi.storage.libs.xcpng.librbd.rbd_utils import get_config_files_list, pool_list, rbd_list, ceph_cluster


class SROperations(_SROperations_):

    def __init__(self):
        self.DEFAULT_SR_NAME = '<Ceph RBD SR>'
        self.DEFAULT_SR_DESCRIPTION = '<Ceph RBD SR>'
        super(SROperations, self).__init__()

    def extend_uri(self, dbg, uri, configuration):
        log.debug("%s: xcpng.librbd.sr.SROperations.extend_uri: uri: %s configuration %s" % (dbg, uri, configuration))

        if CEPH_CLUSTER_TAG in configuration:
            return "%s%s" % (uri, configuration[CEPH_CLUSTER_TAG])
        else:
            return uri

    def create(self, dbg, uri, configuration):
        log.debug("%s: xcpng.librbd.sr.SROperations.create: uri: %s configuration %s" % (dbg, uri, configuration))

        if CEPH_CLUSTER_TAG not in configuration:
            raise Exception('Failed to connect to CEPH cluster. Parameter \'cluster\' is not specified')

        cluster = ceph_cluster(dbg, configuration[CEPH_CLUSTER_TAG])

        try:
            cluster.connect()
            cluster.create_pool(get_sr_name_by_uri(dbg, uri))
        except Exception as e:
            log.debug("%s: xcpng.librbd.sr.SROperations.create: uri: Failed to create SR: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def destroy(self, dbg, uri):
        log.debug("%s: xcpng.librbd.sr.SROperations.destroy: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            cluster.delete_pool(get_sr_name_by_uri(dbg, uri))
        except Exception as e:
            log.debug("%s: xcpng.librbd.sr.SROperations.destory: Failed to destroy SR: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def get_sr_list(self, dbg, uri, configuration):
        log.debug("%s: xcpng.librbd.sr.SROperations.get_sr_list: uri: %s configuration %s" % (dbg, uri, configuration))

        srs = []
        uris = []

        cluster_in_uri = get_cluster_name_by_uri(dbg, uri)

        log.debug("%s: xcpng.librbd.sr.SROperations.get_sr_list: uris: %s" % (dbg, uris))

        if cluster_in_uri == '':
            for cluster in get_config_files_list(dbg):
                uris.append("%s/%s" % (uri[:-1], cluster))
        else:
            uris = [uri]

        log.debug("%s: xcpng.librbd.sr.SROperations.get_sr_list: uris: %s" % (dbg, uris))

        for _uri_ in uris:
            cluster_name = get_cluster_name_by_uri(dbg, _uri_)
            cluster = ceph_cluster(dbg, cluster_name)
            try:
                cluster.connect()
                for pool in pool_list(dbg, cluster):
                    if pool.startswith("%s%s" % (get_sr_type_by_uri(dbg, uri), POOL_PREFIX)):
                        srs.append("%s/%s" % (_uri_, get_sr_uuid_by_name(dbg, pool)))
            except Exception as e:
                log.debug("%s: xcpng.librbd.sr.SROperations.get_sr_list: uri: Failed to get SRs list: uri: %s"
                          % dbg, uri)
                raise Exception(e)
            finally:
                cluster.shutdown()
        log.debug("%s: xcpng.librbd.sr.SROperations.get_sr_list: srs: %s" % (dbg, srs))
        return srs

    def get_vdi_list(self, dbg, uri):
        log.debug("%s: xcpng.librbd.sr.SROperations.get_vdi_list: uri: %s" % (dbg, uri))

        rbds = []
        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            for rbd in rbd_list(dbg, cluster, get_sr_name_by_uri(dbg, uri)):
                if rbd.startswith(VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)]):
                    rbds.append(rbd)
            return rbds
        except Exception as e:
            log.debug("%s: xcpng.librbd.sr.SROperations.get_vdi_list: uri: Failed to get VDIs list: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def sr_import(self, dbg, uri, configuration):
        log.debug("%s: xcpng.librbd.sr.SROperations.sr_import: uri: %s configuration %s" % (dbg, uri, configuration))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))
        pool_name = get_sr_name_by_uri(dbg, uri)

        try:
            cluster.connect()
            if not cluster.pool_exists(pool_name):
                raise Exception("CEPH pool %s doesn\'t exist" % pool_name)
        except Exception as e:
            log.debug("%s: xcpng.librbd.sr.SROperations.sr_import: uri: Failed to import SR: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

        mkdir_p("%s/%s" % (SR_PATH_PREFIX, get_sr_uuid_by_uri(dbg, uri)))

    def sr_export(self, dbg, uri):
        # log.debug("%s: xcpng.librbd.sr.SROperations.sr_export: uri: %s" % (dbg, uri))
        pass

    def get_free_space(self, dbg, uri):
        log.debug("%s: xcpng.librbd.sr.SROperations.get_free_space: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            stats = cluster.get_cluster_stats()
            return stats['kb_avail'] * 1024
        except Exception as e:
            log.debug("%s: xcpng.librbd.sr.SROperations.get_free_space: uri: Failed to get free space: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()

    def get_size(self, dbg, uri):
        log.debug("%s: xcpng.librbd.sr.SROperations.sr_size: uri: %s" % (dbg, uri))

        cluster = ceph_cluster(dbg, get_cluster_name_by_uri(dbg, uri))

        try:
            cluster.connect()
            stats = cluster.get_cluster_stats()
            return stats['kb'] * 1024
        except Exception as e:
            log.debug("%s: xcpng.librbd.sr.SROperations.get_size: uri: Failed to get size: uri: %s"
                      % dbg, uri)
            raise Exception(e)
        finally:
            cluster.shutdown()
