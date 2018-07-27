#!/usr/bin/env python

import rados
import urlparse

from xapi.storage import log


def connect(dbg, uri):
    cluster_name = urlparse.urlparse(uri).netloc
    log.debug("%s: ceph_utils.connect: Cluster_name: %s" % (dbg, cluster_name))

    conf_file = "/etc/ceph/%s.conf" % cluster_name
    log.debug("%s: ceph_utils.connect: conf_file: %s" % (dbg, conf_file))

    cluster = rados.Rados(conffile=conf_file)
    log.debug("%s: ceph_utils.connect: librados version: %s" % (dbg, str(cluster.version())))
    log.debug(
        "%s: ceph_utils.connect: will attempt to connect to: %s" % (dbg, str(cluster.conf_get('mon initial members'))))

    cluster.connect()
    log.debug("%s: ceph_utils.connect: Cluster ID: %s " % (dbg, cluster.get_fsid()))

    return cluster


def disconnect(dbg, cluster):
    log.debug("%s: ceph_utils.disconnect: Cluster ID: %s " % (dbg, cluster.get_fsid()))

    cluster.shutdown()


def get_pool_stats(dbg, cluster, pool_name):
    ioctx = cluster.open_ioctx(pool_name)
    stats = ioctx.get_stats()
    ioctx.close()
    return stats