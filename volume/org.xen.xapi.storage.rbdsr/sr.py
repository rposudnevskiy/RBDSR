#!/usr/bin/env python

from __future__ import division

import os.path
import sys

import xapi.storage.api.volume
from xapi.storage import log

import utils
import ceph_utils
import rbd_utils


class Implementation(xapi.storage.api.volume.SR_skeleton):

    def probe(self, dbg, uri):
        log.debug("{}: SR.probe: uri={}".format(dbg, uri))

        uris = []
        srs = []

        ceph_cluster = ceph_utils.connect(dbg, uri)

        ceph_cluster_name = utils.get_cluster_name_by_uri(dbg, uri)
        ceph_pools = ceph_cluster.list_pools()
        log.debug("%s: SR.probe: Available Pools" % dbg)
        log.debug("%s: SR.probe: ---------------" % dbg)

        for ceph_pool in ceph_pools:
            if ceph_pool.startswith(utils.RBDPOOL_PREFIX):
                log.debug("%s: SR.probe: pool: %s" % (dbg, ceph_pool))

                #pool_meta = rbd_utils.retrievePoolMeta(dbg, ceph_cluster, ceph_pool)

                sr = {}
                sr['sr'] = "rbd://%s/%s" % (ceph_cluster_name, utils.get_sr_uuid_by_name(dbg, ceph_pool))
                sr['name'] = 'CEPHBASED SR' #str(pool_meta[rbd_utils.NAME_TAG] if rbd_utils.NAME_TAG in pool_meta else ''),
                sr['description'] = 'CEPHBASED SR' #str(pool_meta[rbd_utils.DESCRIPTION_TAG] if rbd_utils.DESCRIPTION_TAG in pool_meta else '')

                ceph_cluster_stats = ceph_cluster.get_cluster_stats()

                sr['free_space'] = ceph_cluster_stats['kb_avail']*1024
                sr['total_space'] = ceph_cluster_stats['kb']*1024
                sr['datasources'] = []
                sr['clustered'] = False
                sr['health'] = ['Healthy', '']
                srs.append(sr)

                log.debug("%s: SR.probe: sr: %s" % (dbg, sr))
                #uris.append("rbd://%s" % ceph_cluster_name )

        ceph_utils.disconnect(dbg, ceph_cluster)

        return {
            "srs": srs,
            "uris": uris
        }

    def create(self, dbg, sr_uuid, uri, name, description):
        return None

    def attach(self, dbg, uri):
        log.debug("%s: SR.attach: uri: %s" % (dbg, uri))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        sr_uuid=utils.get_sr_uuid_by_uri(dbg,uri)

        log.debug("%s: SR.attach: sr_uuid: %s" % (dbg, sr_uuid))

        if not ceph_cluster.pool_exists(utils.get_pool_name_by_uri(dbg, uri)):
            raise xapi.storage.api.volume.Sr_not_attached(sr_uuid)

        ceph_utils.disconnect(dbg, ceph_cluster)

        return uri

    def detach(self, dbg, uri):
        log.debug("%s: SR.detach: uri: %s" % (dbg, uri))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        sr_uuid=utils.get_sr_uuid_by_uri(dbg,uri)

        log.debug("%s: SR.detach: sr_uuid: %s" % (dbg, sr_uuid))

        if not ceph_cluster.pool_exists(utils.get_pool_name_by_uri(dbg, uri)):
            raise xapi.storage.api.volume.Sr_not_attached(sr_uuid)

        ceph_utils.disconnect(dbg, ceph_cluster)

    def destroy(self, dbg, uri):
        return None

    def stat(self, dbg, uri):
        log.debug("%s: SR.stat: uri: %s" % (dbg, uri))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        ceph_cluster_stats = ceph_cluster.get_cluster_stats()

        pool_meta = rbd_utils.retrievePoolMeta(dbg, ceph_cluster, utils.get_pool_name_by_uri(dbg, uri))

        # Get the sizes
        tsize = ceph_cluster_stats['kb']*1024
        fsize = ceph_cluster_stats['kb_avail']*1024
        log.debug("%s: SR.stat total_space = %Ld free_space = %Ld" % (dbg, tsize, fsize))

        overprovision = 0

        ceph_utils.disconnect(dbg, ceph_cluster)

        return {
            'sr': uri,
            'uuid': utils.get_sr_uuid_by_uri(dbg,uri),
            'name': str(pool_meta[rbd_utils.NAME_TAG] if rbd_utils.NAME_TAG in pool_meta else str('')),
            'description': str(pool_meta[rbd_utils.DESCRIPTION_TAG] if rbd_utils.DESCRIPTION_TAG in pool_meta else str('')),
            'total_space': tsize,
            'free_space': fsize,
            'overprovision': overprovision,
            'datasources': [],
            'clustered': False,
            'health': ['Healthy', '']
        }

    def set_description(self, dbg, sr, new_description):
        log.debug("%s: SR.set_description: SR: %s New_description: %s"
                  % (dbg, sr, new_description))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        pool_meta = {
            rbd_utils.DESCRIPTION_TAG: new_description,
        }

        try:
            rbd_utils.updatePoolMeta(dbg, ceph_cluster, utils.get_pool_name_by_uri(dbg, sr), pool_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(sr)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def set_name(self, dbg, sr, new_name):
        log.debug("%s: SR.set_name: SR: %s New_name: %s"
                  % (dbg, sr, new_name))

        ceph_cluster = ceph_utils.connect(dbg, sr)

        pool_meta = {
            rbd_utils.NAME_TAG: new_name,
        }

        try:
            rbd_utils.updatePoolMeta(dbg, ceph_cluster, utils.get_pool_name_by_uri(dbg, sr), pool_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(sr)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def ls(self, dbg, uri):
        log.debug("%s: SR.ls: uri: %s" % (dbg, uri))
        results = []
        key = ''

        ceph_cluster = ceph_utils.connect(dbg, uri)

        try:
            rbds = rbd_utils.list(dbg, ceph_cluster, utils.get_pool_name_by_uri(dbg,uri))
            for rbd in rbds:
                if rbd.startswith(utils.VDI_PREFIX):
                    image_name = "%s/%s" % (utils.get_pool_name_by_uri(dbg, uri), rbd)
                    key = image_name
                    log.debug("%s: SR.ls: SR: %s image_name: %s" % (dbg, uri, key))

                    image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
                    #log.debug("%s: SR.ls: SR: %s image: %s Metadata: %s" % (dbg, uri, rbd, image_meta))

                    results.append({
                        'uuid': image_meta[rbd_utils.UUID_TAG],
                        'key': image_meta[rbd_utils.KEY_TAG],
                        'name': image_meta[rbd_utils.NAME_TAG],
                        'description': image_meta[rbd_utils.DESCRIPTION_TAG],
                        'read_write': image_meta[rbd_utils.READ_WRITE_TAG],
                        'virtual_size': image_meta[rbd_utils.VIRTUAL_SIZE_TAG],
                        'physical_utilisation': image_meta[rbd_utils.PHYSICAL_UTILISATION_TAG],
                        'uri': image_meta[rbd_utils.URI_TAG],
                        'keys': image_meta[rbd_utils.CUSTOM_KEYS_TAG],
                        'sharable': image_meta[rbd_utils.SHARABLE_TAG]
                    })
                #log.debug("%s: SR.ls: Result: %s" % (dbg, results))
            return results
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.volume.SR_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == 'SR.probe':
        cmd.probe()
    elif base == 'SR.attach':
        cmd.attach()
    elif base == 'SR.create':
        cmd.create()
    elif base == 'SR.destroy':
        cmd.destroy()
    elif base == 'SR.detach':
        cmd.detach()
    elif base == 'SR.ls':
        cmd.ls()
    elif base == 'SR.stat':
        cmd.stat()
    elif base == 'SR.set_name':
        cmd.set_name()
    elif base == 'SR.set_description':
        cmd.set_description()
    else:
        raise xapi.storage.api.volume.Unimplemented(base)

