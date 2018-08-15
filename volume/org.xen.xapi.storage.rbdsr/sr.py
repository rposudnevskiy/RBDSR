#!/usr/bin/env python

from __future__ import division

import os.path
import sys
import copy

import xapi.storage.api.v4.volume
from xapi.storage import log

from xapi.storage.libs.librbd import utils, ceph_utils, rbd_utils, meta


class Implementation(xapi.storage.api.v4.volume.SR_skeleton):

    def probe(self, dbg, configuration):
        log.debug("{}: SR.probe: configuration={}".format(dbg, configuration))

        uri="rbd+%s://" % configuration[meta.IMAGE_FORMAT_TAG] if meta.IMAGE_FORMAT_TAG in configuration else 'rbd://'
        uri="%s%s" % (uri, configuration[meta.CEPH_CLUSTER_TAG])
        _uri_ = uri
        uri="%s/%s" % (uri, configuration[meta.SR_UUID_TAG]) if meta.SR_UUID_TAG in configuration else uri

        log.debug("{}: SR.probe: uri to probe: {}".format(dbg, uri))

        result = []

        ceph_cluster = ceph_utils.connect(dbg, uri)

        ceph_cluster_name = utils.get_cluster_name_by_uri(dbg, uri)
        ceph_pools = ceph_cluster.list_pools()
        log.debug("%s: SR.probe: Available Pools" % dbg)
        log.debug("%s: SR.probe: ---------------------------------------------------" % dbg)

        for ceph_pool in ceph_pools:
            log.debug("%s: SR.probe: %s" % (dbg, ceph_pool))

            pool_meta = {}
            sr_uuid = utils.get_sr_uuid_by_name(dbg, ceph_pool)

            if ceph_pool.startswith(utils.RBDPOOL_PREFIX):
                if rbd_utils.if_image_exist(dbg, ceph_cluster,
                                            '%s/%s' % (ceph_pool, utils.SR_METADATA_IMAGE_NAME)):

                    pool_meta = meta.RBDMetadataHandler.load(dbg,
                                                             "%s/%s/%s" % (_uri_,
                                                                           utils.get_sr_uuid_by_name(dbg, ceph_pool),
                                                                           utils.SR_METADATA_IMAGE_NAME),
                                                             False)

                if (meta.IMAGE_FORMAT_TAG in configuration and
                        ((meta.CONFIGURATION_TAG in pool_meta and
                            meta.IMAGE_FORMAT_TAG in pool_meta[meta.CONFIGURATION_TAG] and
                            configuration[meta.IMAGE_FORMAT_TAG] != pool_meta[meta.CONFIGURATION_TAG][meta.IMAGE_FORMAT_TAG]) or
                         (meta.CONFIGURATION_TAG in pool_meta and
                            meta.IMAGE_FORMAT_TAG not in pool_meta[meta.CONFIGURATION_TAG]) or
                         meta.CONFIGURATION_TAG not in pool_meta)):

                    ceph_pool = None

                if (meta.SR_UUID_TAG in configuration and
                        ((meta.CONFIGURATION_TAG in pool_meta and
                          meta.SR_UUID_TAG in pool_meta[meta.CONFIGURATION_TAG] and
                              configuration[meta.SR_UUID_TAG] != pool_meta[meta.CONFIGURATION_TAG][
                              meta.SR_UUID_TAG]) or
                         (meta.CONFIGURATION_TAG in pool_meta and
                              meta.SR_UUID_TAG not in pool_meta[meta.CONFIGURATION_TAG] and
                              configuration[meta.SR_UUID_TAG] != sr_uuid) or
                         (meta.CONFIGURATION_TAG not in pool_meta and
                              configuration[meta.SR_UUID_TAG] != sr_uuid))):

                    ceph_pool = None

                if ceph_pool is not None:

                    _result_ = {}
                    _result_['complete'] = True
                    _result_['configuration'] = {}
                    _result_['configuration'] = copy.deepcopy(configuration)
                    _result_['extra_info'] = {}


                    sr = {}
                    sr['sr'] = "rbd://%s/%s" % (ceph_cluster_name, utils.get_sr_uuid_by_name(dbg, ceph_pool))
                    sr['name'] = pool_meta[meta.NAME_TAG] if meta.NAME_TAG in pool_meta else '<CEPHBASED SR>'
                    sr['description'] = pool_meta[meta.DESCRIPTION_TAG] if meta.DESCRIPTION_TAG in pool_meta else '<CEPHBASED SR>'

                    ceph_cluster_stats = ceph_cluster.get_cluster_stats()

                    sr['free_space'] = ceph_cluster_stats['kb_avail']*1024
                    sr['total_space'] = ceph_cluster_stats['kb']*1024
                    sr['datasources'] = []
                    sr['clustered'] = False
                    sr['health'] = ['Healthy', '']

                    _result_['sr'] = sr
                    _result_['configuration']['sr_uuid'] = utils.get_sr_uuid_by_name(dbg, ceph_pool)

                    result.append(_result_)

        ceph_utils.disconnect(dbg, ceph_cluster)

        return result

    def create(self, dbg, sr_uuid, uri, name, description):
        return None

    def attach(self, dbg, configuration):
        log.debug("%s: SR.attach: configuration: %s" % (dbg, configuration))

        uri = "rbd+%s+%s://%s/%s" % (configuration['image-format'],
                                     configuration['datapath'],
                                     configuration['cluster'],
                                     configuration['sr_uuid'])

        ceph_cluster = ceph_utils.connect(dbg, uri)

        #sr_uuid=utils.get_sr_uuid_by_uri(dbg,uri)

        log.debug("%s: SR.attach: sr_uuid: %s uri: %s" % (dbg, configuration['sr_uuid'], uri))

        if not ceph_cluster.pool_exists(utils.get_pool_name_by_uri(dbg, uri)):
            raise xapi.storage.api.v4.volume.Sr_not_attached(configuration['sr_uuid'])

        # Create pool metadata image if it doesn't exist
        log.debug("%s: SR.attach: name: %s/%s" % (dbg, utils.get_pool_name_by_uri(dbg, uri), utils.SR_METADATA_IMAGE_NAME))
        if not rbd_utils.if_image_exist(dbg, ceph_cluster, '%s/%s' % (utils.get_pool_name_by_uri(dbg, uri), utils.SR_METADATA_IMAGE_NAME)):
            rbd_utils.create(dbg, ceph_cluster, '%s/%s' % (utils.get_pool_name_by_uri(dbg, uri), utils.SR_METADATA_IMAGE_NAME), 0)

        ceph_utils.disconnect(dbg, ceph_cluster)

        return uri

    def detach(self, dbg, uri):
        log.debug("%s: SR.detach: uri: %s" % (dbg, uri))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        sr_uuid=utils.get_sr_uuid_by_uri(dbg,uri)

        log.debug("%s: SR.detach: sr_uuid: %s" % (dbg, sr_uuid))

        if not ceph_cluster.pool_exists(utils.get_pool_name_by_uri(dbg, uri)):
            raise xapi.storage.api.v4.volume.Sr_not_attached(sr_uuid)

        ceph_utils.disconnect(dbg, ceph_cluster)

    def destroy(self, dbg, uri):
        return None

    def stat(self, dbg, uri):
        log.debug("%s: SR.stat: uri: %s" % (dbg, uri))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        ceph_cluster_stats = ceph_cluster.get_cluster_stats()

        pool_meta = meta.RBDMetadataHandler.load(dbg, '%s/%s' % (uri, utils.SR_METADATA_IMAGE_NAME), False)

        log.debug("%s: SR.stat: pool_meta: %s" % (dbg, pool_meta))

        # Get the sizes
        tsize = ceph_cluster_stats['kb']*1024
        fsize = ceph_cluster_stats['kb_avail']*1024
        log.debug("%s: SR.stat total_space = %Ld free_space = %Ld" % (dbg, tsize, fsize))

        overprovision = 0

        ceph_utils.disconnect(dbg, ceph_cluster)

        return {
            'sr': uri,
            'uuid': utils.get_sr_uuid_by_uri(dbg,uri),
            'name': pool_meta[meta.NAME_TAG] if meta.NAME_TAG in pool_meta else '<CEPHBASED SR>',
            'description': pool_meta[meta.DESCRIPTION_TAG] if meta.DESCRIPTION_TAG in pool_meta else '<CEPHBASED SR>',
            'total_space': tsize,
            'free_space': fsize,
            'overprovision': overprovision,
            'datasources': [],
            'clustered': False,
            'health': ['Healthy', '']
        }

    def set_description(self, dbg, uri, new_description):
        log.debug("%s: SR.set_description: SR: %s New_description: %s"
                  % (dbg, uri, new_description))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        pool_meta = {
            meta.DESCRIPTION_TAG: new_description,
        }

        try:
            meta.RBDMetadataHandler.update(dbg, '%s/%s' % (uri, utils.SR_METADATA_IMAGE_NAME),pool_meta, False)
        except Exception:
            raise xapi.storage.api.v4.volume.Volume_does_not_exist(uri)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def set_name(self, dbg, uri, new_name):
        log.debug("%s: SR.set_name: SR: %s New_name: %s"
                  % (dbg, uri, new_name))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        pool_meta = {
            meta.NAME_TAG: new_name,
        }

        try:
            meta.RBDMetadataHandler.update(dbg, '%s/%s' % (uri, utils.SR_METADATA_IMAGE_NAME), pool_meta, False)
        except Exception:
            raise xapi.storage.api.v4.volume.Volume_does_not_exist(uri)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

    def ls(self, dbg, uri):
        log.debug("%s: SR.ls: uri: %s" % (dbg, uri))
        results = []
        key = ''

        ceph_cluster = ceph_utils.connect(dbg, uri)

        try:
            rbds = rbd_utils.list(dbg, ceph_cluster, utils.get_pool_name_by_uri(dbg, uri))
            for rbd in rbds:
                if rbd.startswith(utils.VDI_PREFIXES[utils.get_vdi_type_by_uri(dbg, uri)]):
                    log.debug("%s: SR.ls: SR: %s rbd: %s" % (dbg, uri, rbd))

                    key = utils.get_vdi_uuid_by_name(dbg, rbd)

                    log.debug("%s: SR.ls: SR: %s Image: %s" % (dbg, uri, key))

                    image_meta = meta.RBDMetadataHandler.load(dbg, "%s/%s" % (uri, key))
                    #log.debug("%s: SR.ls: SR: %s image: %s Metadata: %s" % (dbg, uri, rbd, image_meta))

                    results.append({
                            meta.UUID_TAG: image_meta[meta.UUID_TAG],
                            meta.KEY_TAG: image_meta[meta.KEY_TAG],
                            meta.NAME_TAG: image_meta[meta.NAME_TAG],
                            meta.DESCRIPTION_TAG: image_meta[meta.DESCRIPTION_TAG],
                            meta.READ_WRITE_TAG: image_meta[meta.READ_WRITE_TAG],
                            meta.VIRTUAL_SIZE_TAG: image_meta[meta.VIRTUAL_SIZE_TAG],
                            meta.PHYSICAL_UTILISATION_TAG: image_meta[meta.PHYSICAL_UTILISATION_TAG],
                            meta.URI_TAG: image_meta[meta.URI_TAG],
                            meta.CUSTOM_KEYS_TAG: image_meta[meta.CUSTOM_KEYS_TAG],
                            meta.SHARABLE_TAG: image_meta[meta.SHARABLE_TAG]
                    })
                #log.debug("%s: SR.ls: Result: %s" % (dbg, results))
            return results
        except Exception:
            raise xapi.storage.api.v4.volume.Volume_does_not_exist(key)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)


if __name__ == "__main__":
    log.log_call_argv()
    cmd = xapi.storage.api.v4.volume.SR_commandline(Implementation())
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
        raise xapi.storage.api.v4.volume.Unimplemented(base)

