#!/usr/bin/env python
"""
Datapath for RBD using QEMU qdisk
"""

import os
import sys
import subprocess
import xapi.storage.api.datapath
import xapi.storage.api.volume
import urlparse

from xapi.storage import log
import qmp

import utils
import ceph_utils
import rbd_utils

QEMU_DP = "/usr/lib64/qemu-dp/bin/qemu-dp"
NBD_CLIENT = "/usr/sbin/nbd-client"
LEAF_NODE_NAME = 'qemu_node'
NEW_LEAF_NODE_NAME = 'new_qemu_node'

class Implementation(xapi.storage.api.datapath.Datapath_skeleton):
    """
    Datapath implementation
    """
    def open(self, dbg, uri, persistent):
        log.debug("%s: Datapath.open: uri: %s persistent: %s" % (dbg, uri, persistent))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                    utils.VDI_PREFIX, utils.get_vdi_uuid_by_uri(dbg, uri))

        try:
            image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
            if rbd_utils.NON_PERSISTENT_TAG in image_meta:
                vdi_non_persistent = image_meta[rbd_utils.NON_PERSISTENT_TAG]
            else:
                vdi_non_persistent = False

            if (persistent):
                log.debug("%s: Datapath.open: uri: %s will be marked as persistent" % (dbg, uri))
                if vdi_non_persistent:
                    # unmark as non-peristent
                    image_meta = {
                        rbd_utils.NON_PERSISTENT_TAG: None,
                    }
                    rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
                    # on detach remove special snapshot to rollback to on detach
            elif vdi_non_persistent:
                log.debug("%s: Datapath.open: uri: %s already marked as non-persistent" % (dbg, uri))
            else:
                log.debug("%s: Datapath.open: uri: %s will be marked as non-persistent" % (dbg, uri))
                # mark as non-peristent
                image_meta = {
                    rbd_utils.NON_PERSISTENT_TAG: True,
                }
                rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
                # on attach create special snapshot to rollback to on detach
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(uri)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        return None


    def close(self, dbg, uri):
        log.debug("%s: Datapath.close: uri: %s" % (dbg, uri))

        ceph_cluster = ceph_utils.connect(dbg, uri)

        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                    utils.VDI_PREFIX, utils.get_vdi_uuid_by_uri(dbg, uri))

        try:
            image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
            if rbd_utils.NON_PERSISTENT_TAG in image_meta:
                vdi_non_persistent = image_meta[rbd_utils.NON_PERSISTENT_TAG]
            else:
                vdi_non_persistent = False

            log.debug("%s: Datapath.open: uri: %s will be marked as persistent" % (dbg, uri))
            if vdi_non_persistent:
                # unmark as non-peristent
                image_meta = {
                    rbd_utils.NON_PERSISTENT_TAG: None,
                }
                rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(uri)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        return None

    def attach(self, dbg, uri, domain):
        log.debug("%s: Datapath.attach: uri: %s domain: %s" % (dbg, uri, domain))

        protocol = 'Qdisk'

        vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)
        qmp_sock = "/var/run/qmp_sock.%s" % vdi_uuid
        nbd_unix_socket ="/var/run/qemu-nbd.%s" % vdi_uuid
        log.debug("%s: Datapath.attach: Spawning qemu process with qmp socket at %s" % (dbg, qmp_sock))

        cmd = [QEMU_DP, qmp_sock]

        qemu_log = "/var/run/qemu_log.%s" % (vdi_uuid)
        qemu_log_fd = open(qemu_log, 'w+')
        qemu_dp = subprocess.Popen(cmd, stdout=qemu_log_fd, stderr=qemu_log_fd)
        log.debug("%s: New qemu process has pid %s" % (dbg, qemu_dp.pid))

        log.debug("%s: metadata --" % (dbg))

        image_meta = {
            rbd_utils.QEMU_PID: qemu_dp.pid,
            rbd_utils.QEMU_QMP_SOCK: qmp_sock,
            rbd_utils.QEMU_NBD_UNIX_SOCKET: nbd_unix_socket
        }

        log.debug("%s: metadata %s" % (dbg, image_meta))

        ceph_cluster = ceph_utils.connect(dbg, uri)
        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                    utils.VDI_PREFIX, utils.get_vdi_uuid_by_uri(dbg, uri))
        try:
            rbd_utils.updateMetadata(dbg, ceph_cluster, image_name, image_meta)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(uri)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        params = 'nbd:unix:%s' % nbd_unix_socket
        qemu_params = '%s:%s:%s' % (vdi_uuid, LEAF_NODE_NAME, qmp_sock)

        params = "hack|%s|%s" % (params, qemu_params)

        return {
            'domain_uuid': '0',
            'implementation': [protocol, params],
        }

    def detach(self, dbg, uri, domain):
        log.debug("%s: Datapath.open: uri: %s domain: %s" % (dbg, uri, domain))

        vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)

        ceph_cluster = ceph_utils.connect(dbg, uri)
        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                    utils.VDI_PREFIX, utils.get_vdi_uuid_by_uri(dbg, uri))

        try:
            image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(vdi_uuid)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        _qmp_ = qmp.QEMUMonitorProtocol(image_meta[rbd_utils.QEMU_QMP_SOCK])
        _qmp_.connect()
        _qmp_.command('quit')
        _qmp_.close()

    def activate(self, dbg, uri, domain):
        log.debug("%s: Datapath.activate: uri: %s domain: %s" % (dbg, uri, domain))

        vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)

        ceph_cluster_name = urlparse.urlparse(uri).netloc
        ceph_conf_file = "/etc/ceph/%s.conf" % ceph_cluster_name

        ceph_cluster = ceph_utils.connect(dbg, uri)
        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                    utils.VDI_PREFIX, utils.get_vdi_uuid_by_uri(dbg, uri))

        try:
            image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(vdi_uuid)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        _qmp_ = qmp.QEMUMonitorProtocol(image_meta[rbd_utils.QEMU_QMP_SOCK])
        _qmp_.connect()

        args = {'driver': 'raw',
#                'cache': {'direct': True, 'no-flush': True},
                'file': { 'driver': 'rbd',
                          'pool': "%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri)),
                          'image': "%s%s" % (utils.VDI_PREFIX, utils.get_vdi_uuid_by_uri(dbg, uri))},
#                'discard': 'unmap',
                "node-name": LEAF_NODE_NAME}

        log.debug("%s: Datapath.activate: args: %s" % (dbg, args))

        _qmp_.command("blockdev-add", **args)

        # Start an NBD server exposing this blockdev
        _qmp_.command("nbd-server-start",
                      addr={'type': 'unix',
                            'data': {'path': image_meta[rbd_utils.QEMU_NBD_UNIX_SOCKET]}})
        _qmp_.command("nbd-server-add",
                          device=LEAF_NODE_NAME, writable=True)
        _qmp_.close()

    def deactivate(self, dbg, uri, domain):
        log.debug("%s: Datapath.deactivate: uri: %s domain: %s" % (dbg, uri, domain))

        vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)

        ceph_cluster = ceph_utils.connect(dbg, uri)
        image_name = "%s%s/%s%s" % (utils.RBDPOOL_PREFIX, utils.get_sr_uuid_by_uri(dbg, uri),
                                    utils.VDI_PREFIX, utils.get_vdi_uuid_by_uri(dbg, uri))

        try:
            image_meta = rbd_utils.retrieveMetadata(dbg, ceph_cluster, image_name)
        except Exception:
            raise xapi.storage.api.volume.Volume_does_not_exist(vdi_uuid)
        finally:
            ceph_utils.disconnect(dbg, ceph_cluster)

        _qmp_ = qmp.QEMUMonitorProtocol(image_meta[rbd_utils.QEMU_QMP_SOCK])
        _qmp_.connect()

        # Stop the NBD server
        _qmp_.command("nbd-server-stop")

        # Remove the block device
        args = {"node-name": LEAF_NODE_NAME}
        _qmp_.command("blockdev-del", **args)

        _qmp_.close()


if __name__ == "__main__":
    log.log_call_argv()
    CMD = xapi.storage.api.datapath.Datapath_commandline(Implementation())
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Datapath.activate":
        CMD.activate()
    elif CMD_BASE == "Datapath.attach":
        CMD.attach()
    elif CMD_BASE == "Datapath.close":
        CMD.close()
    elif CMD_BASE == "Datapath.deactivate":
        CMD.deactivate()
    elif CMD_BASE == "Datapath.detach":
        CMD.detach()
    elif CMD_BASE == "Datapath.open":
        CMD.open()
    else:
        raise xapi.storage.api.datapath.Unimplemented(CMD_BASE)