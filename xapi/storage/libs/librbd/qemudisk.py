#!/usr/bin/env python

import subprocess
import qmp
import os
import re
import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_does_not_exist
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import Volume_does_not_exist

from xapi.storage import log
from xapi.storage.libs.util import call
from xapi.storage.libs.librbd import utils
from xapi.storage.libs.util import mkdir_p

QEMU_DP = "/usr/lib64/qemu-dp/bin/qemu-dp"
NBD_CLIENT = "/usr/sbin/nbd-client"
QEMU_DP_SOCKET_DIR = utils.VAR_RUN_PREFIX + "/qemu-dp"

IMAGE_TYPES = ['qcow2', 'qcow', 'vhdx', 'vpc', 'raw']
ROOT_NODE_NAME = 'qemu_node'
SNAP_NODE_NAME = 'snap_node'
RBD_NODE_NAME = 'rbd_node'


def create(dbg, uri):
    log.debug("%s: qemudisk.create: uri: %s " % (dbg, uri))

    vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)
    sr_uuid = utils.get_sr_uuid_by_uri(dbg, uri)
    vdi_type = utils.get_vdi_type_by_uri(dbg, uri)
    if vdi_type not in IMAGE_TYPES:
        raise Exception('Incorrect VDI type')

    mkdir_p(QEMU_DP_SOCKET_DIR, 0o0700)
    nbd_sock = QEMU_DP_SOCKET_DIR + "/qemu-nbd.{}".format(vdi_uuid)
    qmp_sock = QEMU_DP_SOCKET_DIR + "/qmp_sock.{}".format(vdi_uuid)
    qmp_log  = QEMU_DP_SOCKET_DIR + "/qmp_log.{}".format(vdi_uuid)
    log.debug("%s: qemudisk.create: Spawning qemu process for VDI %s with qmp socket at %s"
              % (dbg, vdi_uuid, qmp_sock))

    cmd = [QEMU_DP, qmp_sock]

    log_fd = open(qmp_log, 'w+')
    p = subprocess.Popen(cmd, stdout=log_fd, stderr=log_fd)

    log.debug("%s: qemudisk.create: New qemu process has pid %d" % (dbg, p.pid))

    return Qemudisk(dbg, sr_uuid, vdi_uuid, vdi_type, p.pid, qmp_sock, nbd_sock, qmp_log)

def introduce(dbg, sr_uuid, vdi_uuid, vdi_type, pid, qmp_sock, nbd_sock, qmp_log):
    log.debug("%s: qemudisk.introduce: sr_uuid: %s vdi_uuid: %s vdi_type: %s pid: %d qmp_sock: %s nbd_sock: %s qmp_log: %s"
              % (dbg, sr_uuid, vdi_uuid, vdi_type, pid, qmp_sock, nbd_sock, qmp_log))

    return Qemudisk(dbg, sr_uuid, vdi_uuid, vdi_type, pid, qmp_sock, nbd_sock, qmp_log)


class Qemudisk(object):
    def __init__(self, dbg, sr_uuid, vdi_uuid, vdi_type, pid, qmp_sock, nbd_sock, qmp_log):
        log.debug("%s: qemudisk.Qemudisk.__init__: sr_uuid: %s vdi_uuid: %s vdi_type: %s pid: %d qmp_sock: %s nbd_sock: %s qmp_log: %s"
                  % (dbg, sr_uuid, vdi_uuid, vdi_type, pid, qmp_sock, nbd_sock, qmp_log))

        self.vdi_uuid = vdi_uuid
        self.sr_uuid = sr_uuid
        self.vdi_type = vdi_type
        self.pid = pid
        self.qmp_sock = qmp_sock
        self.nbd_sock = nbd_sock
        self.qmp_log = qmp_log

        self.params = 'nbd:unix:%s' % self.nbd_sock
        qemu_params = '%s:%s:%s' % (self.vdi_uuid, ROOT_NODE_NAME, self.qmp_sock)

        self.params = "hack|%s|%s" % (self.params, qemu_params)

    def quit(self, dbg):
        log.debug("%s: qemudisk.Qemudisk.quit: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))
        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)
        _qmp_.connect()
        _qmp_.command('quit')
        _qmp_.close()

    def open(self, dbg):
        log.debug("%s: qemudisk.Qemudisk.open: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        args = {'driver': self.vdi_type,
                'cache': {'direct': True, 'no-flush': True},
                #'discard': 'unmap',
                'file': {'driver': 'rbd',
                         'pool': "%s%s" % (utils.RBDPOOL_PREFIX, self.sr_uuid),
                         'image': "%s%s" % (utils.VDI_PREFIXES[self.vdi_type], self.vdi_uuid)},
                         #'node-name': RBD_NODE_NAME},
                "node-name": ROOT_NODE_NAME}

        log.debug("%s: qemudisk.Qemudisk.open: args: %s" % (dbg, args))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)
        _qmp_.connect()

        try:
            _qmp_.command("blockdev-add", **args)

            # Start an NBD server exposing this blockdev
            _qmp_.command("nbd-server-start",
                          addr={'type': 'unix',
                                'data': {'path': self.nbd_sock}})
            _qmp_.command("nbd-server-add",
                          device=ROOT_NODE_NAME, writable=True)
            log.debug("%s: qemudisk.Qemudisk.open: RBD Image opened: %s" % (dbg, args))
        except Exception:
            raise Volume_does_not_exist(self.vdi_uuid)
        finally:
            _qmp_.close()

    def close(self, dbg):
        log.debug("%s: qemudisk.Qemudisk.close: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)
        _qmp_.connect()

        if platform.linux_distribution()[1] == '7.5.0':
            try:
                path = "{}/{}".format(utils.VAR_RUN_PREFIX, self.vdi_uuid)
                with open(path, 'r') as f:
                    line = f.readline().strip()
                call(dbg, ["/usr/bin/xenstore-write", line, "5"])
                os.unlink(path)
            except:
                log.debug("%s: qemudisk.Qemudisk.close: There was no xenstore setup" % dbg)
        elif platform.linux_distribution()[1] == '7.6.0':
            path = "{}/{}".format(utils.VAR_RUN_PREFIX, self.vdi_uuid)
            try:
                with open(path, 'r') as f:
                    line = f.readline().strip()
                os.unlink(path)
                args = {'type': 'qdisk',
                        'domid': int(re.search('domain/(\d+)/',
                                               line).group(1)),
                        'devid': int(re.search('vbd/(\d+)/',
                                               line).group(1))}
                _qmp_.command(dbg, "xen-unwatch-device", **args)
            except:
                log.debug("%s: qemudisk.Qemudisk.close: There was no xenstore setup" % dbg)
        try:
            # Stop the NBD server
            _qmp_.command("nbd-server-stop")
            # Remove the block device
            args = {"node-name": ROOT_NODE_NAME}
            _qmp_.command("blockdev-del", **args)
        except Exception:
            raise Volume_does_not_exist(self.vdi_uuid)
        finally:
            _qmp_.close()

    def snap(self, dbg, snap_uri):
        log.debug("%s: qemudisk.Qemudisk.snap: vdi_uuid %s pid %d qmp_sock %s snap_uri %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock, snap_uri))

        if self.vdi_type != 'qcow2':
            raise Exception('Incorrect VDI type')

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)
        _qmp_.connect()

        args = {'driver': 'qcow2',
                'cache': {'direct': True, 'no-flush': True},
                #'discard': 'unmap',

                'file': {'driver': 'rbd',
                         'pool': utils.get_pool_name_by_uri(dbg, snap_uri),
                         'image': utils.get_image_name_by_uri(dbg, snap_uri)},
                         # 'node-name': RBD_NODE_NAME},
                'node-name': SNAP_NODE_NAME,
                'backing': ''}

        _qmp_.command('blockdev-add', **args)

        args = {'node': ROOT_NODE_NAME,
                'overlay': SNAP_NODE_NAME}

        _qmp_.command('blockdev-snapshot', **args)

        _qmp_.close()

    def suspend(self, dbg):
        log.debug("%s: qemudisk.Qemudisk.suspend: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)
        _qmp_.connect()

        try:
            # Suspend IO on blockdev
            args = {"device": ROOT_NODE_NAME}
            _qmp_.command("x-blockdev-suspend", **args)
        except Exception:
            raise Volume_does_not_exist(self.vdi_uuid)
        finally:
            _qmp_.close()

    def resume(self, dbg):
        log.debug("%s: qemudisk.Qemudisk.resume: vdi_uuid %s pid %d qmp_sock %s"
                  % (dbg, self.vdi_uuid, self.pid, self.qmp_sock))

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)
        _qmp_.connect()

        try:
            # Resume IO on blockdev
            args = {"device": ROOT_NODE_NAME}
            _qmp_.command("x-blockdev-resume", **args)
        except Exception:
            raise Volume_does_not_exist(self.vdi_uuid)
        finally:
            _qmp_.close()