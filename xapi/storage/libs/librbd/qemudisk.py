#!/usr/bin/env python

import utils
import subprocess
import qmp
import os

from xapi.storage.api.v4.volume import Volume_does_not_exist
from xapi.storage import log
from xapi.storage.libs.util import call

QEMU_DP = "/usr/lib64/qemu-dp/bin/qemu-dp"
NBD_CLIENT = "/usr/sbin/nbd-client"

IMAGE_TYPES = ['qcow2', 'qcow', 'vhdx', 'raw']
ROOT_NODE_NAME = 'qemu_node'
RBD_NODE_NAME = 'rbd_node'


def create(dbg, uri):
    log.debug("%s: qemudisk.create: uri: %s " % (dbg, uri))

    vdi_uuid = utils.get_vdi_uuid_by_uri(dbg, uri)
    sr_uuid = utils.get_sr_uuid_by_uri(dbg, uri)
    vdi_type = utils.get_vdi_type_by_uri(dbg, uri)
    if vdi_type not in IMAGE_TYPES:
        raise Exception('Incorrect VDI type')

    nbd_sock = utils.VAR_RUN_PREFIX + "/qemu-nbd.{}".format(vdi_uuid)
    qmp_sock = utils.VAR_RUN_PREFIX + "/qmp_sock.{}".format(vdi_uuid)
    qmp_log  = utils.VAR_RUN_PREFIX + "/qmp_log.{}".format(vdi_uuid)
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

        try:
            path = "{}/{}".format(utils.VAR_RUN_PREFIX, self.vdi_uuid)
            with open(path, 'r') as f:
                line = f.readline().strip()
            call(dbg, ["/usr/bin/xenstore-write", line, "5"])
            os.unlink(path)
        except:
            log.debug("%s: qemudisk.Qemudisk.close: There was no xenstore setup" % dbg)

        _qmp_ = qmp.QEMUMonitorProtocol(self.qmp_sock)
        _qmp_.connect()

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