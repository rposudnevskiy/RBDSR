#!/usr/bin/env python
"""
Datapath for RBD using QEMU qdisk
"""

import os
import sys
import xapi.storage.api.v4.datapath
import xapi.storage.api.v4.volume

from xapi.storage.libs.librbd.datapath import QdiskDatapath

from xapi.storage import log

class Implementation(xapi.storage.api.v4.datapath.Datapath_skeleton):
    """
    Datapath implementation
    """
    def open(self, dbg, uri, persistent):
        log.debug("%s: Datapath.open: uri: %s persistent: %s" % (dbg, uri, persistent))
        return QdiskDatapath.open(dbg, uri, persistent)

    def close(self, dbg, uri):
        log.debug("%s: Datapath.close: uri: %s" % (dbg, uri))
        return QdiskDatapath.close(dbg, uri)

    def attach(self, dbg, uri, domain):
        log.debug("%s: Datapath.attach: uri: %s domain: %s" % (dbg, uri, domain))
        return QdiskDatapath.attach(dbg, uri, domain)

    def detach(self, dbg, uri, domain):
        log.debug("%s: Datapath.detach: uri: %s domain: %s" % (dbg, uri, domain))
        return QdiskDatapath.detach(dbg, uri, domain)

    def activate(self, dbg, uri, domain):
        log.debug("%s: Datapath.activate: uri: %s domain: %s" % (dbg, uri, domain))
        return QdiskDatapath.activate(dbg, uri, domain)

    def deactivate(self, dbg, uri, domain):
        log.debug("%s: Datapath.deactivate: uri: %s domain: %s" % (dbg, uri, domain))
        return QdiskDatapath.deactivate(dbg, uri, domain)


if __name__ == "__main__":
    log.log_call_argv()
    CMD = xapi.storage.api.v4.datapath.Datapath_commandline(Implementation())
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
        raise xapi.storage.api.v4.datapath.Unimplemented(CMD_BASE)