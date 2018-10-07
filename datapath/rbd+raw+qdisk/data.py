#!/usr/bin/env python
"""
Data interface for RBD using QEMU qdisk
"""

import os
import sys
import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.datapath import Unimplemented
    raise Unimplemented(os.path.basename(sys.argv[0]))
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.datapath import Data_skeleton, Data_commandline, Unimplemented

from xapi.storage.libs.librbd.datapath import QdiskData
from xapi.storage import log

class Implementation(Data_skeleton):
    """
    Data interface implementation
    """
    def copy(self, dbg, uri, domain, remote, blocklist):
        log.debug("%s: Data.copy: uri: %s domain: %s remote: %s blocklist: %s" % (dbg, uri, domain, remote, blocklist))
        return QdiskData.copy(dbg, uri, domain, remote, blocklist)

    def mirror(self, dbg, uri, domain, remote):
        log.debug("%s: Data.mirror: uri: %s domain: %s remote: %s" % (dbg, uri, domain, remote))
        return QdiskData.mirror(dbg, uri, domain, remote)

    def stat(self, dbg, operation):
        log.debug("%s: Data.stat: operation: %s" % (dbg, operation))
        return QdiskData.stat(dbg, operation)

    def cancel(self, dbg, operation):
        log.debug("%s: Data.cancel: operation: %s" % (dbg, operation))
        return QdiskData.cancel(dbg, operation)

    def destroy(self, dbg, operation):
        log.debug("%s: Data.destroy: operation: %s" % (dbg, operation))
        return QdiskData.destroy(dbg, operation)

    def ls(self, dbg):
        log.debug("%s: Data.ls" % dbg)
        return QdiskData.open(dbg)


if __name__ == "__main__":
    log.log_call_argv()
    CMD = Data_commandline(Implementation())
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Data.copy":
        CMD.copy()
    elif CMD_BASE == "Data.mirror":
        CMD.mirror()
    elif CMD_BASE == "Data.stat":
        CMD.stat()
    elif CMD_BASE == "Data.cancel":
        CMD.cancel()
    elif CMD_BASE == "Data.destroy":
        CMD.destroy()
    elif CMD_BASE == "Data.ls":
        CMD.ls()
    else:
        raise Unimplemented(CMD_BASE)