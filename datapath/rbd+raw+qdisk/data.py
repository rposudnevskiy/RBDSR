#!/usr/bin/env python
"""
Data interface for RBD using QEMU qdisk
"""

import os
import sys
import platform

from xapi.storage import log

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.datapath import Unimplemented
    raise Unimplemented(os.path.basename(sys.argv[0]))
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.datapath import Data_commandline, Unimplemented

from xapi.storage.libs.xcpng.librbd.data import Implementation, QdiskData


if __name__ == "__main__":
    log.log_call_argv()
    CMD = Data_commandline(Implementation(QdiskData))
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