#!/usr/bin/env python
"""
Datapath for Sheepdog VDI using QEMU qdisk
"""

import os
import sys
import platform

from xapi.storage import log

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.datapath import Datapath_commandline, Unimplemented
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.datapath import Datapath_commandline, Unimplemented

from xapi.storage.libs.xcpng.librbd.datapath import Implementation, DATAPATHES

if __name__ == "__main__":
    log.log_call_argv()
    CMD = Datapath_commandline(Implementation(DATAPATHES))
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
        raise Unimplemented(CMD_BASE)