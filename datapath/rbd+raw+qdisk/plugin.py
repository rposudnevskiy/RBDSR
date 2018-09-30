#!/usr/bin/env python

import os
import sys
import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.plugin import Plugin_skeleton, Plugin_commandline, Unimplemented
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.plugin import Plugin_skeleton, Plugin_commandline, Unimplemented

from xapi.storage import log


class Implementation(Plugin_skeleton):

    def query(self, dbg):
        if platform.linux_distribution()[1] == '7.5.0':
            return {
                "plugin": "rbd+raw+qdisk",
                "name": "The RBD QEMU qdisk user-space datapath plugin",
                "description": ("This plugin manages and configures qdisk"
                                " instances backend for rbd image in raw format built"
                                " using librbd"),
                "vendor": "Roman V. Posudnevskiy",
                "copyright": "(c) 2017 Roman V. Posudnevskiy",
                "version": "3.0",
                "required_api_version": "4.0",
                "features": [],
                "configuration": {},
                "required_cluster_stack": []}
        elif platform.linux_distribution()[1] == '7.6.0':
            return {
                "plugin": "rbd+raw+qdisk",
                "name": "The RBD QEMU qdisk user-space datapath plugin",
                "description": ("This plugin manages and configures qdisk"
                                " instances backend for rbd image in raw format built"
                                " using librbd"),
                "vendor": "Roman V. Posudnevskiy",
                "copyright": "(c) 2017 Roman V. Posudnevskiy",
                "version": "3.0",
                "required_api_version": "5.0",
                "features": [],
                "configuration": {},
                "required_cluster_stack": []}


if __name__ == "__main__":
    log.log_call_argv()
    CMD = Plugin_commandline(Implementation())
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Plugin.Query":
        CMD.query()
    else:
        raise Unimplemented(CMD_BASE)