#!/usr/bin/env python

import os
import sys
import xapi.storage.api.plugin
from xapi.storage import log


class Implementation(xapi.storage.api.plugin.Plugin_skeleton):

    def query(self, dbg):
        return {
            "plugin": "rbd",
            "name": "The RBD QEMU qdisk user-space datapath plugin",
            "description": ("This plugin manages and configures qdisk"
                            " instances backend for rbd image in raw format built"
                            " using ..."),
            "vendor": "Roman V. Posudnevskiy",
            "copyright": "(c) 2017 Roman V. Posudnevskiy",
            "version": "3.0",
            "required_api_version": "3.0",
            "features": [],
            "configuration": {},
            "required_cluster_stack": []}


if __name__ == "__main__":
    log.log_call_argv()
    CMD = xapi.storage.api.plugin.Plugin_commandline(Implementation())
    CMD_BASE = os.path.basename(sys.argv[0])
    if CMD_BASE == "Plugin.Query":
        CMD.query()
    else:
        raise xapi.storage.api.plugin.Unimplemented(CMD_BASE)