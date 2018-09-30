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

    def diagnostics(self, dbg):
        return "No diagnostic data to report"

    def query(self, dbg):
        if platform.linux_distribution()[1] == '7.5.0':
            return {
                "plugin": "rbdsr",
                "name": "RBDSR Volume plugin",
                "description": ("This plugin creates a SR on a "
                                "CEPH pool"),
                "vendor": "Roman V. Posudnevskiy",
                "copyright": "(c) 2017 Roman V. Posudnevskiy",
                "version": "3.0",
                "required_api_version": "4.0",
                "features": [
                    "SR_ATTACH",
                    "SR_DETACH",
                    "SR_CREATE",
                    "VDI_CREATE",
                    "VDI_DESTROY",
                    "VDI_ATTACH",
                    "VDI_ATTACH_OFFLINE",
                    "VDI_DETACH",
                    "VDI_ACTIVATE",
                    "VDI_DEACTIVATE",
                    "VDI_UPDATE",
                    "VDI_CLONE",
                    "VDI_SNAPSHOT",
                    "VDI_RESIZE",
                    "VDI_RESIZE_ONLINE",
                    "SR_METADATA"],
                "configuration": {},
                "required_cluster_stack": []}
        elif platform.linux_distribution()[1] == '7.6.0':
            return {
                "plugin": "rbdsr",
                "name": "RBDSR Volume plugin",
                "description": ("This plugin creates a SR on a "
                                "CEPH pool"),
                "vendor": "Roman V. Posudnevskiy",
                "copyright": "(c) 2017 Roman V. Posudnevskiy",
                "version": "3.0",
                "required_api_version": "5.0",
                "features": [
                    "SR_ATTACH",
                    "SR_DETACH",
                    "SR_CREATE",
                    "VDI_CREATE",
                    "VDI_DESTROY",
                    "VDI_ATTACH",
                    "VDI_ATTACH_OFFLINE",
                    "VDI_DETACH",
                    "VDI_ACTIVATE",
                    "VDI_DEACTIVATE",
                    "VDI_UPDATE",
                    "VDI_CLONE",
                    "VDI_SNAPSHOT",
                    "VDI_RESIZE",
                    "VDI_RESIZE_ONLINE",
                    "SR_METADATA"],
                "configuration": {},
                "required_cluster_stack": []}


if __name__ == "__main__":
    log.log_call_argv()
    cmd = Plugin_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == 'Plugin.diagnostics':
        cmd.diagnostics()
    elif base == 'Plugin.Query':
        cmd.query()
    else:
        raise Unimplemented(base)
