#!/usr/bin/env python

from xapi.storage import log
from xapi.storage.libs.xcpng.qemudisk import Qemudisk as _Qemudisk_
from xapi.storage.libs.xcpng.qemudisk import ROOT_NODE_NAME
from xapi.storage.libs.xcpng.librbd.meta import MetadataHandler
from xapi.storage.libs.xcpng.utils import get_sr_name_by_uri, get_vdi_name_by_uri


class Qemudisk(_Qemudisk_):

    def _set_open_args(self, dbg):
        log.debug("%s: xcpng.librbd.qemudisk.Qemudisk._set_open_args" % dbg)

        uri = "rbd+%s+qdisk:///%s/%s" % (self.vdi_type, self.sr_uuid, self.vdi_uuid)

        self.open_args = {'driver': self.vdi_type,
                          'cache': {'direct': True, 'no-flush': True},
                          # 'discard': 'unmap',
                          'file': {'driver': 'rbd',
                                   'pool': get_sr_name_by_uri(dbg, uri),
                                   'image': get_vdi_name_by_uri(dbg, uri)},
                                   # 'node-name': RBD_NODE_NAME},
                          "node-name": ROOT_NODE_NAME}
