#!/usr/bin/env python

from xapi.storage.libs.xcpng.data import QdiskData as _QdiskData_
from xapi.storage.libs.xcpng.data import Implementation
from xapi.storage.libs.xcpng.librbd.qemudisk import Qemudisk
from xapi.storage.libs.xcpng.librbd.meta import MetadataHandler


class QdiskData(_QdiskData_):

    def __init__(self):
        super(QdiskData, self).__init__()
        self.MetadataHandler = MetadataHandler()
        self.qemudisk = Qemudisk
