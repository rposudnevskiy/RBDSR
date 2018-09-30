#!/usr/bin/env python

import os
import sys
import platform

if platform.linux_distribution()[1] == '7.5.0':
    from xapi.storage.api.v4.volume import Volume_skeleton, Volume_commandline, Unimplemented
elif platform.linux_distribution()[1] == '7.6.0':
    from xapi.storage.api.v5.volume import Volume_skeleton, Volume_commandline, Unimplemented

from xapi.storage import log
from xapi.storage.libs.librbd import utils

from xapi.storage.libs.librbd.volume import RAWVolume, QCOW2Volume

class Implementation(Volume_skeleton):

    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("%s: Volume.create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))
        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.create(dbg, sr, name, description, size, sharable)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.create(dbg, sr, name, description, size, sharable)

    def clone(self, dbg, sr, key, mode='clone'):
        log.debug("%s: Volume.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.clone(dbg, sr, key, mode)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.clone(dbg, sr, key, mode)

    def snapshot(self, dbg, sr, key):
        return self.clone(dbg,sr,key, mode='snapshot')

    def destroy(self, dbg, sr, key):
        log.debug("%s: Volume.destroy: SR: %s Key: %s"
                  % (dbg, sr, key))

        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.destroy(dbg, sr, key)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.destroy(dbg, sr, key)

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: Volume.set_name: SR: %s Key: %s New_name: %s"
                  % (dbg, sr, key, new_name))

        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.set_name(dbg, sr, key, new_name)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.set_name(dbg, sr, key, new_name)

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: Volume.set_description: SR: %s Key: %s New_description: %s"
                  % (dbg, sr, key, new_description))

        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.set_description(dbg, sr, key, new_description)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.set_description(dbg, sr, key, new_description)

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: Volume.set: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sr, key, k, v))

        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.set(dbg, sr, key, k, v)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.set(dbg, sr, key, k, v)

    def unset(self, dbg, sr, key, k):
        log.debug("%s: Volume.unset: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sr, key, k))

        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.unset(dbg, sr, key, k)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.unset(dbg, sr, key, k)

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: Volume.resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.resize(dbg, sr, key, new_size)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.resize(dbg, sr, key, new_size)

    def stat(self, dbg, sr, key):
        log.debug("%s: Volume.stat: SR: %s Key: %s"
                  % (dbg, sr, key))

        if utils.get_vdi_type_by_uri(dbg, sr) == 'raw':
            return RAWVolume.stat(dbg, sr, key)
        elif utils.get_vdi_type_by_uri(dbg, sr) == 'qcow2':
            return QCOW2Volume.stat(dbg, sr, key)

##   def compare(self, dbg, sr, key, key2):

##   def similar_content(self, dbg, sr, key):

##    def enable_cbt(self, dbg, sr, key):

##    def disable_cbt(self, dbg, sr, key):

##    def data_destroy(self, dbg, sr, key):

##   def list_changed_blocks(self, dbg, sr, key, key2, offset, length):

if __name__ == "__main__":
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    cmd = Volume_commandline(Implementation())
    base = os.path.basename(sys.argv[0])
    if base == "Volume.create":
        cmd.create()
    elif base == "Volume.snapshot":
        cmd.snapshot()
    elif base == "Volume.clone":
        cmd.clone()
    elif base == "Volume.destroy":
        cmd.destroy()
    elif base == "Volume.set_name":
        cmd.set_name()
    elif base == "Volume.set_description":
        cmd.set_description()
    elif base == "Volume.set":
        cmd.set()
    elif base == "Volume.unset":
        cmd.unset()
    elif base == "Volume.resize":
        cmd.resize()
    elif base == "Volume.stat":
        cmd.stat()
#    elif base == "Volume.compare":
#        cmd.compare()
#    elif base == "Volume.similar_content":
#        cmd.similar_content()
#    elif base == "Volume.enable_cbt":
#        cmd.enable_cbt()
#    elif base == "Volume.disable_cbt":
#        cmd.disable_cbt()
#    elif base == "Volume.data_destroy":
#        cmd.data_destroy()
#    elif base == "Volume.list_changed_blocks":
#        cmd.list_changed_blocks()
    else:
        raise Unimplemented(base)