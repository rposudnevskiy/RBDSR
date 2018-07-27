#!/usr/bin/env python

import os
import sys
import xapi.storage.api.v4.volume
from xapi.storage import log

from xapi.storage.libs.librbd.volume import RAWVolume

class Implementation(xapi.storage.api.v4.volume.Volume_skeleton):

    def create(self, dbg, sr, name, description, size, sharable):
        log.debug("%s: Volume.create: SR: %s Name: %s Description: %s Size: %s"
                  % (dbg, sr, name, description, size))
        return RAWVolume.create(dbg, sr, name, description, size, sharable)


    def clone(self, dbg, sr, key, mode='clone'):
        log.debug("%s: Volume.%s: SR: %s Key: %s"
                  % (dbg, sys._getframe().f_code.co_name, sr, key))

        return RAWVolume.clone(dbg, sr, key, mode)

    def snapshot(self, dbg, sr, key):
        return self.clone(dbg,sr,key, mode='snapshot')

    def destroy(self, dbg, sr, key):
        log.debug("%s: Volume.destroy: SR: %s Key: %s"
                  % (dbg, sr, key))

        return RAWVolume.destroy(dbg,sr, key)

    def set_name(self, dbg, sr, key, new_name):
        log.debug("%s: Volume.set_name: SR: %s Key: %s New_name: %s"
                  % (dbg, sr, key, new_name))

        return RAWVolume.set_name(dbg, sr, key, new_name)

    def set_description(self, dbg, sr, key, new_description):
        log.debug("%s: Volume.set_description: SR: %s Key: %s New_description: %s"
                  % (dbg, sr, key, new_description))

        return RAWVolume.set_description(dbg, sr, key, new_description)

    def set(self, dbg, sr, key, k, v):
        log.debug("%s: Volume.set: SR: %s Key: %s Custom_key: %s Value: %s"
                  % (dbg, sr, key, k, v))

        return RAWVolume.set(dbg, sr, key, k, v)

    def unset(self, dbg, sr, key, k):
        log.debug("%s: Volume.unset: SR: %s Key: %s Custom_key: %s"
                  % (dbg, sr, key, k))

        return RAWVolume.unset(dbg, sr, key, k)

    def resize(self, dbg, sr, key, new_size):
        log.debug("%s: Volume.resize: SR: %s Key: %s New_size: %s"
                  % (dbg, sr, key, new_size))

        return RAWVolume.resize(dbg, sr, key, new_size)

    def stat(self, dbg, sr, key):
        log.debug("%s: Volume.stat: SR: %s Key: %s"
                  % (dbg, sr, key))

        return RAWVolume.stat(dbg, sr, key)

##   def compare(self, dbg, sr, key, key2):

##   def similar_content(self, dbg, sr, key):

##    def enable_cbt(self, dbg, sr, key):

##    def disable_cbt(self, dbg, sr, key):

##    def data_destroy(self, dbg, sr, key):

##   def list_changed_blocks(self, dbg, sr, key, key2, offset, length):

if __name__ == "__main__":
    """Parse the arguments and call the required command"""
    log.log_call_argv()
    cmd = xapi.storage.api.v4.volume.Volume_commandline(Implementation())
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
        raise xapi.storage.api.v4.volume.Unimplemented(base)