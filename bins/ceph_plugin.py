#!/usr/bin/python
#
# Copyright (C) Roman V. Posudnevskiy (ramzes_r@yahoo.com)
#
# This program is free software; you can redistribute it and/or modify
# it under the terms of the GNU Lesser General Public License as published
# by the Free Software Foundation; version 2.1 only.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU Lesser General Public License for more details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with this program; if not, write to the Free Software Foundation, Inc.,
# 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301  USA

import os
import re
import sys
import XenAPIPlugin

sys.path.append("/opt/xensource/sm/")
import util
import os.path
from rbdsr_lock import file_lock


def _disable_rbd_caching(userbdmeta, CEPH_POOL_NAME, _vdi_name):
    if userbdmeta == 'True':
        util.pread2(['rbd', 'image-meta', 'set', "%s/%s" % (CEPH_POOL_NAME, _vdi_name), 'conf_rbd_cache', 'false'])
    if userbdmeta == 'False':
        if not os.path.isfile("/etc/ceph/ceph.conf.nocaching"):
            os.system("printf \"[client]\\n\\trbd cache = false\\n\\n\" > /etc/ceph/ceph.conf.nocaching")
            os.system("cat /etc/ceph/ceph.conf >> /etc/ceph/ceph.conf.nocaching")


def _find_nbd_devices_used(use_dev, NBDS_MAX):
    nbd_devices_used = re.findall(r'/dev/nbd([0-9]{1,2})', util.pread2(['rbd', 'nbd', 'ls']))
    if nbd_devices_used:
        nbd_devices_used = sorted([int(x) for x in nbd_devices_used])
        if use_dev in nbd_devices_used:
            free_devices = [x for x in range(3, int(NBDS_MAX) + 1) if x not in nbd_devices_used]
            if free_devices:
                return free_devices[0]
            else:
                util.SMlog('_map: NBD_MAX level reached')
                return False
    return use_dev


@file_lock()
def _map(session, arg_dict):
    """ called with devlinks """
    mode = arg_dict['mode']
    dev_name = arg_dict['dev_name']
    _dev_name = arg_dict['_dev_name']
    _dmdev_name = arg_dict['_dmdev_name']
    _dm_name = arg_dict['_dm_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    sharable = arg_dict['sharable']
    disable_caching = arg_dict['disable_caching']
    size = arg_dict['size']
    dmmode = arg_dict['dmmode']
    _dmbasedev_name = ''

    if '_dmbasedev_name' in arg_dict:
        _dmbasedev_name = arg_dict['_dmbasedev_name']

    if '_snap_name' in arg_dict:
        _vdi_name = arg_dict['_snap_name']
    else:
        _vdi_name = arg_dict['_vdi_name']

    cmd = None
    if mode == 'kernel':
        cmd = ['rbd', 'map', _vdi_name, '--pool', CEPH_POOL_NAME, '--name', CEPH_USER]
    elif mode == 'nbd':
        use_dev = _find_nbd_devices_used(int(arg_dict['dev']), NBDS_MAX)
        if not use_dev:
            util.SMlog('_map: Could not allocate nbd device for "%s": use_dev: %s'
                       % (arg_dict['dev'], use_dev))
            return False

        dev = "%s%s" % ('/dev/nbd', use_dev)
        if sharable == 'True' or disable_caching == 'True':
            util.SMlog('_map: disabling rbd cache for %s' % _vdi_name)
            _disable_rbd_caching(arg_dict['userbdmeta'], CEPH_POOL_NAME, _vdi_name)

        if sharable == 'True':
            if arg_dict['userbdmeta'] == 'True':
                cmd = ['rbd-nbd', 'map', '--device', dev, '--nbds_max', NBDS_MAX,
                       "%s/%s" % (CEPH_POOL_NAME, _vdi_name), '--name', CEPH_USER]
            else:
                cmd = ['rbd-nbd', 'map', '--device', dev, '--nbds_max', NBDS_MAX, '-c',
                       '/etc/ceph/ceph.conf.nocaching', "%s/%s" % (CEPH_POOL_NAME, _vdi_name), '--name', CEPH_USER]
        else:
            cmd = ['rbd-nbd', 'map',
                   '--name', CEPH_USER,
                   '--device', dev,
                   '--nbds_max', NBDS_MAX,
                   "%s/%s" % (CEPH_POOL_NAME, _vdi_name)]

        util.pread2(['ln', '-f', dev, _dev_name])

    if cmd is not None:
        if arg_dict['read_only'] == 'True':
            cmd.append('--read-only')

        # util.pread2(cmd).rstrip('\n')
        util.pread2(cmd)

    if dmmode == 'linear':
        util.pread2(['dmsetup', 'create', _dm_name, '--table', "0 %s linear %s 0" % (str(int(size) / 512), _dev_name)])
        util.pread2(['ln', '-sf', _dmdev_name, dev_name])
    elif dmmode == 'mirror':
        _dmzero_name = "%s%s" % (_dm_name, '-zero')
        _dmzerodev_name = "%s%s" % (_dmdev_name, '-zero')
        util.pread2(['dmsetup', 'create', _dmzero_name, '--table', "0 %s zero" % str(int(size) / 512)])
        util.pread2(['dmsetup', 'create', _dm_name, '--table', "0 %s snapshot %s %s P 1" % (str(int(size) / 512),
                                                                                            _dmzerodev_name, _dev_name)])
        util.pread2(['ln', '-sf', _dmdev_name, dev_name])
    elif dmmode == 'base':
        _dmbase_name = "%s%s" % (_dm_name, '-base')
        _dmbasedev_name = "%s%s" % (_dmdev_name, '-base')
        util.pread2(['dmsetup', 'create', _dmbase_name, '--table', "0 %s snapshot-origin %s" % (str(int(size) / 512),
                                                                                                _dev_name)])
        util.pread2(['ln', '-sf', _dmbasedev_name, dev_name])
    elif dmmode == 'cow':
        # util.pread2(['dmsetup', 'suspend', _dmbase_name])
        util.pread2(['dmsetup', 'create', _dm_name, '--table', "0 %s snapshot %s %s P 1" % (str(int(size) / 512),
                                                                                            _dmbasedev_name,
                                                                                            _dev_name)])
        # util.pread2(['dmsetup', 'resume', _dmbase_name])
        util.pread2(['ln', '-sf', _dmdev_name, dev_name])
    elif dmmode == 'cow2base':
        util.pread2(['dmsetup', 'create', _dm_name, '--table', "0 %s snapshot %s %s P 1" % (str(int(size) / 512),
                                                                                            _dmbasedev_name,
                                                                                            _dev_name)])
        _dmbase_name = "%s%s" % (_dm_name, '-base')
        _dmbasedev_name = "%s%s" % (_dmdev_name, '-base')
        util.pread2(['dmsetup', 'create', _dmbase_name, '--table', "0 %s snapshot-origin %s" % (str(int(size) / 512),
                                                                                                _dmdev_name)])
        util.pread2(['ln', '-sf', _dmbasedev_name, dev_name])
    else:
        util.pread2(['ln', '-sf', _dev_name, dev_name])

    return 'mapped'


def _unmap(session, arg_dict):
    mode = arg_dict['mode']
    dev_name = arg_dict['dev_name']
    _dev_name = arg_dict['_dev_name']
    _dm_name = arg_dict['_dm_name']
    CEPH_USER = arg_dict['CEPH_USER']
    dmmode = arg_dict['dmmode']

    dev = util.pread2(['realpath', _dev_name]).rstrip('\n')

    util.pread2(['unlink', dev_name])
    if dmmode == 'linear':
        util.pread2(['dmsetup', 'remove', _dm_name])
    elif dmmode == 'mirror':
        _dmzero_name = "%s%s" % (_dm_name, "-zero")
        util.pread2(['dmsetup', 'remove', _dm_name])
        util.pread2(['dmsetup', 'remove', _dmzero_name])
    elif dmmode == 'base':
        _dmbase_name = "%s%s" % (_dm_name, '-base')
        util.pread2(['dmsetup', 'remove', _dmbase_name])
    elif dmmode == 'cow':
        util.pread2(['dmsetup', 'remove', _dm_name])
    elif dmmode == 'cow2base':
        _dmbase_name = "%s%s" % (_dm_name, '-base')
        util.pread2(['dmsetup', 'remove', _dmbase_name])
        util.pread2(['dmsetup', 'remove', _dm_name])

    if mode == 'kernel':
        util.pread2(['rbd', 'unmap', dev, '--name', CEPH_USER])
    elif mode == 'fuse':
        pass
    elif mode == 'nbd':
        util.pread2(['rbd-nbd', 'unmap', dev, '--name', CEPH_USER])
        util.pread2(['unlink', _dev_name])

    return "unmapped"


@file_lock()
def __map(session, arg_dict):
    mode = arg_dict['mode']
    _dm_name = arg_dict['_dm_name']
    CEPH_POOL_NAME = arg_dict['CEPH_POOL_NAME']
    CEPH_USER = arg_dict['CEPH_USER']
    NBDS_MAX = arg_dict['NBDS_MAX']
    sharable = arg_dict['sharable']
    disable_caching = arg_dict['disable_caching']
    dmmode = arg_dict['dmmode']
    _vdi_name = arg_dict['_vdi_name']

    if arg_dict['read_only'] == 'True':
        read_only = '--read-only'
    else:
        read_only = ''

    cmd = None
    if mode == 'kernel':
        cmd = ['rbd', 'map', _vdi_name, '--pool', CEPH_POOL_NAME, '--name', CEPH_USER]
    elif mode == 'nbd':
        use_dev = _find_nbd_devices_used(int(arg_dict['dev']), NBDS_MAX)
        if not use_dev:
            util.SMlog('_map: Could not allocate nbd device for "%s": use_dev: %s'
                       % (arg_dict['dev'], use_dev))
            return False

        dev = "%s%s" % ('/dev/nbd', use_dev)
        if sharable == 'True' or disable_caching == 'True':
            util.SMlog('_map: disabling rbd cache for %s' % _vdi_name)
            _disable_rbd_caching(arg_dict['userbdmeta'], CEPH_POOL_NAME, _vdi_name)

        if sharable == 'True':
            if arg_dict['userbdmeta'] == 'True':
                cmd = ['rbd-nbd', 'map', '--device', dev, '--nbds_max', NBDS_MAX,
                       "%s/%s" % (CEPH_POOL_NAME, _vdi_name), '--name', CEPH_USER]
            else:
                cmd = ['rbd-nbd', 'map', '--device', dev, '--nbds_max', NBDS_MAX, '-c',
                       '/etc/ceph/ceph.conf.nocaching', "%s/%s" % (CEPH_POOL_NAME, _vdi_name),
                       '--name', CEPH_USER]
        else:
            cmd = ['rbd-nbd', 'map',
                   '--name', CEPH_USER,
                   '--device', dev,
                   '--nbds_max', NBDS_MAX,
                   "%s/%s" % (CEPH_POOL_NAME, _vdi_name)]

    if cmd is not None:
        if arg_dict['read_only'] == 'True':
            cmd.append('--read-only')

        # util.pread2(cmd).rstrip('\n')
        util.pread2(cmd)

    if dmmode != 'None':
        util.pread2(['dmsetup', 'resume', _dm_name])

    return 'mapped'


def __unmap(session, arg_dict):
    mode = arg_dict['mode']
    _dev_name = arg_dict['_dev_name']
    _dm_name = arg_dict['_dm_name']
    CEPH_USER = arg_dict['CEPH_USER']
    dmmode = arg_dict['dmmode']

    dev = util.pread2(['realpath', _dev_name]).rstrip('\n')

    if dmmode != 'None':
        util.pread2(['dmsetup', 'suspend', _dm_name])

    if mode == 'kernel':
        util.pread2(['rbd', 'unmap', dev, '--name', CEPH_USER])
    elif mode == 'fuse':
        pass
    elif mode == 'nbd':
        util.pread2(['rbd-nbd', 'unmap', dev, '--name', CEPH_USER])

    return "unmapped"


if __name__ == '__main__':
    XenAPIPlugin.dispatch({'map': _map,
                           'unmap': _unmap,
                           '_map': __map,
                           '_unmap': __unmap})
