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

"""Serialization for concurrent operations using rbd locking mechanism"""
import os
import util
import json
import time
from rbdsr_common import RBDPOOL_PREFIX, CEPH_USER_DEFAULT
from fcntl import LOCK_EX, LOCK_NB, flock
from time import sleep

VERBOSE = True
SRLOCK_IMAGE = '__srlock__'
NBD_LOCK_FILE = '/tmp/nbd_lock'
TIMEOUT = 1
MODE_BLOCK = 'b'
MODE_NO_BLOCK = 'n'
MODE_RETRY = 'r'


class UnableToLock(Exception):
    pass


class InvalidMode(Exception):
    pass


def file_lock(lock_file=NBD_LOCK_FILE, mode=MODE_RETRY, retries=5, timeout=TIMEOUT):
    """
    :param lock_file: full path to file that will be used as lock.
    :type lock_file: string
    :param mode: MODE_BLOCK, MODE_NO_BLOCK, MODE_RETRY)
    :type mode: string.
    :param retries: retry x times
    :type retries: int
    :param timeout: wait between retry
    :type timeout: int
    """

    def decorator(target):
        def wrapper(*args, **kwargs):
            util.SMlog('rbdsr_lock.file_lock: trying to aquire')
            try:
                if not (os.path.exists(lock_file) and os.path.isfile(lock_file)):
                    open(lock_file, 'a').close()
            except IOError as e:
                util.SMlog('rbdsr_lock.file_lock: Unable to create lock file: %s' % str(e))
                return

            operation = LOCK_EX
            if mode in [MODE_NO_BLOCK, MODE_RETRY]:
                operation = LOCK_EX | LOCK_NB

            f = open(lock_file, 'a')
            if mode in [MODE_BLOCK, MODE_NO_BLOCK]:
                try:
                    flock(f, operation)
                except IOError as e:
                    util.SMlog('rbdsr_lock.file_lock: Unable to get exclusive lock: %s' % str(e))
                    return

            elif mode == MODE_RETRY:
                for i in range(0, retries + 1):
                    try:
                        flock(f, operation)
                        break
                    except IOError as e:
                        if i == retries:
                            util.SMlog('rbdsr_lock.file_lock: Unable to get exclusive lock: %s' % str(e))
                            return
                        sleep(timeout)

            else:
                raise InvalidMode('rbdsr_lock.file_loc: %s is not a valid mode.')

            # Execute the target
            try:
                util.SMlog('rbdsr_lock.file_lock: lock aquired, run target')
                result = target(*args, **kwargs)
            except Exception as e:
                # Release the lock by closing the file
                f.close()
                raise e

            f.close()
            util.SMlog('rbdsr_lock.file_lock: released lock')
            return result
        return wrapper
    return decorator


class Lock(object):
    """rdb-based locks on a rbd image."""

    def __init__(self, sr_uuid, cephx_id="client.%s" % CEPH_USER_DEFAULT):
        util.SMlog("rbdsr_lock.Lock.__int__: sr_uuid = %s, cephx_id = %s" % (sr_uuid, cephx_id))

        self.sr_uuid = sr_uuid
        self._pool = "%s%s" % (RBDPOOL_PREFIX, sr_uuid)
        self._cephx_id = cephx_id
        self._srlock_image = SRLOCK_IMAGE

        if not self._if_rbd_exist(self._srlock_image):
            util.pread2(["rbd", "create", self._srlock_image, "--size", "0", "--pool", self._pool, "--name",
                         self._cephx_id])

    def _if_rbd_exist(self, rbd_name):
        """
        :param vdi_name:
        :return:
        """
        util.SMlog("rbdsr_lock.Lock._if_vdi_exist: rbd_name=%s" % rbd_name)

        try:
            util.pread2(["rbd", "info", rbd_name, "--pool", self._pool, "--format", "json", "--name",
                         self._cephx_id])
            return True
        except Exception:
            return False

    def _get_srlocker(self):
        util.SMlog("rbdsr_lock.Lock._get_srlocker")
        locks = json.loads(util.pread2(["rbd", "--format", "json", "--name", self._cephx_id, "--pool", self._pool,
                                        "lock", "list", self._srlock_image]))
        try:
            return locks['__locked__']['locker']
        except KeyError:
            return None

    def cleanup(self):
        """Release a previously acquired lock."""
        util.SMlog("rbdsr_lock.Lock.cleanup")

        _locker = self._get_srlocker()
        try:
            util.pread2(["rbd", "--name", self._cephx_id, "--pool", self._pool,
                         "lock", "rm", self._srlock_image, '__locked__', _locker])
            if VERBOSE:
                util.SMlog("rbdsr_lock: released %s" % _locker)
            return True
        except Exception:
            if VERBOSE:
                util.SMlog("rbdsr_lock: Can't release %s" % _locker)
            return False

    def acquire(self):
        """Blocking lock aquisition, with warnings."""
        util.SMlog("rbdsr_lock.Lock.acquire")
        if not self._trylock():
            _locker = self._get_srlocker()
            util.SMlog("rbdsr_lock: Failed to lock on first attempt, blocked by %s... waiting for lock..." % _locker)
            self._lock()
        if VERBOSE:
            _locker = self._get_srlocker()
            util.SMlog("rbdsr_lock: acquired '%s'" % _locker)

    def acquireNoblock(self):
        """Acquire lock if possible, or return false if lock already held"""
        util.SMlog("rbdsr_lock.Lock.acquireNoblock")

        ret = self._trylock()
        exists = self.held()

        if VERBOSE:
            util.SMlog("rbdsr_lock: tried lock, acquired: %s (exists: %s)" % \
                       (ret, exists))
        return ret

    def held(self):
        """True if @self acquired the lock, False otherwise."""
        util.SMlog("rbdsr_lock.Lock.held`")

        if self._get_srlocker() is not None:
            return True
        else:
            return False

    def release(self):
        """Release a previously acquired lock."""
        util.SMlog("rbdsr_lock.Lock.release")

        _locker = self._get_srlocker()
        try:
            util.pread2(["rbd", "--name", self._cephx_id, "--pool", self._pool,
                         "lock", "rm", self._srlock_image, '__locked__', _locker])
            if VERBOSE:
                util.SMlog("rbdsr_lock: released %s" % _locker)
            return True
        except Exception:
            if VERBOSE:
                util.SMlog("rbdsr_lock: Can't release %s" % _locker)
            return False

    def _trylock(self):
        util.SMlog("rbdsr_lock.Lock._trylock")
        if VERBOSE:
            util.SMlog("rbdsr_lock: Trying to lock '%s'" % self._srlock_image)
        if not self.held():
            try:
                util.pread2(["rbd", "--name", self._cephx_id, "--pool", self._pool,
                             "lock", "add", self._srlock_image, '__locked__'])
                if VERBOSE:
                    util.SMlog("rbdsr_lock: acquired")
                return True
            except Exception:
                return False
        else:
            return False

    def _lock(self):
        util.SMlog("rbdsr_lock.Lock._lock")

        while not self._trylock():
            time.sleep(TIMEOUT)


if __debug__:
    import sys
    from datetime import datetime


    def test():

        # Init Lock class
        t1 = datetime.now()
        lock = Lock('8f09c0b5-881b-4dbd-8b70-106fe301c57a')
        t2 = datetime.now()
        delta = t2 - t1
        print("'init' takes %s seconds" % delta.total_seconds())

        # Should not be yet held.
        t1 = datetime.now()
        assert lock.held() == False
        t2 = datetime.now()
        delta = t2 - t1
        print("'held' takes %s seconds" % delta.total_seconds())

        # Create a Lock
        t1 = datetime.now()
        lock.acquire()
        t2 = datetime.now()
        delta = t2 - t1
        print("'acquire' takes %s seconds" % delta.total_seconds())

        # Second lock shall throw in debug mode.
        t1 = datetime.now()
        lock.acquire()
        t2 = datetime.now()
        delta = t2 - t1
        print("'acquire' takes %s seconds" % delta.total_seconds())

        t1 = datetime.now()
        lock.release()
        t2 = datetime.now()
        delta = t2 - t1
        print("'release' takes %s seconds" % delta.total_seconds())

        # lock.cleanup('test')


    if __name__ == '__main__':
        print >> sys.stderr, "Running self tests..."
        test()
        print >> sys.stderr, "OK."
