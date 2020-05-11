#!/usr/bin/env python

from rbd import RBD, Image, ImageBusy, ImageExists
from rados import Rados
import os, fnmatch

from xapi.storage import log

VOLBLOCKSIZE = 2097152
RBD_IMAGE_ORDER = 21  # the image is split into (2**order = VOLBLOCKSIZE) bytes objects

def get_config_files_list(dbg):
    log.debug("%s: xcpng.librbd.rbd_utils.get_config_files_list" % (dbg))
    files = []
    pattern = '*.conf'
    if os.path.exists('/etc/ceph'):
        for file in os.listdir('/etc/ceph'):
            if fnmatch.fnmatch(file, pattern):
                files.append(os.path.splitext(os.path.basename(file))[0])
    return files


def ceph_cluster(dbg, cluster_name):
    log.debug("%s: xcpng.librbd.rbd_utils.ceph_cluster: Cluster: %s" % (dbg, cluster_name))

    conf_file = "/etc/ceph/%s.conf" % cluster_name

    if not os.path.exists(conf_file):
        log.error("%s: xcpng.librbd.rbd_utils.ceph_cluster: Config file for CEPH cluster %s doesn't exists." %
                  (dbg, cluster_name))
        raise Exception("CEPH config file %s doesn't exists" % conf_file)

    cluster = Rados(conffile=conf_file)

    return cluster


def rbd_list(dbg, cluster, pool):
    log.debug("%s: xcpng.librbd.rbd_utils.get_rbd_list: %s" % (dbg, pool))
    ioctx = cluster.open_ioctx(pool)
    rbd_inst = RBD()
    try:
        rbds = rbd_inst.list(ioctx)
        return rbds
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.get_rbd_list: Failed to get list of rbds for pool: %s " % (dbg, pool))
        raise Exception(e)
    finally:
        ioctx.close()


def pool_list(dbg, cluster):
    log.debug("%s: xcpng.librbd.rbd_utils.pool_list: Cluster ID: %s" % (dbg, cluster.get_fsid()))
    try:
        pools = cluster.list_pools()
        return pools
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.get_pool_list: Failed to get list of pools for Cluster ID: %s" %
                  (dbg, cluster.get_fsid()))
        raise Exception(e)


def rbd_create(dbg, cluster, pool, name, size):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_create: Cluster ID: %s Pool: %s Name: %s Size: %s"
              % (dbg, cluster.get_fsid(), pool, name, size))
    ioctx = cluster.open_ioctx(pool)
    rbd_inst = RBD()
    try:
        rbd_inst.create(ioctx, name, size, order=RBD_IMAGE_ORDER)
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_create: Failed to create an image: Cluster ID: %s Pool %s Name: %s Size: %s"
                  % (dbg, cluster.get_fsid(), pool, name, size))
        raise Exception(e)
    finally:
        ioctx.close()


def rbd_remove(dbg, cluster, pool, name):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_remove: Cluster ID: %s Pool %s Name: %s"
              % (dbg, cluster.get_fsid(), pool, name))
    ioctx = cluster.open_ioctx(pool)
    rbd_inst = RBD()
    try:
        rbd_inst.remove(ioctx, name)
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_remove: Failed to remove an image: Cluster ID: %s Pool %s Name: %s"
                  % (dbg, cluster.get_fsid(), pool, name))
        raise Exception(e)
    finally:
        ioctx.close()


def rbd_resize(dbg, cluster, pool, name, size):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_resize: Cluster ID: %s Pool: %s Name: %s Size: %s"
              % (dbg, cluster.get_fsid(), pool, name, size))
    ioctx = cluster.open_ioctx(pool)
    image = Image(ioctx, name)

    try:
        image.resize(size)
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_resize: Failed to resize an image: Cluster ID: %s Pool %s Name: %s Size: %s"
                  % (dbg, cluster.get_fsid(), pool, name, size))
        raise Exception(e)
    finally:
        ioctx.close()


def rbd_utilization(dbg, cluster, pool, name):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_utilization: Cluster ID: %s Pool: %s Name: %s"
              % (dbg, cluster.get_fsid(), pool, name))
    ioctx = cluster.open_ioctx(pool)
    image = Image(ioctx, name)

    try:
        image_stat = image.stat()
        return image_stat['num_objs']*image_stat['obj_size']
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_utilisation: Failed to get an image utilization: Cluster ID: %s Pool %s Name: %s"
                  % (dbg, cluster.get_fsid(), pool, name))
        raise Exception(e)
    finally:
        ioctx.close()


def rbd_rename(dbg, cluster, pool, old_name, new_name):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_rename: Cluster ID: %s Pool: %s Old name: %s New name: %s"
              % (dbg, cluster.get_fsid(), pool, old_name, new_name))

    ioctx = cluster.open_ioctx(pool)
    rbd_inst = RBD()

    try:
        rbd_inst.rename(ioctx, old_name, new_name)
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_utilisation: Failed to get an image utilization: Cluster ID: %s Pool %s Old Name: %s New Name: %s"
                  % (dbg, cluster.get_fsid(), pool, old_name, new_name))
        raise Exception(e)
    finally:
        ioctx.close()


def rbd_clone(dbg, cluster, parent_pool, parent, snapshot, clone_pool, clone):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_clone: Cluster ID: %s Parent Pool: %s Parent: %s Snapshot: %s Clone Pool: %s Clone: %s"
              % (dbg, cluster.get_fsid(), parent_pool, parent, snapshot, clone_pool, clone))
    p_ioctx = cluster.open_ioctx(parent_pool)
    p_image = Image(p_ioctx, parent)
    c_ioctx = cluster.open_ioctx(clone_pool)
    rbd_inst = RBD()

    try:
        if not p_image.is_protected_snap(snapshot):
            p_image.protect_snap(snapshot)
        rbd_inst.clone(p_ioctx, parent, snapshot, c_ioctx, clone)
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_clone: Failed to make a clone: Cluster ID: %s Parent Pool: %s Parent: %s Snapshot: %s Clone Pool: %s Clone: %s"
              % (dbg, cluster.get_fsid(), parent_pool, parent, snapshot, clone_pool, clone))
        raise Exception(e)
    finally:
        p_ioctx.close()
        c_ioctx.close()


def rbd_snapshot(dbg, cluster, pool, name, snapshot):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_snapshot: Cluster ID: %s Pool: %s Name: %s Snapshot: %s"
        % (dbg, cluster.get_fsid(), pool, name, snapshot))
    ioctx = cluster.open_ioctx(pool)
    image = Image(ioctx, name)

    try:
        image.create_snap(snapshot)
    except Exception as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_clone: Failed to take a snapshot: Cluster ID: %s Pool: %s Name: %s Snapshot: %s"
                  % (dbg, cluster.get_fsid(), pool, name, snapshot))
        raise Exception(e)
    finally:
        ioctx.close()


def rbd_exists(dbg, cluster, pool, name):
    log.debug("%s: rbd_utils.rbd_exist: Cluster ID: %s Image: %s"
              % (dbg, cluster.get_fsid(), name))
    ioctx = cluster.open_ioctx(pool)
    try:
        Image(ioctx, name)
        return True
    except Exception:
        return False
    finally:
        ioctx.close()


def rbd_lock(dbg, cluster, pool, name):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_lock: Cluster ID: %s Pool: %s Name: %s"
              % (dbg, cluster.get_fsid(), pool, name))
    ioctx = cluster.open_ioctx(pool)
    image = Image(ioctx, name)
    try:
        image.lock_exclusive('xapi-xcpng-lock')
        return ioctx, image
    except ImageBusy or ImageExists as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_lock: Failed to acquire exclusive lock: Cluster ID: %s Pool: %s Name: %s"
                  % (dbg, cluster.get_fsid(), pool, name))
        image.close()
        ioctx.close()
        raise Exception(e)

def is_locked(dbg, cluster, pool, name):
    log.debug("%s: xcpng.librbd.rbd_utils.is_locked: Cluster ID: %s Pool: %s Name: %s"
              % (dbg, cluster.get_fsid(), pool, name))
    ioctx = cluster.open_ioctx(pool)
    image = Image(ioctx, name)
    if len(image.list_lockers()) > 0:
        return True
    else:
        return False


def rbd_unlock(dbg, lh):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_unlock" % (dbg))
    lh[2].unlock('xapi-xcpng-lock')
    lh[2].close()
    lh[1].close()


def rbd_read(dbg, cluster, pool, name, offset, length):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_read: Cluster ID: %s Pool: %s Name: %s Offset: %s Length: %s"
              % (dbg, cluster.get_fsid(), pool, name, offset, length))
    ioctx = cluster.open_ioctx(pool)
    try:
        image = Image(ioctx, name)
        return image.read(offset, length)
    except ImageBusy or ImageExists as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_read: Failed to read from the image: Cluster ID: %s Pool: %s Name: %s"
                  % (dbg, cluster.get_fsid(), pool, name))
        raise Exception(e)
    finally:
        ioctx.close()


def rbd_write(dbg, cluster, pool, name, data, offset, length):
    log.debug("%s: xcpng.librbd.rbd_utils.rbd_wite: Cluster ID: %s Pool: %s Name: %s Offset: %s Length: %s"
              % (dbg, cluster.get_fsid(), pool, name, offset, length))
    ioctx = cluster.open_ioctx(pool)
    try:
        image = Image(ioctx, name)
        image.write(data, offset)
    except ImageBusy or ImageExists as e:
        log.error("%s: xcpng.librbd.rbd_utils.rbd_lock: Failed to write to the image: Cluster ID: %s Pool: %s Name: %s"
                  % (dbg, cluster.get_fsid(), pool, name))
        raise Exception(e)
    finally:
        ioctx.close()
