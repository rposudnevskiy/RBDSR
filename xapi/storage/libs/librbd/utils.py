#!/usr/bin/env python

import re
import math
import urlparse
import xapi

RBDPOOL_PREFIX = 'RBD_XenStorage-'
VDI_PREFIXES = {'raw': 'RAW-', 'vpc': 'VHD-', 'vhdx': 'VHDX-', 'qcow2': 'QCOW2-', 'qcow': 'QCOW-'}
SR_METADATA_IMAGE_NAME = '__meta__'
VAR_RUN_PREFIX = '/var/run'

MIN_VHD_SIZE = 2 * 1024 * 1024
MAX_VHD_SIZE = 2093050 * 1024 * 1024
VHD_BLOCK_SIZE = 2 * 1024 * 1024
RBD_BLOCK_SIZE = 2 * 1024 * 1024
RBD_BLOCK_ORDER = int(math.log(RBD_BLOCK_SIZE,2))

def get_vdi_type_by_uri(dbg, uri):
    scheme = urlparse.urlparse(uri).scheme
    regex = re.compile('(.*)\+(.*)\+(.*)')
    result = regex.match(scheme)
    return result.group(2)

def subst_vdi_type_in_uri(dbg, uri, type):
    regex = re.compile('(.*)\+(.*)\+(.*)')
    result = regex.match(uri)
    return "%s+%s+%s" % (result.group(1), type, result.group(3))

def get_datapath_by_uri(dbg, uri):
    scheme = urlparse.urlparse(uri).scheme
    regex = re.compile('(.*)\+(.*)\+(.*)')
    result = regex.match(scheme)
    return result.group(3)

def get_cluster_name_by_uri(dbg, uri):
    return urlparse.urlparse(uri).netloc

def get_sr_uuid_by_uri(dbg, uri):
    path = urlparse.urlparse(uri).path
    regex = re.compile('^/([_abcdefjhklmnopqrstvxyz1234567890-]*)/*([_abcdefjhklmnopqrstvxyz1234567890-]*)$')
    result = regex.match(path)
    return result.group(1)

def get_sr_uuid_by_name(dbg, name):
    regex = re.compile(RBDPOOL_PREFIX)
    return regex.sub('', name)

def get_vdi_uuid_by_uri(dbg, uri):
    path = urlparse.urlparse(uri).path
    regex = re.compile('^/([_abcdefjhklmnopqrstvxyz1234567890-]*)/*([_abcdefjhklmnopqrstvxyz1234567890-]*)$')
    result = regex.match(path)
    return result.group(2)

def get_vdi_uuid_by_name(dbg, name):
    regex = re.compile('.*-(.{8}-.{4}-.{4}-.{4}-.{12})')
    result = regex.match(name)
    return result.group(1)

def get_pool_name_by_uri(dbg, uri):
    return "%s%s" % (RBDPOOL_PREFIX, get_sr_uuid_by_uri(dbg, uri))

def get_image_name_by_uri(dbg, uri):
    return "%s%s" % (VDI_PREFIXES[get_vdi_type_by_uri(dbg, uri)], get_vdi_uuid_by_uri(dbg, uri))

#
# Copyright (C) Citrix Systems Inc.
#
# Function from /opt/xensource/sm/vhdutil.py

def calcOverheadEmpty(virtual_size):
    """Calculate the VHD space overhead (metadata size) for an empty VDI of
    size virtual_size"""
    overhead = 0
    size_mb = virtual_size / (1024 * 1024)

    # Footer + footer copy + header + possible CoW parent locator fields
    overhead = 3 * 1024

    # BAT 4 Bytes per block segment
    overhead += (size_mb / 2) * 4
    overhead = roundup(512, overhead)

    # BATMAP 1 bit per block segment
    overhead += (size_mb / 2) / 8
    overhead = roundup(4096, overhead)

    return overhead

def calcOverheadBitmap(virtual_size):
    num_blocks = virtual_size / VHD_BLOCK_SIZE
    if virtual_size % VHD_BLOCK_SIZE:
        num_blocks += 1
    return num_blocks * 4096

def calcOverheadFull(virtual_size):
    """Calculate the VHD space overhead for a full VDI of size virtual_size
    (this includes bitmaps, which constitute the bulk of the overhead)"""
    return calcOverheadEmpty(virtual_size) + calcOverheadBitmap(virtual_size)

def fullSizeVHD(virtual_size):
    return virtual_size + calcOverheadFull(virtual_size)

def roundup(divisor, value):
    """Retruns the rounded up value so it is divisible by divisor."""
    if value == 0:
        value = 1
    if value % divisor != 0:
        return ((int(value) / divisor) + 1) * divisor
    return value

def validate_and_round_vhd_size(size):
    """ Take the supplied vhd size, in bytes, and check it is positive and less
    that the maximum supported size, rounding up to the next block boundary
    """
    if (size < 0 or size > MAX_VHD_SIZE):
        raise xapi.XenAPIException('VDISize',
                                   ['VDI size must be between %d MB and %d MB' %
                                   ((MIN_VHD_SIZE / 1024 / 1024), (MAX_VHD_SIZE / 1024 / 1024))])

    if (size < MIN_VHD_SIZE):
        size = MIN_VHD_SIZE

    size = roundup(VHD_BLOCK_SIZE, size)

    return size
