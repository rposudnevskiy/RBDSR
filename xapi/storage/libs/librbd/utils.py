#!/usr/bin/env python

import re
import urlparse

RBDPOOL_PREFIX = 'RBD_XenStorage-'
VDI_PREFIXES = {'raw': 'RAW-', 'vhdx': 'VHDX-', 'qcow2': 'QCOW2-', 'qcow': 'QCOW-'}
SR_METADATA_IMAGE_NAME = '__meta__'
VAR_RUN_PREFIX = '/var/run'


def get_vdi_type_by_uri(dbg, uri):
    scheme = urlparse.urlparse(uri).scheme
    regex = re.compile('(.*)\+(.*)\+(.*)')
    result = regex.match(scheme)
    return result.group(2)

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
