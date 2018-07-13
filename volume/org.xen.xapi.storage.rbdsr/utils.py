#!/usr/bin/env python

import re
import urlparse
from xapi.storage import log

RBDPOOL_PREFIX = 'RBD_XenStorage-'
VDI_PREFIX = 'RBD-'
SR_METADATA_IMAGE_NAME = '__meta__'

def get_cluster_name_by_uri(dbg, uri):
    return urlparse.urlparse(uri).netloc

def get_sr_uuid_by_uri(dbg, uri):
    path = urlparse.urlparse(uri).path
    regex = re.compile('^/([abcdefjhklmnopqrstvxyz1234567890-]*)/*([abcdefjhklmnopqrstvxyz1234567890-]*)$')
    result = regex.match(path)
    return result.group(1)

def get_sr_uuid_by_name(dbg, name):
    regex = re.compile(RBDPOOL_PREFIX)
    return regex.sub('', name)

def get_vdi_uuid_by_uri(dbg, uri):
    path = urlparse.urlparse(uri).path
    regex = re.compile('^/([abcdefjhklmnopqrstvxyz1234567890-]*)/*([abcdefjhklmnopqrstvxyz1234567890-]*)$')
    result = regex.match(path)
    return result.group(2)

def get_vdi_uuid_by_name(dbg, name):
    regex = re.compile(VDI_PREFIX)
    return regex.sub('', name)

def get_pool_name_by_uri(dbg, uri):
    return "%s%s" % (RBDPOOL_PREFIX, get_sr_uuid_by_uri(dbg, uri))

def get_image_name_by_uri(dbg, uri):
    return "%s%s" % (VDI_PREFIX, get_vdi_uuid_by_uri(dbg, uri))

def get_current_host_uuid():
    with open("/etc/xensource-inventory") as fd:
        for line in fd:
            if line.strip().startswith("INSTALLATION_UUID"):
                return line.split("'")[1]