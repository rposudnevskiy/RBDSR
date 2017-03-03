#!/usr/bin/python -u
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
# 51 Franklin Street, Fifth math.floor, Boston, MA  02110-1301  USA

from __future__ import print_function
from struct import *
import uuid
import sys, getopt
import re
import time

SECTOR_SIZE = 512
VHD_DEFAULT_BLOCK_SIZE = 2097152
VHD_DYNAMIC_HARDDISK_TYPE = 0x00000003# Dynamic hard disk
VHD_DIFF_HARDDISK_TYPE = 0x00000004# Differencing hard disk

VDI_PREFIX = "VHD-"
SNAPSHOT_PREFIX = "SNAP-"

#-- VHD FOOTTER FIELDs --#
_vhd_footter_cookie_                 = 0
_vhd_footter_features_               = 1
_vhd_footter_file_format_version_    = 2
_vhd_footter_data_offset_            = 3
_vhd_footter_time_stamp_             = 4
_vhd_footter_creator_application_    = 5
_vhd_footter_creator_version_        = 6
_vhd_footter_creator_host_os_        = 7
_vhd_footter_original_size_          = 8
_vhd_footter_current_size_           = 9
_vhd_footter_disk_geometry_          = 10
_vhd_footter_disk_type_              = 11
_vhd_footter_checksum_               = 12
_vhd_footter_unique_iq_              = 13
_vhd_footter_saved_state_            = 14
_vhd_footter_rbd_image_uuid_         = 15
#-- VHD FOOTTER FIELDs --#

#-- VHD DISK GEOMETRY FIELs --#
_vhd_disk_geometry_cylinders_            = 0
_vhd_disk_geometry_heads_                = 1
_vhd_disk_geometry_sectors_per_cylinder_ = 2
#-- VHD DISK GEOMETRY FIELs --#

#-- DYNAMIC DISK HEADER FIELDs --#
_dynamic_disk_header_cookie_                    = 0
_dynamic_disk_header_data_offset_               = 1
_dynamic_disk_header_table_offset_              = 2
_dynamic_disk_header_header_version_            = 3
_dynamic_disk_header_max_table_entries_         = 4
_dynamic_disk_header_block_size_                = 5
_dynamic_disk_header_checksum_                  = 6
_dynamic_disk_header_parent_unique_id_          = 7
_dynamic_disk_header_parent_time_stamp_         = 8
_dynamic_disk_header_parent_unicode_name_       = 10
_dynamic_disk_header_parent_locator_entry_1_    = 11
_dynamic_disk_header_parent_locator_entry_2_    = 12
_dynamic_disk_header_parent_locator_entry_3_    = 13
_dynamic_disk_header_parent_locator_entry_4_    = 14
_dynamic_disk_header_parent_locator_entry_5_    = 15
_dynamic_disk_header_parent_locator_entry_6_    = 16
_dynamic_disk_header_parent_locator_entry_7_    = 17
_dynamic_disk_header_parent_locator_entry_8_    = 18
#-- DYNAMIC DISK HEADER FIELDs --#

#-- BATMAP FIELDs --#
_batmap_cookie_      = 0
_batmap_offset_      = 1
_batmap_size_        = 2
_batmap_version_     = 3
_batmap_checksum_    = 4
_batmap_marker_      = 5
#-- BATMAP FIELDs --#

#-- PARENT LOCATOR ENTRY FIELD --#
_parent_locator_platform_code_          = 0
_parent_locator_platform_data_space_    = 1
_parent_locator_platform_data_length_   = 2
_parent_locator_platform_data_offset_   = 4
#-- PARENT LOCATOR ENTRY FIELD --#_

#-- DISK TYPES --#
_disk_type_none                     = 0
_disk_type_fixed_hard_disk          = 2
_disk_type_dynamic_hard_disk        = 3
_disk_type_differencing_hard_disk   = 4
#-- DISK TYPES --#

#-- PLATFORM CODEs --#
_platform_code_None_    = 0x0
_platform_code_Wi2r_    = 0x57693272
_platform_code_Wi2k_    = 0x5769326B
_platform_code_W2ru_    = 0x57327275
_platform_code_W2ku_    = 0x57326B75
_platform_code_Mac_     = 0x4D616320
_platform_code_MacX_    = 0x4D616358
#-- PLATFORM CODEs --#

VHD_FOTTER_FORMAT = "!8sIIQI4sIIQQ4sII16sB16s411s"
VHD_FOTTER_RECORD_SIZE = 512
VHD_DISK_GEOMETRY_FORMAT = "!HBB"
VHD_DISK_GEOMETRY_RECORD_SIZE = 4
VHD_DYNAMIC_DISK_HEADER_FORMAT = "!8sQQIIII16sII512s24s24s24s24s24s24s24s24s256s"
VHD_DYNAMIC_DISK_HEADER_RECORD_SIZE = 1024
VHD_PARENT_LOCATOR_ENTRY_FORMAT = "!IIIIQ"
VHD_PARENT_LOCATOR_ENTRY_RECORD_SIZE = 24
VHD_PARENT_LOCATORS_COUNT = 9
VHD_BATMAP_HEADER_FORMAT = "!8sQIIIB483s"
VHD_BATMAP_HEADER_SIZE = 512

#-- RBD DIFF v1 META AND DATA FIELDs --#
RBD_HEADER = "rbd diff v1\n"
RBD_DIFF_META_ENDIAN_PREFIX = "<"#bigendian ! or littleendian <
RBD_DIFF_META_RECORD_TAG = "c"
RBD_DIFF_META_RECORD_TAG_SIZE = 1
RBD_DIFF_META_SNAP = "I"
RBD_DIFF_META_SNAP_SIZE = 4
RBD_DIFF_META_SIZE = "Q"
RBD_DIFF_META_SIZE_SIZE = 8
RBD_DIFF_DATA = "QQ"
RBD_DIFF_DATA_SIZE = 16
#-- RBD DIFF v1 META AND DATA FIELDs --#
#-------------------------------------------------------------------------------------------------------------------------------------------------------#
def eprint(*args, **kwargs):
    print(*args, file=sys.stderr, **kwargs)

def INFO(v, string):
    if v is True:
        eprint("[INFO]%s" % string)

def ERROR(v, string):
    if v is not True:
        eprint("[ERROR]%s" % string)
    else:
        eprint("[ERROR]%s" % string)
        
def modTupleByIndex(tup, index, ins):
    return tuple(tup[0:index]) + (ins,) + tuple(tup[index+1:])

def checksum(vhd_record):
    checksum = 0
    b = bytearray()
    b.extend(vhd_record)
    for index in range(VHD_FOTTER_RECORD_SIZE):
        checksum += b[index]
    checksum = ~checksum + 2**32
    return checksum

def get_size_aligned_to_sector_boundary(size):
    if size%SECTOR_SIZE>0:
        aligned_size = ((size//SECTOR_SIZE)+1)*SECTOR_SIZE
    else:
        aligned_size = size
    return aligned_size

def get_bitmap_size(dynamic_disk_header):
    sectors_in_block = dynamic_disk_header[_dynamic_disk_header_block_size_]/SECTOR_SIZE 
    bitmap_size = sectors_in_block/8
    return get_size_aligned_to_sector_boundary(bitmap_size)

def gen_empty_bitarray_for_bitmap(bitmap_size):
    bitarray = []
    for bitmap_index in range(bitmap_size):
        for bit_index in range(8):
            bitarray.append(0)
    return bitarray

def gen_bitmap_from_bitarray(bitarray):
    _bytearray_ = {}
    bitmap = ''
    for bitarray_index in range(len(bitarray)):
        bit = 0
        byte_index = bitarray_index//8
        bit_in_byte = bitarray_index%8
        if bitarray[bitarray_index] == 1:
            bit = 128 >> bit_in_byte
        if _bytearray_.has_key(byte_index):
            _bytearray_[byte_index] = _bytearray_[byte_index] | bit
        else:
            _bytearray_[byte_index] = bit
    for byte_index in range(len(_bytearray_)):
        bitmap = bitmap + pack('!c', chr(_bytearray_[byte_index]))
    return bitmap

def get_bitarray_from_bitmap(bitmap, bitmap_size):
    bitarray = []
    _bitmap_ = bytearray()
    _bitmap_.extend(bitmap)
    for bitmap_index in range(bitmap_size):
        for bit_index in range(8):
            offset = 128 >> bit_index
            if (_bitmap_[bitmap_index] & offset) > 0:
                bitarray.append(1)
            else:
                bitarray.append(0)
    return bitarray

def gen_empty_vhd_bat(image_size):
    bat_list = []
    max_tab_entries = image_size / VHD_DEFAULT_BLOCK_SIZE
    for bat_index in range(max_tab_entries):
        bat_list.append(0xffffffff)
    return bat_list

def gen_empty_batmap():
    return pack("!%ds" % (SECTOR_SIZE*2), '')

def gen_batmap_header(batmap):
    reserved = ''
    for i in range(483):
        reserved = reserved + pack('!c', chr(0))
    batmap_header_struct = ('tdbatmap', 0, 0, 0x00010002, 0, 0, reserved) #empty
    return batmap_header_struct

def pack_vhd_bat(bat_list):
    max_tab_entries = len(bat_list)
    bat = ''
    for bat_index in range(max_tab_entries):
        bat = bat + pack("!I", bat_list[bat_index])
    if max_tab_entries < SECTOR_SIZE/4:
        bat = bat + pack("!%ds" % (SECTOR_SIZE - max_tab_entries*4), '')
    return bat

def gen_vhd_geometry_struct(image_size):
    totalSectors = image_size / SECTOR_SIZE
    if totalSectors > 65535*16*255:
        totalSectors = 65535*16*255
    if totalSectors >= 65535*16*255:
        sectorsPerTrack = 255
        heads = 16
        cylinderTimesHeads = totalSectors / sectorsPerTrack
    else:
        sectorsPerTrack = 17
        cylinderTimesHeads = totalSectors / sectorsPerTrack
        heads = (cylinderTimesHeads + 1023) / 1024
        if heads < 4:
            heads = 4
        if (cylinderTimesHeads >= (heads * 1024)) | (heads > 16):
            sectorsPerTrack = 31
            heads = 16
            cylinderTimesHeads = totalSectors / sectorsPerTrack
        if cylinderTimesHeads >= (heads * 1024):
            sectorsPerTrack = 63
            heads = 16
            cylinderTimesHeads = totalSectors / sectorsPerTrack
    cylinders = cylinderTimesHeads / heads
    geometry_struct = (cylinders, heads, sectorsPerTrack)
    #INFO(verbose, "[gen_vhd_geometry_struct]: totalSectors = %d, Cyliders = %d, heads = %d, sectors per track = %d" % (totalSectors, cylinders, heads, sectorsPerTrack))
    return geometry_struct

def gen_vhd_footer_struct(disk_type, image_size, vhd_uuid, rbd_uuid, checksum):
    vhd_geometry_struct = gen_vhd_geometry_struct(image_size)
    vhd_geometry = pack(VHD_DISK_GEOMETRY_FORMAT, vhd_geometry_struct[0], vhd_geometry_struct[1], vhd_geometry_struct[2])
    reserved = ''
    for i in range(411):
        reserved = reserved + pack('!c', chr(0))
    vhd_footer_struct = ('conectix', 0x00000002, 0x00010000, 0x00000200, time.time(), 'tap', 0x00010003,
                         0x00000000, image_size, image_size, vhd_geometry, disk_type, checksum, vhd_uuid, 0,
                         rbd_uuid, reserved)
    return vhd_footer_struct

def gen_vhd_dynamic_disk_header_struct(table_offset, image_size, checksum, parent_uuid, parent_unicode_name):
    max_tab_entries = image_size / VHD_DEFAULT_BLOCK_SIZE
    reserved = ''
    for i in range(256):
        reserved = reserved + pack('!c', chr(0))
    parent_locator_entry_empty_struct = (0x00000000, 0, 0, 0, 0) #empty
    parent_locator_entry_empty = pack(VHD_PARENT_LOCATOR_ENTRY_FORMAT, *parent_locator_entry_empty_struct)
    dynamic_disk_header_struct = ('cxsparse', 0xffffffffffffffff, table_offset, 0x00010000, max_tab_entries,
                    VHD_DEFAULT_BLOCK_SIZE, checksum, parent_uuid, time.time(), 0x00000000, parent_unicode_name.encode("UTF-16BE"),
                    parent_locator_entry_empty, parent_locator_entry_empty, parent_locator_entry_empty,
                    parent_locator_entry_empty, parent_locator_entry_empty, parent_locator_entry_empty,
                    parent_locator_entry_empty, parent_locator_entry_empty,
                    reserved)
    return dynamic_disk_header_struct

def get_sector_bitmap_and_data(vhdfile, data_block_offset, block_size):
    sectors_in_block = block_size/SECTOR_SIZE
    bitmap_size = sectors_in_block/8
    if bitmap_size%512>0:
        bitmap_size=((bitmap_size//512)+1)*512
    _format_ = "!%is%is" % (bitmap_size,block_size)
    vhdfile.seek((data_block_offset)*512, 0)
    BUFFER=vhdfile.read(bitmap_size+block_size)
    bitmap_and_data=unpack(_format_,BUFFER)
    _format_ = "!"
    for i in range(sectors_in_block):
        _format_ = _format_ + "512s"
    data = unpack(_format_,bitmap_and_data[1])
    return [bitmap_and_data[0],data]

def get_raw_byte_offset_of_sector(block_number, sector_in_block, block_size, sector_size):
    return block_number*block_size+sector_in_block*sector_size

def get_raw_sector_offset_of_sector(block_number, sector_in_block, block_size, sector_size):
    sector_per_block = block_size / sector_size
    return block_number*sector_per_block+sector_in_block

#-------------------------------------------------------------------------------------------------------------------------------------------------------#
def rbd2vhd(rbd, vhd, rbd_image_uuid, verbose):
    
    VHD_FH = open(vhd, "wb")
    if rbd == "-":
        RBDDIFF_FH = sys.stdin
    else:
        RBDDIFF_FH = open(rbd, "rb")    
       
    rbd_header = RBDDIFF_FH.read(len(RBD_HEADER))
    
    rbd_meta_read_finished = 0
    vhd_headers_written = 0
    blocks_bitmaps = {}
    from_snap_name = ''
    to_snap_name = ''
    parent_exists = False
    allocated_block_count=0
    last_written_sector_in_block = 0
    
    while True:
        record_tag = RBDDIFF_FH.read(RBD_DIFF_META_RECORD_TAG_SIZE)
        #record_tag = unpack("%s%s" % (RBD_DIFF_META_ENDIAN_PREFIX, RBD_DIFF_META_RECORD_TAG), record)
        if not record_tag:
            INFO(verbose, "[rbd2vhd]: RBD: EOF")
            break
        else:
            INFO(verbose, "[rbd2vhd]: RBD Record TAG = \'%c\'" % record_tag)
            if record_tag == "e":
                INFO(verbose, "[rbd2vhd]: RBD: EOF")
                break
            if record_tag == "f":
                record = RBDDIFF_FH.read(RBD_DIFF_META_SNAP_SIZE)
                snap_name_length = int(unpack("%s%s" % (RBD_DIFF_META_ENDIAN_PREFIX, RBD_DIFF_META_SNAP), record)[0])
                record = RBDDIFF_FH.read(snap_name_length)
                from_snap_name = unpack("%s%ds" % (RBD_DIFF_META_ENDIAN_PREFIX, snap_name_length),record)[0]
                regex = re.compile(SNAPSHOT_PREFIX)
                from_snap_name = regex.sub('', from_snap_name)
                INFO(verbose, "[rbd2vhd]: RBD: From snap = %s" % from_snap_name)
            elif record_tag == "t":
                record = RBDDIFF_FH.read(RBD_DIFF_META_SNAP_SIZE)
                snap_name_length = int(unpack("%s%s" % (RBD_DIFF_META_ENDIAN_PREFIX, RBD_DIFF_META_SNAP), record)[0])
                record = RBDDIFF_FH.read(snap_name_length)
                to_snap_name = unpack("%s%ds" % (RBD_DIFF_META_ENDIAN_PREFIX, snap_name_length),record)[0]
                regex = re.compile(SNAPSHOT_PREFIX)
                to_snap_name = regex.sub('', to_snap_name)
                INFO(verbose, "[rbd2vhd]: RBD: To snap = %s" % to_snap_name)
            elif record_tag == "s":
                record = RBDDIFF_FH.read(RBD_DIFF_META_SIZE_SIZE)
                image_size = int(unpack("%s%s" % (RBD_DIFF_META_ENDIAN_PREFIX, RBD_DIFF_META_SIZE), record)[0])
                INFO(verbose, "[rbd2vhd]: RBD: Image size = %d" % image_size)
            elif record_tag == "w":
                record = RBDDIFF_FH.read(RBD_DIFF_DATA_SIZE)
                _record_ = unpack("%s%s" % (RBD_DIFF_META_ENDIAN_PREFIX, RBD_DIFF_DATA), record)
                offset = _record_[0]
                length = _record_[1]
                INFO(verbose, "[rbd2vhd]: RBD: Data offset = 0x%08x and length = %d" % (offset, length))
                if rbd_meta_read_finished == 0:
                    rbd_meta_read_finished = 1
            elif record_tag == "z":
                record = RBDDIFF_FH.read(RBD_DIFF_DATA_SIZE)
                _record_ = unpack("%s%s" % (RBD_DIFF_META_ENDIAN_PREFIX, RBD_DIFF_DATA), record)
                offset = _record_[0]
                length = _record_[1]
                INFO(verbose, "[rbd2vhd]: RBD: Zero data offset = 0x%08x and length = %d" % (offset, length))
                if rbd_meta_read_finished == 0:
                    rbd_meta_read_finished = 1
            else:
                ERROR(verbose, "[rbd2vhd]: RBD: Error while reading rbd_diff file")
                sys.exit(2)
                
            if (rbd_meta_read_finished == 1) & (vhd_headers_written == 0):
                if rbd_image_uuid == '':
                    ERROR(verbose, "[rbd2vhd]: RBD: Error: RBD image UUID is not specified")
                    sys.exit(1)
                if from_snap_name:
                        parent_uuid = uuid.UUID(from_snap_name)
                        parent_exists = True
                else:
                        parent_uuid = uuid.UUID('00000000-0000-0000-0000-000000000000')
                        parent_exists = False
                if to_snap_name:
                    vhd_uuid = uuid.UUID(to_snap_name)
                    rbd_uuid = uuid.UUID(rbd_image_uuid)
                else:
                    vhd_uuid = uuid.UUID(rbd_image_uuid)
                    rbd_uuid = uuid.UUID(rbd_image_uuid)
                
                if parent_exists:
                    vhd_footer_struct = gen_vhd_footer_struct(VHD_DIFF_HARDDISK_TYPE, image_size, vhd_uuid.bytes, rbd_uuid.bytes, 0)
                else:
                    vhd_footer_struct = gen_vhd_footer_struct(VHD_DYNAMIC_HARDDISK_TYPE, image_size, vhd_uuid.bytes, rbd_uuid.bytes, 0)
                VHD_FOOTER = pack(VHD_FOTTER_FORMAT, *vhd_footer_struct)
                vhd_footer_struct = modTupleByIndex(vhd_footer_struct, _vhd_footter_checksum_, checksum(VHD_FOOTER))
                VHD_FOOTER = pack(VHD_FOTTER_FORMAT, *vhd_footer_struct)
                
                if parent_exists:
                    vhd_dynamic_disk_header_struct = gen_vhd_dynamic_disk_header_struct(VHD_FOTTER_RECORD_SIZE+VHD_DYNAMIC_DISK_HEADER_RECORD_SIZE, image_size, 0, parent_uuid.bytes, "%s.vhd" % str(parent_uuid))
                else:
                    vhd_dynamic_disk_header_struct = gen_vhd_dynamic_disk_header_struct(VHD_FOTTER_RECORD_SIZE+VHD_DYNAMIC_DISK_HEADER_RECORD_SIZE, image_size, 0, '', '')
                VHD_DYNAMIC_DISK_HEADER = pack(VHD_DYNAMIC_DISK_HEADER_FORMAT, *vhd_dynamic_disk_header_struct)
                vhd_footer_struct = modTupleByIndex(vhd_dynamic_disk_header_struct, _dynamic_disk_header_checksum_, checksum(VHD_DYNAMIC_DISK_HEADER))
                VHD_DYNAMIC_DISK_HEADER = pack(VHD_DYNAMIC_DISK_HEADER_FORMAT, *vhd_dynamic_disk_header_struct)
                
                vhd_bat_list = gen_empty_vhd_bat(image_size)
                VHD_BAT = pack_vhd_bat(vhd_bat_list)
                
                vhd_file_offset = 0
                
                VHD_FH.write(VHD_FOOTER)
                vhd_file_offset += VHD_FOTTER_RECORD_SIZE
                
                VHD_FH.write(VHD_DYNAMIC_DISK_HEADER)
                vhd_file_offset += VHD_DYNAMIC_DISK_HEADER_RECORD_SIZE
                
                VHD_FH.write(VHD_BAT)
                vhd_file_offset += len(VHD_BAT)
                
                VHD_BATMAP = gen_empty_batmap()
                vhd_batmap_header_struct = gen_batmap_header(VHD_BATMAP)
                vhd_batmap_header_struct = modTupleByIndex(vhd_batmap_header_struct, _batmap_offset_, vhd_file_offset+VHD_BATMAP_HEADER_SIZE)
                vhd_batmap_header_struct = modTupleByIndex(vhd_batmap_header_struct, _batmap_size_, len(VHD_BATMAP)/SECTOR_SIZE)
                vhd_batmap_header_struct = modTupleByIndex(vhd_batmap_header_struct, _batmap_checksum_, checksum(VHD_BATMAP))
                VHD_BATMAP_HEADER = pack(VHD_BATMAP_HEADER_FORMAT, *vhd_batmap_header_struct)
                
                VHD_FH.write(VHD_BATMAP_HEADER)
                vhd_file_offset += VHD_BATMAP_HEADER_SIZE
                
                VHD_FH.write(VHD_BATMAP)
                vhd_file_offset += len(VHD_BATMAP)
                
                for index in range(VHD_PARENT_LOCATORS_COUNT):
                    if parent_exists:
                        if index == 0:
                            parent_locator = "file://./%s.vhd" % parent_uuid
                            parent_locator_encoded = parent_locator
                            parent_locator_entry_struct = (_platform_code_MacX_, get_size_aligned_to_sector_boundary(len(parent_locator_encoded)), len(parent_locator_encoded), 0, vhd_file_offset) #MacX
                        elif index == 1:
                            parent_locator = ".\%s.vhd" % parent_uuid
                            parent_locator_encoded = parent_locator.encode("UTF-16BE")
                            parent_locator_entry_struct = (_platform_code_W2ku_, get_size_aligned_to_sector_boundary(len(parent_locator_encoded)), len(parent_locator_encoded), 0, vhd_file_offset) #W2ku
                        elif index == 2:
                            parent_locator = ".\%s.vhd" % parent_uuid
                            parent_locator_encoded = parent_locator.encode("UTF-16BE")
                            parent_locator_entry_struct = (_platform_code_W2ru_, get_size_aligned_to_sector_boundary(len(parent_locator_encoded)), len(parent_locator_encoded), 0, vhd_file_offset) #W2ru
                        else:
                            parent_locator_encoded = ''
                            parent_locator_entry_struct = (0x00000000, 0, 0, 0, 0) #empty
                    else:
                        parent_locator_encoded = ''
                        parent_locator_entry_struct = (0x00000000, 0, 0, 0, 0) #empty
                    
                    if len(parent_locator_encoded) > 0:
                        parent_locator_packed = pack("!%ds" % get_size_aligned_to_sector_boundary(len(parent_locator_encoded)), parent_locator_encoded)
                    else:
                        parent_locator_packed = pack("!%ds" % SECTOR_SIZE, parent_locator_encoded)
                        
                    parent_locator_entry = pack(VHD_PARENT_LOCATOR_ENTRY_FORMAT, *parent_locator_entry_struct)
                    vhd_dynamic_disk_header_struct = modTupleByIndex(vhd_dynamic_disk_header_struct, _dynamic_disk_header_parent_locator_entry_1_ + index, parent_locator_entry)
                    
                    VHD_FH.write(parent_locator_packed)
                    vhd_file_offset += len(parent_locator_packed)
                    
                #print_vhd_footer(vhd_footer_struct)
                #print_dynamic_disk_header(dynamic_disk_header_struct)
                
                vhd_headers_written = 1
                block_bitmap_size = get_bitmap_size(vhd_dynamic_disk_header_struct)
                data_offset = vhd_file_offset
                
                INFO(verbose, "[rbd2vhd]: Begining of data offset 0x%08x" % data_offset)
                INFO(verbose, "[rbd2vhd]: Begining of data real offset 0x%08x" % VHD_FH.tell())
                
            if (rbd_meta_read_finished == 1) & (vhd_headers_written == 1):
                _offset_ = offset
                while length > 0:
                    SectorsPerBlock = VHD_DEFAULT_BLOCK_SIZE / SECTOR_SIZE
                    RawSectorNumber = _offset_ / SECTOR_SIZE
                    BlockNumber = RawSectorNumber // (VHD_DEFAULT_BLOCK_SIZE//SECTOR_SIZE)
                    SectorInBlock = RawSectorNumber % SectorsPerBlock
                    
                    if vhd_bat_list[BlockNumber] == 0xffffffff:
                        if last_written_sector_in_block != 0:
                            INFO(verbose, "[rbd2vhd]: Write %d zero sectors to the end of block" % (SectorsPerBlock - SectorInBlock - read_sectors))
                            _buffer_ = pack("!%ds" % ((SectorsPerBlock - last_written_sector_in_block)*SECTOR_SIZE), '')
                            VHD_FH.write(_buffer_)
                            vhd_file_offset += (SectorsPerBlock - SectorInBlock - read_sectors)*SECTOR_SIZE
                        INFO(verbose, "[rbd2vhd]: New block %d allocated" % BlockNumber)
                        block_offset_in_bytes = data_offset+allocated_block_count*VHD_DEFAULT_BLOCK_SIZE + block_bitmap_size*allocated_block_count
                        block_offset_in_sectors = block_offset_in_bytes / SECTOR_SIZE
                        vhd_bat_list[BlockNumber] = block_offset_in_sectors
                        INFO(verbose, "[rbd2vhd]: New block offset in bytes 0x%08x" % block_offset_in_bytes)
                        INFO(verbose, "[rbd2vhd]: New block offset in sectors %d" % block_offset_in_sectors)
                        allocated_block_count = allocated_block_count + 1
                        INFO(verbose, "[rbd2vhd]: Write %d bytes of empty sectors bitmap" % len(gen_bitmap_from_bitarray(gen_empty_bitarray_for_bitmap(block_bitmap_size))))
                        VHD_FH.write(gen_bitmap_from_bitarray(gen_empty_bitarray_for_bitmap(block_bitmap_size)))
                        INFO(verbose, "[rbd2vhd]: Skeep %d bytes (%d sectors)" % (SectorInBlock*SECTOR_SIZE, SectorInBlock))
                        VHD_FH.seek(SectorInBlock*SECTOR_SIZE,1)
                        vhd_file_offset += block_bitmap_size + SectorInBlock*SECTOR_SIZE
                        last_written_sector_in_block = 0
                    
                    if length < (SectorsPerBlock-SectorInBlock)*SECTOR_SIZE:
                        if record_tag == "w":
                            _buffer_ = RBDDIFF_FH.read(length)
                        elif record_tag == "z":
                            _buffer_ = pack("!$ds" % length, '')
                        read_length = length
                        length = 0
                    else:
                        if record_tag == "w":
                            _buffer_ = RBDDIFF_FH.read((SectorsPerBlock-SectorInBlock)*SECTOR_SIZE)
                        elif record_tag == "z":
                            _buffer_ = pack("!$ds" % (SectorsPerBlock-SectorInBlock)*SECTOR_SIZE, '')
                        read_length = (SectorsPerBlock-SectorInBlock)*SECTOR_SIZE
                        length = length - read_length
                    
                    read_sectors = read_length/SECTOR_SIZE
                    
                    for sector_index in range(read_length/SECTOR_SIZE):
                        if blocks_bitmaps.has_key(BlockNumber):
                            if blocks_bitmaps[BlockNumber][SectorInBlock+sector_index] == 0:
                                del blocks_bitmaps[BlockNumber][SectorInBlock+sector_index]
                                blocks_bitmaps[BlockNumber].insert(SectorInBlock+sector_index, 1)
                        else:
                            blocks_bitmaps[BlockNumber] = gen_empty_bitarray_for_bitmap(block_bitmap_size)
                            del blocks_bitmaps[BlockNumber][SectorInBlock+sector_index]
                            blocks_bitmaps[BlockNumber].insert(SectorInBlock+sector_index, 1)
                    
                    INFO(verbose, "[rbd2vhd]: SectorsPerBlock %d, SectorInBlock %d, read_sectors %d" % (SectorsPerBlock, SectorInBlock, read_sectors))
                    
                    if last_written_sector_in_block != 0:
                        sectors_to_skeep = SectorInBlock - last_written_sector_in_block
                        INFO(verbose, "[rbd2vhd]: Skeep %d sectors to the next sector with data" % sectors_to_skeep)
                        VHD_FH.seek(sectors_to_skeep*SECTOR_SIZE,1)
                        vhd_file_offset += sectors_to_skeep*SECTOR_SIZE
                    
                    INFO(verbose, "[rbd2vhd]: Write %d bytes of data" % len(_buffer_))
                    VHD_FH.write(_buffer_)
                    vhd_file_offset += read_length
                    _offset_ += read_length
                    
                    last_written_sector_in_block = SectorInBlock + read_sectors
    
    if last_written_sector_in_block != 0:
        INFO(verbose, "[rbd2vhd]: Write %d zero sectors to the end of block" % (SectorsPerBlock - SectorInBlock - read_sectors))
        _buffer_ = pack("!%ds" % ((SectorsPerBlock - last_written_sector_in_block)*SECTOR_SIZE), '')
        VHD_FH.write(_buffer_)
        vhd_file_offset += (SectorsPerBlock - SectorInBlock - read_sectors)*SECTOR_SIZE
    
    VHD_FH.write(VHD_FOOTER)
    VHD_FH.seek(VHD_FOTTER_RECORD_SIZE,0)
    VHD_DYNAMIC_DISK_HEADER = pack(VHD_DYNAMIC_DISK_HEADER_FORMAT, *vhd_dynamic_disk_header_struct)
    VHD_FH.write(VHD_DYNAMIC_DISK_HEADER)
    vhd_file_offset = VHD_FOTTER_RECORD_SIZE + VHD_DYNAMIC_DISK_HEADER_RECORD_SIZE
    VHD_BAT = pack_vhd_bat(vhd_bat_list)
    VHD_FH.write(VHD_BAT)
    vhd_file_offset += len(VHD_BAT)
    INFO(verbose, "[rbd2vhd]: Rewrite BAT (write %d entries, %d bytes)" % (vhd_dynamic_disk_header_struct[_dynamic_disk_header_max_table_entries_], vhd_dynamic_disk_header_struct[_dynamic_disk_header_max_table_entries_]*4))
    
    INFO(verbose, "[rbd2vhd]: Current offset in VHD file is 0x%08x" % vhd_file_offset)
    
    for BlockNumber in range(len(vhd_bat_list)):
        if vhd_bat_list[BlockNumber] != 0xffffffff:
            INFO(verbose, "[rbd2vhd]: Block %d offset is 0x%08x, skeep 0x%08x bytes from last offest 0x%08x" % (BlockNumber, vhd_bat_list[BlockNumber]*SECTOR_SIZE, (vhd_bat_list[BlockNumber]*SECTOR_SIZE-vhd_file_offset), vhd_file_offset))
            VHD_FH.seek((vhd_bat_list[BlockNumber]*SECTOR_SIZE-vhd_file_offset),1)
            vhd_file_offset = vhd_bat_list[BlockNumber]*SECTOR_SIZE
            INFO(verbose, "[rbd2vhd]: Rewrite block %d sector bitmap" % BlockNumber)
            VHD_FH.write(gen_bitmap_from_bitarray(blocks_bitmaps[BlockNumber]))
            vhd_file_offset += block_bitmap_size
    
    VHD_FH.close
    if RBDDIFF_FH is not sys.stdin:
        RBDDIFF_FH.close
    
    return 0
#-------------------------------------------------------------------------------------------------------------------------------------------------------#
def vhd2rbd(vhd, rbd, verbose):
    
    VHD_FH = open(vhd, "rb")
    if rbd == "-":
        RBDDIFF_FH = sys.stdout
    else:
        RBDDIFF_FH = open(rbd, "wb")
    
    VHD_FOOTER = unpack(VHD_FOTTER_FORMAT, VHD_FH.read(VHD_FOTTER_RECORD_SIZE))
    DYNAMIC_DISK_HEADER = unpack(VHD_DYNAMIC_DISK_HEADER_FORMAT, VHD_FH.read(VHD_DYNAMIC_DISK_HEADER_RECORD_SIZE))
    BAT_TABLE = unpack("!%iI" % DYNAMIC_DISK_HEADER[_dynamic_disk_header_max_table_entries_],
                       VHD_FH.read(DYNAMIC_DISK_HEADER[_dynamic_disk_header_max_table_entries_]*4))
    BATMAP_HEADER = unpack(VHD_BATMAP_HEADER_FORMAT, VHD_FH.read(VHD_BATMAP_HEADER_SIZE))
    
    # Write RBD diff header
    INFO(verbose, "[vhd2rbd]: Writing RBD diff header ...")
    RBDDIFF_FH.write(RBD_HEADER)
    
    # Write RBD from_snap record
    if DYNAMIC_DISK_HEADER[_dynamic_disk_header_parent_unique_id_] != '\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00\x00':
        from_snap_uuid = uuid.UUID(bytes=DYNAMIC_DISK_HEADER[_dynamic_disk_header_parent_unique_id_])
        from_snap = "%s%s" % (SNAPSHOT_PREFIX, str(from_snap_uuid))
        RBDDIFF_FH.write(pack("%s%s%s%ds" % (RBD_DIFF_META_ENDIAN_PREFIX, RBD_DIFF_META_RECORD_TAG, RBD_DIFF_META_SNAP, len(from_snap)), 'f', len(from_snap), from_snap))
    
    # Write RBD to_snap record
    if VHD_FOOTER[_vhd_footter_unique_iq_] != VHD_FOOTER[_vhd_footter_rbd_image_uuid_]:
        to_snap_uuid = uuid.UUID(bytes=VHD_FOOTER[_vhd_footter_unique_iq_])
        to_snap = "%s%s" % (SNAPSHOT_PREFIX, str(to_snap_uuid))
        RBDDIFF_FH.write(pack("%s%s%s%ds" % (RBD_DIFF_META_ENDIAN_PREFIX, RBD_DIFF_META_RECORD_TAG, RBD_DIFF_META_SNAP, len(to_snap)), 't', len(to_snap), to_snap))
    
    # Write RBD Size record
    RBDDIFF_FH.write(pack(RBD_DIFF_META_ENDIAN_PREFIX+RBD_DIFF_META_RECORD_TAG+RBD_DIFF_META_SIZE, 's', VHD_FOOTER[_vhd_footter_current_size_]))
    
    total_changed_sectors = 0
    total_changed_sectors_ = 0
    raw_first_sector = -1
    raw_last_sector = -1
    in_block_first_sector = -1
    in_block_last_sector = -1
    
    for block_index in range(DYNAMIC_DISK_HEADER[_dynamic_disk_header_max_table_entries_]):
        if BAT_TABLE[block_index] != 0xffffffff:
            DATA_BLOCK = get_sector_bitmap_and_data(VHD_FH, BAT_TABLE[block_index], DYNAMIC_DISK_HEADER[_dynamic_disk_header_block_size_])
            
            BITMAP_SIZE = get_bitmap_size(DYNAMIC_DISK_HEADER)
            INFO(verbose, "[vhd2rbd]: Read VHD block %d" % block_index)
            
            BITMAPARRAY = get_bitarray_from_bitmap(DATA_BLOCK[0], BITMAP_SIZE)
            
            for sector_in_block_index in range(BITMAP_SIZE*8):
                if BITMAPARRAY[sector_in_block_index] == 1:
                    INFO(verbose, "[vhd2rbd]: Read sector %d with data in block %d" % (sector_in_block_index, block_index))
                    total_changed_sectors_ += 1
                    if raw_first_sector == -1:
                        raw_first_sector = get_raw_sector_offset_of_sector(block_index, sector_in_block_index, DYNAMIC_DISK_HEADER[_dynamic_disk_header_block_size_], SECTOR_SIZE)
                        in_block_first_sector = sector_in_block_index
                        raw_last_sector = -1
                else:
                    if (raw_last_sector == -1) & (raw_first_sector != -1) :
                        raw_last_sector = get_raw_sector_offset_of_sector(block_index, sector_in_block_index-1, DYNAMIC_DISK_HEADER[_dynamic_disk_header_block_size_], SECTOR_SIZE)
                        in_block_last_sector = sector_in_block_index-1
                        INFO(verbose, "[vhd2rbd]: Data sectors range (raw) %d - %d" % (raw_first_sector, raw_last_sector))
                        INFO(verbose, "[vhd2rbd]: Data sectors range (in block) %d - %d" % (in_block_first_sector, in_block_last_sector))
                        # Write RBD data record header
                        INFO(verbose, "[vhd2rbd]: Write RBD data record header ...")
                        RBDDIFF_FH.write(pack(RBD_DIFF_META_ENDIAN_PREFIX+RBD_DIFF_META_RECORD_TAG+RBD_DIFF_DATA, 'w', raw_first_sector*512,(raw_last_sector-raw_first_sector+1)*512))
                        INFO(verbose, "[vhd2rbd]: Write RBD data record data ...")
                        for sector_index in range(in_block_first_sector, in_block_last_sector+1):
                            INFO(verbose, "[vhd2rbd]: Write sector %d from block %d" % (sector_index, block_index))
                            total_changed_sectors += 1
                            # Write RBD data record data
                            RBDDIFF_FH.write(pack(RBD_DIFF_META_ENDIAN_PREFIX+"512s",DATA_BLOCK[1][sector_index]))
                        raw_first_sector = -1
                        raw_last_sector = -1
                        in_block_first_sector = -1
                        in_block_last_sector = -1
            
            if (sector_in_block_index == BITMAP_SIZE*8-1) and (raw_first_sector != -1) :
                raw_last_sector = get_raw_sector_offset_of_sector(block_index, sector_in_block_index, DYNAMIC_DISK_HEADER[_dynamic_disk_header_block_size_], SECTOR_SIZE)
                in_block_last_sector = sector_in_block_index
                INFO(verbose, "[vhd2rbd]: Data sectors range (raw) %d - %d" % (raw_first_sector, raw_last_sector))
                INFO(verbose, "[vhd2rbd]: Data sectors range (in block) %d - %d" % (in_block_first_sector, in_block_last_sector))
                # Write RBD data record header
                RBDDIFF_FH.write(pack(RBD_DIFF_META_ENDIAN_PREFIX+RBD_DIFF_META_RECORD_TAG+RBD_DIFF_DATA, 'w', raw_first_sector*512,(raw_last_sector-raw_first_sector+1)*512))
                INFO(verbose, "[vhd2rbd]: Write RBD data record header ...")
                for sector_index in range(in_block_first_sector, in_block_last_sector+1):
                    INFO(verbose, "[vhd2rbd]: Write sector %d from block %d" % (sector_index, block_index))
                    # Write RBD data record data
                    total_changed_sectors += 1
                    RBDDIFF_FH.write(pack(RBD_DIFF_META_ENDIAN_PREFIX+"512s",DATA_BLOCK[1][sector_index]))
                raw_first_sector = -1
                raw_last_sector = -1
                in_block_first_sector = -1
                in_block_last_sector = -1
    
    INFO(verbose, "[vhd2rbd]: Total wrintten sectors : %d" % total_changed_sectors)
    INFO(verbose, "[vhd2rbd]: Total written bytes: %d" % (total_changed_sectors*512))
    
    RBDDIFF_FH.write('e')
    
    VHD_FH.close
    if RBDDIFF_FH is not sys.stdout:
        RBDDIFF_FH.close
    
    return 0
#-------------------------------------------------------------------------------------------------------------------------------------------------------#
def main(argv):
    vhd_file = ''
    rbd_file = ''
    vhd_uuid = ''
    verbose = False
    
    cmdname = sys.argv[0]
    regex = re.compile('.*/*.*/')
    cmdname = regex.sub('', cmdname)
    regex = re.compile('\.\w+')
    cmdname = regex.sub('', cmdname)
    
    if len(sys.argv) > 1:
        try:
            opts, args = getopt.getopt(argv,"hv",["vhd=","rbd=","uuid="])
        except getopt.GetoptError:
            eprint('Usage:')
            eprint('\tvhd2rbd --vhd <vhd_file> --rbd <rbd_file> [-v]')
            eprint('\trbd2vhd --rbd <rbd_file> --vhd <vhd_file> [--uuid <vhd_uuid>] [-v]')
            sys.exit(2)
        for opt, arg in opts:
            if opt == '-h':
                eprint('Usage:')
                eprint('\tvhd2rbd --vhd <vhd_file> --rbd <rbd_file> [-v]')
                eprint('\trbd2vhd --rbd <rbd_file> --vhd <vhd_file> [--uuid <vhd_uuid>] [-v]')
                sys.exit()
            if opt == '-v':
                verbose = True
            elif opt == '--vhd':
                vhd_file = arg
            elif opt == '--rbd':
                rbd_file = arg
            elif opt == '--uuid':
                vhd_uuid = arg
        INFO(verbose, "[main]: VHD file is \'%s\'" % vhd_file)
        INFO(verbose, "[main]: RBD file is \'%s\'" % rbd_file)
        
        if (cmdname == 'vhd2rbd'):
            vhd2rbd(vhd_file, rbd_file, verbose)
        elif(cmdname == 'rbd2vhd'):
            rbd2vhd(rbd_file, vhd_file, vhd_uuid, verbose)
            pass
    else:
            eprint('Usage:')
            eprint('\tvhd2rbd --vhd <vhd_file> --rbd <rbd_file> [-v]')
            eprint('\trbd2vhd --rbd <rbd_file> --vhd <vhd_file> [--uuid <vdi_uuid>] [-v]')

if __name__ == "__main__":
   main(sys.argv[1:])