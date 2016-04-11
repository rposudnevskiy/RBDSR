#!/bin/sh
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

DM_BASE_NAME=$1
retval=true
while [ "$retval" = "true" ]
do
    retval=`dmsetup status $DM_BASE_NAME | sed "s/\// /"g | awk '{if ($4==$6) print "false"; else print "true"}'`
done