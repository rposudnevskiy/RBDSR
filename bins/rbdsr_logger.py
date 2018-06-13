#!/usr/bin/python -u
#
# Copyright (C) Amin Dandache (amin.dandache@vico-research.com)
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
# Friedrich-List-Str. 46, 70771 Leinfelden-Echterdingen, DE

from __future__ import print_function
import syslog
import util


class LogRbd:
    @classmethod
    def debug(cls, msg):
        try:
            util.SMlog(msg)

            # additional syslog logging
            syslog.openlog('rbdsr', logoption=syslog.LOG_PID, facility=syslog.LOG_DAEMON)
            syslog.syslog(msg)
        except Exception as e:
            syslog.openlog('rbdsr', logoption=syslog.LOG_PID, facility=syslog.LOG_DAEMON)
            syslog.syslog('rbdsr_logger: LogRbd.debug: FATAL ERROR occured in logging msg: %s' % str(e))
