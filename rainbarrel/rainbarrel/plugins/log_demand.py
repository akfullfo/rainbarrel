#!/usr/bin/env python
# ________________________________________________________________________
#
#  Copyright (C) 2015 Andrew Fullford
#
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
# ________________________________________________________________________
#
 
import os

from ..plugin import *

def record_data(barrel, tag, ts, value):
	if not barrel.datadir:
		return
	iso_ts = barrel.iso8601(ts)
	barrel.log.debug("%s: %.3f", tag, value)
	fname = os.path.join(barrel.datadir, barrel.iso8601(ts, terse=False)[:10] + '.' + tag)

	with open(fname, 'a') as f:
		f.write("%s\t%.3f\n" % (iso_ts, value))

class demand(Plugin):
	def handle(self, barrel, **params):
		last_updated_name = barrel.state.get('_last_element')
		if not last_updated_name:
			raise PluginError("No _last_element element in state")
		if last_updated_name == 'InstantaneousDemand':
			info = barrel.state.get(last_updated_name)
			if not info:
				raise PluginError("%s entry missing from state", last_updated_name)
			record_data(barrel, 'demand', info['_timestamp'], info['_readings']['Demand'])
		else:
			barrel.log.debug("demand handler ignoring '%s'", last_updated_name)

class delivered(Plugin):
	def handle(self, barrel, **params):
		last_updated_name = barrel.state.get('_last_element')
		if not last_updated_name:
			raise PluginError("No _last_element element in state")
		if last_updated_name == 'CurrentSummationDelivered':
			info = barrel.state.get(last_updated_name)
			if not info:
				raise PluginError("%s entry missing from state", last_updated_name)
			record_data(barrel, 'delivered', info['_timestamp'], info['_readings']['SummationDelivered'])
		else:
			barrel.log.debug("summation handler ignoring '%s'", last_updated_name)

class received(Plugin):
	def handle(self, barrel, **params):
		last_updated_name = barrel.state.get('_last_element')
		if not last_updated_name:
			raise PluginError("No _last_element element in state")
		if last_updated_name == 'CurrentSummationDelivered':
			info = barrel.state.get(last_updated_name)
			if not info:
				raise PluginError("%s entry missing from state", last_updated_name)
			record_data(barrel, 'received', info['_timestamp'], info['_readings']['SummationReceived'])
		else:
			barrel.log.debug("summation handler ignoring '%s'", last_updated_name)
