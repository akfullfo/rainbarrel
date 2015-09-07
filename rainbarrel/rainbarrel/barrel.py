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
 
import os, time, re, socket, struct, json, logging, netaddr
import xml.dom.minidom
from datetime import datetime
from dateutil import tz
from taskforce import poll, httpd

class Barrel(object):

	#  File in datadir used to record current state
	#
	statefile_name = 'rainbarrel.state'

	#  Match valid hex numbers
	#
	is_hex = re.compile(r'^0x[0-9a-fA-F]+$')

	#  This is the number of seconds between Jan 1, 1970 UTC (unix epoch) and
	#  Jan 1, 2000 UTC (rainforest epoch).
	#
	#  Except the meter seems to actually be on local time, so subtract 6hrs
	#
	time_t_offset_2000 = 946706400 - 21600

	#  Configuration information for each message type.
	#
	properties = {
		'InstantaneousDemand': {
			'event_name': 'demand',
			'desired_period': 30
		},
		'DeviceInfo': {
		},
		'NetworkInfo': {
		},
		'CurrentSummationDelivered': {
			'event_name': 'summation',
			'desired_period': 300
		},
		'PriceCluster': {
			#'event_name': 'price',
			#'desired_period': 60
		},
		'TimeCluster': {
		},
		'BlockPriceDetail': {
		}
	}

	#  Don't trigger a period update if the delta is less than this
	period_reset_tolerance = 5

	#  Response tuple when no action is needed
	#
	noop_response = (200, '', 'text/plain')

	set_schedule_template = """
<RavenCommand>
	<Name>set_schedule</Name>
	<DeviceMacId>%s</DeviceMacId>
	<Event>%s</Event>
	<Frequency>0x%04x</Frequency>
	<Enabled>Y</Enabled>
</RavenCommand>
"""
	def __init__(self, config, **params):
		self.log = params.get('log')
		if not self.log:
			self.log = logging.getLogger(__name__)
			self.log.addHandler(logging.NullHandler())
		self.config = config
		self.datadir = os.path.expandvars(os.path.expanduser(self.config.get('datadir')))
		if self.datadir:
			if not os.access(self.datadir, os.W_OK | os.X_OK):
				#  Throw this early because we don't actually attempt
				#  access until a POST arrives.
				#
				raise Exception("Data directory '%s' is not writable" % (self.datadir))
			self.statefile = os.path.join(self.datadir, self.statefile_name)
			self.state = self.load_state()
			self.recording = True
		else:
			self.statfile = None
			self.state = {}
			self.recording = False

		#  This is set by fill().
		self.is_inet = None

		self.authorized_addrs = None
		if self.config.get('authaddrs') is not None:
			self.authorized_addrs = []
			for addr in self.config['authaddrs']:
				self.authorized_addrs.append(netaddr.IPNetwork(addr))

	def iso8601(self, tim = time.time(), **params):
		local = params.get('local', self.config.get('timestamp_local', True))
		resolution = params.get('resolution', self.config.get('timestamp_resolution', 'msec'))
		terse = params.get('terse', self.config.get('timestamp_terse', False))
		if local:
			dt = datetime.fromtimestamp(tim, tz.tzlocal())
		else:
			#  Set the utc timezone so we get tz-aware rather than naive
			dt = datetime.fromtimestamp(tim, tz.tzutc())
		if terse:
			t = dt.strftime("%Y%m%dT%H%M%S")
		else:
			t = dt.strftime("%Y-%m-%dT%H:%M:%S")
		if resolution.lower().startswith('ms'):
			t += '.%03d' % (int((dt.microsecond + 0.5) / 1000), )
		elif resolution.lower().startswith('us'):
			t += '.%06d' % (dt.microsecond, )
		tzs = dt.strftime("%z")
		if terse:
			if tzs == '+0000':
				tzs = 'Z'
		else:
			tzs = tzs[:3] + ':' + tzs[3:]
		return t + tzs

	def load_state(self):
		if not self.statefile:
			self.log.info("State file processing disabled")
			return {}
		try:
			with open(self.statefile, 'r') as f:
				prev_state = f.read()
		except Exception as e:
			self.log.warning("No previous state file '%s' -- %s", self.statefile, str(e))
			return {}
		try:
			return json.loads(prev_state)
		except Exception as e:
			self.log.warning("Parse failed on previous state file '%s' -- %s", self.statefile, str(e))
			return {}

	def save_state(self):
		if not self.statefile:
			self.log.debug("State file processing disabled")
			return
		temp = self.statefile + '.tmp'
		try:
			with open(temp, 'w') as f:
				f.write(json.dumps(self.state, indent=4))
		except Exception as e:
			self.log.error("Write failed to '%s' -- %s", temp, str(e))
			return
		try:
			os.rename(temp, self.statefile)
		except Exception as e:
			self.log.error("Rename of '%s' to '%s' failed -- %s", temp, self.statefile, str(e))

	def log_command(self, info, level=logging.DEBUG):
		self.log.log(level, "%s ...", info['_name'])
		for line in json.dumps(info, indent=4).splitlines():
			self.log.log(level, "    %s", line)

	def check_schedule(self, info):
		name = info['_name']
		props = self.properties.get(name)
		dev_mac = info['_raw']['DeviceMacId']
		if not props:
			self.log.debug("No properties for '%s', skipping schedule check", name)
			return self.noop_response
		desired_period = props.get('desired_period')
		if not desired_period:
			self.log.warning("No desired period for '%s', skipping schedule check", name)
			return self.noop_response
		event_name = props.get('event_name')
		if not event_name:
			self.log.warning("No event name for '%s', skipping schedule check", name)
			return self.noop_response
		prev_info = self.state.get(name)
		curr_ts = info['_timestamp']
		if prev_info and '_timestamp' in prev_info and prev_info['_timestamp']:
			prev_ts = prev_info['_timestamp']
		else:
			self.log.info("No previous timestamp for '%s', skipping schedule check", name)
			return self.noop_response
		delta = curr_ts - prev_ts
		if abs(delta - desired_period) < self.period_reset_tolerance:
			self.log.debug("Report delta %.1fs within tolerance for '%s', skipping schedule change", delta, event_name)
			return self.noop_response
		self.log.info("Delta of %.1fs for %s differs from desired %.0f, changing schedule",
							delta, event_name, desired_period)
		content = self.set_schedule_template % (dev_mac, event_name, desired_period)
		for line in content.splitlines():
			self.log.debug("%s", line.rstrip())
		return (200, content, 'text/xml')

	def read_meter(self, item, info):
		name = info['_name']
		sys_timestamp = info['_timestamp']

		timestamp = info.get('TimeStamp')
		value = info.get(item)
		multiplier = info.get('Multiplier')
		divisor = info.get('Divisor')
		if value is None:
			raise Exception("Missing '%s' in %s" % (item, name,))
		if multiplier is None:
			raise Exception("Missing 'Multiplier' in %s" % (name,))
		if divisor is None:
			raise Exception("Missing 'Divisor' in %s" % (name,))
		result = float(value) * multiplier / divisor
		try:
			clock_delta = ', clock delta %.3fs' % (sys_timestamp - timestamp - self.time_t_offset_2000, )
		except Exception as e:
			self.log.warning("Could not convert TimeStamp %s to clock delta -- %s", repr(timestamp), str(e))
			clock_delta = ''
		self.log.debug("Result is %.3f kW%s", result, clock_delta)

		return result

	def record_data(self, tag, ts, value):
		if not self.datadir:
			return
		iso_ts = self.iso8601(ts)
		self.log.debug("%s: %.3f", tag, value)
		fname = os.path.join(self.datadir, self.iso8601(ts, terse=False)[:10] + '.' + tag)

		with open(fname, 'a') as f:
			f.write("%s\t%.3f\n" % (iso_ts, value))

	def process(self, path, postmap, **params):
		if self.is_inet and self.authorized_addrs is not None and 'handler' not in params:
			self.log.error("Missing handler - cannot validate client address")
			return None
		if 'data' not in params:
			self.log.error("Missing payload from device")
			return None
		if self.is_inet and self.authorized_addrs is not None:
			client_ip = netaddr.IPAddress(params['handler'].client_address[0])
			self.log.info("Checking client address %s", client_ip)
			authorized = False
			for net in self.authorized_addrs:
				if client_ip in net:
					authorized = True
					break
			if not authorized:
				self.log.warning("Unauthorized access from client at %s", client_ip)
				return None
		payload = params['data']
		try:
			dom = xml.dom.minidom.parseString(payload)
		except Exception as e:
			self.log.error("Payload parse failed -- %s", str(e), exc_info=True)
			self.log.error("Failed payload was ...")
			if len(payload) > 1000:
				payload = payload[:1000] + ' ...'
			for line in str(payload).splitlines():
				self.log.error("  ] %s", line)
			return
		self.log.debug("Payload parsed successfully ...")
		for line in dom.toprettyxml(indent="  ").splitlines():
			line = line.rstrip()
			if line:
				self.log.debug("  ] %s", line)

		resp = (200, '', 'text/plain')

		for node in dom.getElementsByTagName('rainforest'):
			now = time.time()
			header = {}
			header['_version'] = node.getAttribute('version')
			header['_mac'] = node.getAttribute('macId')
			rainforest_timestamp = node.getAttribute('timestamp')
			if rainforest_timestamp:
				try:
					header['_timestamp'] = int(rainforest_timestamp.strip('s'))
					self.log.debug("Rainforest %s v%s timestamp delta %.3f",
							header['_mac'], header['_version'], now - header['_timestamp'])
				except Exception as e:
					self.log.warning("Could not load Rainforest timestamp '%s' -- %s",
												rainforest_timestamp, str(e))
					header['_timestamp'] = None
			else:
				self.log.warning("No timestamp in rainforest POST xml")

			if node.nodeType == node.ELEMENT_NODE and node.hasChildNodes():
				for command in node.childNodes:
					if command.nodeType != command.ELEMENT_NODE:
						continue
					if hasattr(self, command.nodeName) and callable(getattr(self, command.nodeName)):
						self.log.debug("%s has command element '%s'", node.nodeName, command.nodeName)
						info = {}
						raw_info = {}
						for elem in command.childNodes:
							if elem.nodeType != elem.ELEMENT_NODE:
								continue
							tag = elem.nodeName
							val = elem.childNodes[0].nodeValue.strip()
							valstr = val
							if self.is_hex.match(val):
								try:
									val = int(val, 0)
									if val > 0x7FFFFFFF:
										val -= 0x100000000
								except:
									pass
							self.log.debug("%s %s = %s", command.nodeName, elem.nodeName, repr(val))
							info[tag] = val
							raw_info[tag] = valstr
						info['_name'] = command.nodeName
						info['_raw'] = raw_info
						info['_timestamp'] = now

						#  Run the per-message processing
						#
						getattr(self, command.nodeName)(info)

						resp = self.check_schedule(info)

						self.state[command.nodeName] = info

					else:
						self.log.info("%s has unknown element '%s'", node.nodeName, command.nodeName)
			self.state['_timestamp'] = header['_timestamp']
			for tag in ['_mac', '_version']:
				if header[tag] != self.state.get(tag):
					self.log.info("Rainforest %s value changed from %s to %s",
										tag, repr(self.state.get(tag)), header[tag])
					self.state[tag] = header[tag]
			self.save_state()
			return resp

	def InstantaneousDemand(self, info):
		self.log_command(info)
		self.record_data('demand', info['_timestamp'], self.read_meter('Demand', info))

	def DeviceInfo(self, info):
		self.log_command(info)

	def NetworkInfo(self, info):
		self.log_command(info)

	def PriceCluster(self, info):
		self.log_command(info)

	def CurrentSummationDelivered(self, info):
		self.log_command(info)
		self.record_data('summation', info['_timestamp'], self.read_meter('SummationDelivered', info))

	def TimeCluster(self, info):
		self.log_command(info)

	def BlockPriceDetail(self, info):
		self.log_command(info, level=logging.INFO)

	def fill(self):
		"""
		Operate the service.  This never exits, so return is always via
		an exception.
	"""
		service = httpd.HttpService()
		service.listen = self.config['listen']
		service.certfile = self.config.get('certfile')
		if 'timeout' in self.config:
			service.timeout = float(config['timeout'])
		httpd_server = httpd.server(service, log=self.log)
		httpd_server.register_post(r'/rest', self.process)

		self.is_inet = (httpd_server.address_family in [socket.AF_INET, socket.AF_INET6])

		pset = poll.poll()
		pset.register(httpd_server, poll.POLLIN)

		while True:
			evlist = pset.poll()
			if evlist:
				for item, mask in evlist:
					if item == httpd_server:
						item.handle_request()
					else:
						raise Exception("Unknown poll item %s", repr(item))
			else:
				raise Exception("Timeout when none requested")
