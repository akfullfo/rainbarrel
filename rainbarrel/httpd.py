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

import os, sys, stat, errno, re, logging, ssl
from cgi import parse_header, parse_multipart
try:
	import socketserver
	import http.server as http_server
	from urllib.parse import parse_qs, urlparse
except:
	import SocketServer as socketserver
	import BaseHTTPServer as http_server
	from urlparse import parse_qs, urlparse

from .__init__ import __version__ as rainbarrel_version

#  Default when no address is provided
def_address = '/var/run/s.rainbarrel'

#  Used when a IP address is provided with no port
def_port = 8080

class HttpService(object):
	"""
	Defines all configuration needed to start an HTTP service.  In future, the
	attributes may be extended to include such things as authenication method,
	ciphers, SSL version list, etc.

	A freshly initialized instance describes a service that would listen on
	def_address, disallow control operations, and use 'http' protocol rather
	than 'https'.

	The caller should instantiate this class, change configuration attributes
	as needed, and then create the service using the httpd.server() method.

	'listen' may be specified as "[host][:port]" for TCP, or as "path" to select
	a Udom service (path must contain at least one "/" character).

	If 'allow_control' is True, control commands to change legion or task state
	will be allowed.

	If 'certfile' is specified, the connection will be wrapped as SSL using the
	specified path to a PEM certificate file.

	'timeout' specifies the time in float seconds that I/O operations may take
	before the request is aborted.

	Using a class for configuration allows extensions to be made in the
	impelementation without callers needing to change until they want to make use
	of the new feature.
"""
	def __init__(self):
		self.listen = ''
		self.allow_control = False
		self.certfile = None
		self.timeout = 3.0

	def __str__(self):
		return "[%s]%s%s" % (
				self.listen if hasattr(self, 'listen') and self.listen else ':default:',
				' controlling' if hasattr(self, 'allow_control') and self.allow_control else '',
				' cert='+self.certfile if hasattr(self, 'certfile') and self.certfile else ''
		)

	def cmp(self, other_service):
		"""
		Compare with an instance of this object.  Returns None if the object
		is not comparable, False is relevant attributes don't match and True
		if they do.
	"""
		if not isinstance(other_service, HttpService):
			return None
		for att in dir(self):
			if att == 'cmp' or att.startswith('_'):
				continue
			if not hasattr(other_service, att):
				return None
			if getattr(self, att) != getattr(other_service, att):
				return False
		return True

class HTTP_handler(http_server.BaseHTTPRequestHandler):
	server_version = 'rainbarrel/' + rainbarrel_version

	#  Uncomment if we want to keep the python version a secret
	#sys_version = ''

	def fault(self, code, message):
		self.send_response(code)
		if code < 500:
			self.server.log.warning("HTTP %d on '%s' -- %s", code, self.path, message)
			message = message.encode('utf-8')
			self.send_header("Content-Type", "text/plain")
			self.send_header("Content-Length", len(message))
			self.end_headers()
			self.wfile.write(message)
		else:
			self.server.log.error("HTTP %d on '%s' -- %s", code, self.path, message)
			self.end_headers()

	def do_GET(self):
		params = {'handler': self}
		try:
			resp = self.server.serve_get(self.path, **params)
			if not resp:
				self.fault(404, self.path + ' not found')
				return
			if type(resp) != tuple or len(resp) != 3:
				self.fault(500, 'Bad callback response for ' + self.path)
			code, content, content_type = resp
		except Exception as e:
			self.server.log.warning("Traceback -- %s", str(e), exc_info=True)
			self.fault(500, "Callback error -- " + str(e))
			return
		content = content.encode('utf-8')
		self.send_response(code)
		self.send_header("Content-Type", content_type)
		self.send_header("Content-Length", len(content))
		self.end_headers()
		self.wfile.write(content)

	def do_POST(self):
		content_type = self.headers.get('content-type')
		postmap = {}
		params = {'handler': self}
		if content_type:
			try:
				ctype, pdict = parse_header(content_type)
				if ctype == 'multipart/form-data':
					postmap = parse_multipart(self.rfile, pdict)
				elif ctype == 'application/x-www-form-urlencoded':
					length = int(self.headers['content-length'])
					postmap = parse_qs(self.rfile.read(length), keep_blank_values=1)
				else:
					length = int(self.headers['content-length'])
					params['type'] = ctype
					params['data'] = self.rfile.read(length)
			except Exception as e:
				self.fault(400, "Parse error -- " + str(e))
				return
		try:
			resp = self.server.serve_post(self.path, postmap, **params)
			if not resp:
				self.fault(404, self.path + ' not found')
				return
			if type(resp) != tuple or len(resp) != 3:
				self.fault(500, 'Bad callback response for ' + self.path)
			code, content, content_type = resp
		except Exception as e:
			self.server.log.warning("Traceback -- %s", str(e), exc_info=True)
			self.fault(500, "Callback error -- " + str(e))
			return

		content = content.encode('utf-8')
		self.send_response(code)
		self.send_header("Content-Type", content_type)
		self.send_header("Content-Length", len(content))
		self.end_headers()
		self.wfile.write(content)

	def format_addr(self, addr, showport=False):
		if type(addr) is tuple and len(addr) == 2:
			if showport:
				return "%s:%d" % (addr[0], addr[1])
			else:
				return str(addr[0])
		elif type(addr) is str:
			return addr
		else:
			return str(addr)

	def log_message(self, fmt, *fargs):
		try:
			msg = fmt.strip() % fargs
		except Exception as e:
			msg = "Error formatting '%s' -- %s" % (str(fmt), str(e))
		try:
			saddr = self.format_addr(self.server.server_address, showport=True)
		except:
			saddr = 'unknown'
		try:
			raddr = self.format_addr(self.client_address)
		except:
			raddr = 'unknown'
		self.server.log.info("%s>%s %s", raddr, saddr, msg)

class BaseServer(object):

	def register_get(self, regex, callback):
		"""
		Register a regex for processing HTTP GET
		requests.  If the callback is None, any
		existing registration is removed.
	"""
		if callback is None:
			if regex in self.get_registrations:
				del self.get_registrations[regex]
		else:
			self.get_registrations[regex] = (re.compile(regex), callback)

	def register_post(self, regex, callback):
		"""
		Register a regex for processing HTTP POST
		requests.  If the callback is None, any
		existing registration is removed.

		The callback will be called as:
	
			callback(path, postmap)
	"""
		if callback is None:
			if regex in self.post_registrations:
				del self.post_registrations[regex]
		else:
			self.post_registrations[regex] = (re.compile(regex), callback)

	def _match_path(self, path, registrations):
		match_len = 0
		matched = None
		for ex, callback in registrations.values():
			m = ex.match(path)
			if m:
				l = len(m.group(0))
				if l > match_len:
					match_len = l
					matched = callback
		return matched

	def serve_get(self, path, **params):
		"""
		Find a GET callback for the given HTTP path, call it and return the
		results.  The callback is called with two arguments, the path used to
		match it, and params which include the BaseHTTPRequestHandler instance.

		The callback must return a tuple:

			(code, content, content_type)

		If multiple registrations match the path, the one with the longest
		matching text will be used.  Matches are always anchored at the start
		of the path.

		None is returned if no registered callback is willing to handle a path.
	"""
		if path is None: return None

		matched = self._match_path(path, self.get_registrations)
		if matched is None:
			return None
		else:
			return matched(path, **params)

	def serve_post(self, path, postmap, **params):
		"""
		Find a POST callback for the given HTTP path, call it and return
		the results.  The callback is called with the path used to match
		it, a dict of vars from the POST body and params which include the
		BaseHTTPRequestHandler instance.

		The callback must return a tuple:

			(code, content, content_type)

		If multiple registrations match the path, the one with the longest
		matching text will be used.  Matches are always anchored at the start
		of the path.

		None is returned if no registered callback is willing to handle a path.
	"""
		matched = self._match_path(path, self.post_registrations)
		if matched is None:
			return None
		else:
			return matched(path, postmap, **params)

class TCPServer(socketserver.TCPServer, BaseServer):
	daemon_threads = True
	allow_reuse_address = True

	def __init__(self, host, port, timeout, log):
		self.host = host
		self.port = port
		self.timeout = timeout
		self.log = log
		self.get_registrations = {}
		self.post_registrations = {}
		self.allow_control = False
		super(TCPServer, self).__init__((host, port), HTTP_handler)

	def close(self):
		pass

	def get_request(self):
		info = super(TCPServer, self).get_request()
		if self.timeout:
			info[0].settimeout(self.timeout)
		return info

def server(service, log=None):
	"""
	Creates a blocking http service based on the passed HttpService instance.

	Parameters:

	  service	- Service configuration.  See the HttpService class above.
	  log		- A 'logging' object to log errors and activity.
"""
	if log:
		log = log
	else:
		log = logging.getLogger(__name__)
		log.addHandler(logging.NullHandler())
	if not service.timeout:
		service.timeout = None

	if not service.listen:
		service.listen = def_address

	port = None
	m = re.match(r'^(.*):(.*)$', service.listen)
	if m:
		log.debug("Matched host '%s', port '%s'", m.group(1), m.group(2))
		host = m.group(1)
		try:
			port = int(m.group(2))
		except:
			raise Exception("TCP listen port must be an integer")
	else:
		host = service.listen
		log.debug("No match, proceding with host '%s'", host)
	if not port:
		port = def_sslport if service.certfile else def_port
	httpd = TCPServer(host, port, service.timeout, log)
	log.info("HTTP service %s", str(service))
	return httpd
