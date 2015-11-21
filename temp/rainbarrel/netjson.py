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
 
import os, time, re, socket, json, logging

__doc__ = """
Transfers python data types as netstring-encapsulated JSON text.
Netstarings have the form:

	length:data,

where "length" is a base 10 size written in ascii digits, "data"
is any arbitrary byte-stream.  The colon separates the length from the
data.  The comma is present to lend limited format validation and
to make streams somewhat human-readable.

See https://en.wikipedia.org/wiki/Netstring.
"""

class Writer(object):
	"""
	This class provides a call to queue a data item and a separate call to provide
	a single socket send().  These methods return the number of unsent queue items
	which will be 0 when the queue is empty.

	When operating under the control of select.poll or similar, the caller should
	enable output events for the socket when a data item is queued, and disable
	them when the send method returns zero.

	Any errors (lost connections, etc) are raised as exceptions.  Recovery will
	typically require this object to be discarded and the socket closed.
"""

	class Buffer(object):
		def __init__(self, data, indent=None):
			j = json.dumps(data, indent=indent)
			self.data = str(len(j)) + ':' + j + ','
			self.remaining = len(self.data)

	def __init__(self, sock, **params):
		self.log = params.get('log')
		if not self.log:
			self.log = logging.getLogger(__name__)
			self.log.addHandler(logging.NullHandler())
		if self.log.isEnabledFor(logging.DEBUG):
			self.indent = 4
		else:
			self.indent = None
		self.sock = sock
		self.q = []

	def queue(self, data=None):
		"""
		Queues a data object to be transitted as a JSON-encoded netstring.
		Returns the length of the queue in data objects (not bytes).
		If "data" is None or not specified, the length of the queue
		is returned without adding to it.
	"""
		if data is not None:
			self.q.append(self.Buffer(data, indent=self.indent))

			if self.log.isEnabledFor(logging.DEBUG):
				pending = 0
				for item in self.q:
					pending += item.size - item.sofar
				self.log.debug("Queued %d bytes, pending items %d, pending bytes %d",
					self.q[-1].size, len(self.q), pending)
		return len(self.q)

	def send(self):
		while len(self.q) > 0 and self.q[0].remaining <= 0:
			self.log.error("Send queue held item with no remaining data")
			self.q.pop(0)
		if len(self.q) == 0:
			self.log.info("send() called with no data pending")
			return 0

		cnt = self.sock.send(self.q[0].data, socket.MSG_DONTWAIT)
		if cnt <= 0:
			raise Exception("Unexpected return %d from socket.send()", cnt)
		self.q[0].data = self.q[0].data[:cnt]
		self.q[0].remaining -= cnt

		if self.q[0].remaining <= 0:
			self.log.debug("send() emptied a queue bucket")
			self.q.pop(0)

		if self.log.isEnabledFor(logging.DEBUG):
			pending = 0
			for item in self.q:
				pending += item.size - item.sofar
			self.log.debug("Pending items %d, pending bytes %d", len(self.q), pending)
		return len(self.q)

class Reader(object):
	"""
	This class implements a generator that performs a recv() on the socket passed, and
	yields the next complete data type received.  An exception is raised if an error occurs
	including encapsulation and encoding errors.

	The "connection_lost" attribute will be True if the remote connection closed.
	An exception is raised if the connection is lost while data is still pending.

	The "maxsize" param is used to place an upper limit on the number of bytes that must be
	read before the ':' separator is encountered.  This allows early detection of faulty
	encapsulation.  The default allows sizes up to 1 TB.

	The behavior following an exception is undefined.  The caller should discard the object
	and close the remote connection.
"""

	def __init__(self, sock, **params):
		self.log = params.get('log')
		if not self.log:
			self.log = logging.getLogger(__name__)
			self.log.addHandler(logging.NullHandler())
		self.sock = sock
		self.iosize = params.get('iosize', 4096)
		self.connection_lost = False
		self.max_colon_offset = len(str(params.get('maxsize', 1024*1024*1024*1024))) + 1
		self.pending = ''
		self.need = None

		#  This supports a small extension to the netstring protocol by allowing a couple
		#  of white-space charcaters between the command and the size.  The upshot is you
		#  can do testing with telnet by sending the data with a CR.
		#
		self.regex_header = re.compile(r'^(\s{0,2}\d+):')

	def recv(self):
		data = self.sock.recv(self.iosize)
		if data == '':
			self.connection_lost = True
			if len(self.pending.lstrip()) > 0:
				raise Exception("Unexpected EOF with unprocessed data")
			return
		self.pending += data

		while True:
			self.log.debug("Pending: >>>%s<<<<", str(self.pending))
			if self.need is None:
				if self.pending.find(':') < 0:
					if len(self.pending) > self.max_colon_offset:
						raise Exception("No colon encountered within length limit of %d" %
												(self.max_colon_offset,))
					self.log.debug("Colon not found with %d packet bytes, continuing", len(self.pending))
					return
				m = self.regex_header.match(self.pending)
				if not m:
					raise Exception("Bad length encountered in packet header")
				size_str = m.group(1)
				self.log.debug("Size str %d bytes: >>>%s<<<<", len(size_str), size_str)

				#  We need to delay processing until all the data plus the trailing comma have been read.
				#
				self.need = int(size_str.strip()) + 1

				self.log.debug("Found length %d", self.need)

				#  Now remove the size header, leaving only some fragment of data and possible trailing comma.
				#
				self.pending = self.pending[len(size_str)+1:]

			if len(self.pending) >= self.need:
				data = self.pending[:self.need]
				self.pending = self.pending[self.need:]
				self.need = None

				if data[-1] != ',':
					raise Exception("Trailing comma missing from data")

				yield json.loads(data[:-1])
			else:
				self.log.debug("Returning with %d additional data needed", self.need - len(self.pending))
				return
