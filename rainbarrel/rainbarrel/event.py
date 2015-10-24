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
 
import os, time, errno, socket, json, logging
import event
import netjson
from taskforce import poll

#  How long a fresh connection will be retained waiting for a
#  registration message.
#
max_registration_wait = 5

#  How long a connection will be retained after it has been
#  shut down waiting for the far-end to close.
#
max_discard_wait = 5

class Event(object):
	"""
	Handles event communication.

	Event processing is established when a client connects to the event
	socket and sends a registration message indicating which events
	should be reported.  Processing is terminated when the client
	closes the connection.

	Events are reported using the registered event information and the
	complete rainbarrel state.
"""

	def __init__(self, event_set):
		self.event_set = event_set
		self.log = self.event_set.log

		self.sock, self.client = self.event_set._listen.accept()
		self.log.info("New connection from %s", repr(self.client))
		self.connection_time = time.time()
		self.registration_timeout = self.connection_time + max_registration_wait
		self.shutting_down = None
		self.discard = None
		self.filter_list = None
		self.event_set._pset.register(self, poll.POLLIN)
		self.reader = netjson.Reader(self.sock, log=self.log)
		self.writer = netjson.Writer(self.sock, log=self.log)

	def fileno(self):
		return self.sock.fileno()

	def idle(self):
		"""
		Performs any housekeeping needed for the Event instance.
	"""
		if self.discard and time.time() > self.discard:
			self.log.waring("Connection from %s exceeded shutdown wait, closing immediately", repr(self.client))
			self.close()
		if self.filter_list is not None:
			self.log.debug("Connection from %s idle", repr(self.client))
			return
		if self.registration_timeout and time.time() > self.registration_timeout:
			self.log.warning("Connection from %s expired before registration was received", repr(self.client))
			self.shutdown()
		elif self.registration_timeout:
			self.log.info("Connection from %s waiting %.1f secs on registration",
							repr(self.client), time.time() - self.connection_time)

	def shutdown(self):
		if self.shutting_down:
			self.log.warning("Close called %.1f secs after shutdown, immediate close triggered",
						time.time() - self.shutting_down)
			self.close()
		else:
			self.shutting_down = time.time()
			self.discard = self.shutting_down + max_discard_wait
			self.sock.shutdown(socket.SHUT_WR)

	def close(self):
		"""
		Completely remove event resources.  Normally this
		is called on EOF from the client.
	"""
		if self.sock:
			self.event_set._pset.unregister(self)
			try:
				self.sock.shutdown(socket.SHUT_RDWR)
				self.sock.close()
			except socket.error as e:
				if hasattr(errno, 'ENOTCONN') and e.errno == errno.ENOTCONN:
					self.log.debug("Client %s already closed connection -- %s", repr(self.client), str(e))
				else:
					self.log.warning("Close for client %s failed on event connection -- %s",
												repr(self.client), str(e))
			except Exception as e:
				self.log.warning("Close failed on event connection -- %s", str(e))
			self.sock = None
		self.event_set._events.discard(self)

	def handle(self, mask):
		"""
		Handle poll() event.  This will read available data and/or
		write pending data.  When all pending data has been written,
		the POLLOUT is removed from the pset.
	"""
		self.log.info("Handle called")
		if mask & poll.POLLIN:
			items = 0
			for item in self.reader.recv():
				items += 1
				self.log.info("Received %s", repr(item))
				self.filter_list = item
			self.log.info("Received %d item%s in this batch", items, '' if items == 1 else 's')
			if self.reader.connection_lost:
				self.log.info("EOF on %s connection", repr(self.client))
				self.close()
			if items > 0:
				self.writer.queue(item)
				self.event_set._pset.modify(self, poll.POLLIN|poll.POLLOUT)

		if mask & poll.POLLOUT:
			self.writer.send()
			if self.writer.queue() == 0:
				self.event_set._pset.modify(self, poll.POLLIN)

	def filter(self, state):
		"""
		Filters the current state to determine if the event
		has fired.  If so, the state is queued to be send
		to the client.

		This method is called whenever the state is changed
		by the Rainforest device.
	"""
		self.log.info("Filter called")

class EventSet(object):
	def __init__(self, pset, listen, **params):
		self.log = params.get('log')
		if not self.log:
			self.log = logging.getLogger(__name__)
			self.log.addHandler(logging.NullHandler())

		self._pset = pset
		self._listen = listen
		self._events = set()

	def __enter__(self):
		return self

	def __exit__(self, type, value, traceback):
		self.reset()

	def add(self):
		"""
		Add an event instance to handle the connection
		waiting on the listen socket.
	"""
		self.log.info("New connection detected")
		self._events.add(Event(self))

	def reset(self):
		"""
		Clear out all existing connections.  This should be
		called after any disruption in processing, eg when the
		parent restarts itself due following an unexpected
		exception.  It is safe to call at any time but note
		that existing connections are closed immediately without
		following the shutdown protocol.
	"""
		self.log.info("Reset all events")
		for ev in list(self._events):
			try: ev.close()
			except: pass
		self._events = set()

	def idle(self):
		"""
		Calls the idle() method on all recorded events.
		This should be called by the
	"""
		for ev in list(self._events):
			ev.idle()

	def filter(self, state):
		"""
		Calls the filter() method on all recorded events.
	"""
		for ev in self._events:
			ev.filter(state)
