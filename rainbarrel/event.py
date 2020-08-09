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

import time
import errno
import socket
import re
import logging
import netjson
from pollinator import poll

#  How long a fresh connection will be retained waiting for a
#  registration message.
#
max_registration_wait = 5

#  How long a connection will be retained after it has been
#  shut down waiting for the far-end to close.
#
max_discard_wait = 5

#  The operators supported for use in filters
#
filter_operators = set(['>', '<', '==', '>=', '<=', '!='])

#  Matches variable names
#
re_varname = re.compile(r'^\w+$')


class Event(object):
    """
    Handles event communication.

    Event processing is established when a client connects to the event
    socket and sends a subscription message indicating which events
    should be reported.  Processing is terminated when the client
    closes the connection.

    Events are reported using the complete rainbarrel state and
    the subscription filter blocks that matched the state.  The
    resulting publication is a map with the form:

    {
      "status": true|false,
      "error": "error message if status is false",
      "state": {current rainbarrel state},
      "blocks": [list of filter blocks that matched the state or triggered the error]
    }

    A subscription consists of a list of filter blocks.  A filter block is
    a map which may individually match the state and cause the state to be
    published.  A filter block has the form:

    {
      "name": "user provided filter block name",
      "type": "edge"|"level" (default is "level"),
      "state": true|false,
      "filters": [filter_tuple [, filter_tuple, ...]]
    }

    "state" is the initial state for edge events.  The default will cause
    an edge event to always trigger during the initial scan.  The state is
    updated whenever an edge event is triggered.

    All filter tuples in the list must all match the state for the filter
    block to trigger a publication.  "OR" cases are handled with multiple
    filter blocks.

    A filter_tuple has the form:

      ( "state.path", comparator, value )

    where:
      state.path    is a dotted path to a value entry in the state.
            An example is "InstantaneousDemand._readings.Demand".
      comparator    is a string representation of a comparison operator.
            Possible values are: "==", ">=", "<=", "!=", ">", "<".
      value     is any valid JSON value (string, number, Boolean) that
            can be reasonably compared to the path value.

    Note that although the individual filters can be writtent as Python tuples,
    they are serialized by JSON as lists, so they when returned in the publication,
    the "filters" element will arrive as a list of lists.

    Example of a simple filter:

    [
      { "name": "demand_exceeded", "filters": [["InstantaneousDemand._readings.Demand", ">", 1.5]] }
    ]

    This references the rainbarrel-derived Demand value in the InstantaneousDemand "_readings" element.
    Derived values tend to be a little easier to use because they are in consistent units (kW in this
    case) instead of the integer encoded decimal values that come from the device.
"""

    def __init__(self, event_set):
        self.event_set = event_set
        self.log = self.event_set.log

        self.sock, self.client = self.event_set._listen.accept()
        self.log.info("New connection from %r", self.client)
        self.connection_time = time.time()
        self.registration_timeout = self.connection_time + max_registration_wait
        self.shutting_down = None
        self.discard = None
        self.subscription_list = None
        self.expression_list = None
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
            self.log.waring("Connection from %r exceeded shutdown wait, closing immediately", self.client)
            self.close()
        if self.subscription_list is not None:
            self.log.debug("Connection from %r idle", self.client)
            return
        if self.registration_timeout and time.time() > self.registration_timeout:
            self.log.warning("Connection from %r expired before registration was received", self.client)
            self.shutdown()
        elif self.registration_timeout:
            self.log.info("Connection from %r waiting %.1f secs on registration",
                            self.client, time.time() - self.connection_time)

    def shutdown(self):
        if self.shutting_down:
            self.log.info("Shutdown called again")
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
                    self.log.debug("Client %r already closed connection -- %s", self.client, e)
                else:
                    self.log.warning("Close for client %r failed on event connection -- %s", self.client, e)
            except Exception as e:
                self.log.warning("Close failed on event connection -- %s", e)
            self.sock = None
        self.event_set._events.discard(self)

    def error(self, block, state, msg=None, exc=None):
        if isinstance(block, dict):
            if 'name' in block:
                name = str(block['name'])
            else:
                name = 'unknown'
        else:
            name = 'invalid'
        if exc:
            self.log.error("Block %r error -- %s", name, exc, exc_info=self.log.isEnabledFor(logging.DEBUG))
        else:
            self.log.error("Block %r error -- %s", name, msg)
        self.writer.queue({
            "status": False,
            "state": state,
            "blocks": [block]
        })
        self.event_set._pset.modify(self, poll.POLLIN | poll.POLLOUT)
        self.shutdown()

    def compile(self):
        """
        Generate and compile the actual expressions needed
        to execute the filter blocks.  This is done by
        building a parallel tree of filters for the
        subscription.  Any error encountered during
        compilation causes the entire subscription to be
        rejected.

        Once built, the expressions should be executed via:

            res = eval(expr, {}, {'state': state})
            if res is not True and res is not False:
                self.error(block, state, msg='filter expression did not return true/false')

        This restricts the expression's access to just the
        content of the state tree.
    """
        if self.subscription_list is None or len(self.subscription_list) == 0:
            self.log.warning("Filter called with no filter list recorded")
            return
        self.expression_list = []
        for block in self.subscription_list:
            if 'filters' not in block:
                self.error(block, None, msg="Filter block provided has no 'filters' element")
                return
            try:
                name = str(block.get('name'))
                if 'type' not in block:
                    block['type'] = 'level'
                if block['type'] not in ['edge', 'level']:
                    raise Exception("Unknown filter block type '%s'" % block['type'])
                if 'state' not in block:
                    block['state'] = None
                if 'filters' not in block:
                    block['filters'] = []
                if not isinstance(block['filters'], list):
                    block['filters'] = [block['filters']]
                expression_block = []
                for filter in block['filters']:
                    if len(filter) == 1:
                        #  A single string must be the name of a state command, eg 'InstantaneousDemand'
                        #
                        cmd = filter[0].strip()

                        #  Might like to validate these against the known list, eg in
                        #  barrel().properties.keys()
                        #
                        if re_varname.match(cmd) is None:
                            self.error(block, None, msg="Invalid state command name '%s'" % (cmd,))
                            return
                        expr = """state['_last_element'] == '%s'""" % (cmd,)
                    elif len(filter) == 3:
                        #  A three-tuple compares a state variable with a constant
                        #
                        path = filter[0].strip()
                        op = filter[1].strip()
                        val = filter[2]

                        f = path.split('.')
                        if len(f) == 0:
                            self.error(block, None, msg='Empty state path')
                            return
                        sv = 'state'
                        cmd = None
                        while len(f) > 0:
                            tag = str(f.pop(0))
                            if cmd is None:
                                cmd = tag
                            if re_varname.match(tag) is None:
                                self.error(block, None, msg='Invalid path element "%s"' % (tag,))
                                return
                            sv += "['" + tag + "']"
                        if op not in filter_operators:
                            self.error(block, None, msg="Invalid filter operator '%s'" % (op,))
                            return
                        expr = """state['_last_element'] == '%s' and %s %s %r""" % (cmd, sv, op, val)
                    else:
                        self.error(block, None, msg='Invalid filter length')
                        return
                    self.log.debug("Compiling: %s", expr)
                    expression_block.append(compile(expr, name, 'eval'))
            except Exception as e:
                self.error(block, None, exc=e)
                return

        self.expression_list.append(expression_block)

    def filter(self, state):
        """
        Filters the current state to determine if the event
        has fired.  If so, the state is queued to be send
        to the client.

        This method is called whenever the state is changed
        by the Rainforest device.
    """
        if self.subscription_list is None or len(self.subscription_list) == 0:
            self.log.warning("Filter called with no filter list recorded")
            return
        if self.expression_list is None:
            raise Exception("Attempt to filter before subscription has been complied")

        self.log.debug("Filter called")
        matched_blocks = []
        for subs_pos in range(len(self.subscription_list)):
            subs_block = self.subscription_list[subs_pos]
            expr_block = self.expression_list[subs_pos]
            try:
                self.log.debug("Checking %s filter block %r", subs_block['type'], subs_block.get('name'))
                matched = True
                for expr in expr_block:
                    res = eval(expr, {}, {'state': state})
                    if res is not True and res is not False:
                        raise Exception('filter expression did not return true/false')
                    if res:
                        self.log.debug("Block %r matched", subs_block.get('name'))
                    else:
                        self.log.debug("Block %r NO match, last element changed was %r",
                                        subs_block.get('name'), state['_last_element'])
                        matched = False
                        break
                if subs_block['type'] == 'edge':
                    if matched != subs_block['state']:
                        subs_block['state'] = matched
                        matched_blocks.append(subs_block)
                elif matched:
                    subs_block['state'] = matched
                    matched_blocks.append(subs_block)
            except Exception as e:
                self.error(subs_block, state, exc=e)
                return
        if len(matched_blocks) > 0:
            self.writer.queue({
                "status": True,
                "state": state,
                "blocks": matched_blocks
            })
            self.event_set._pset.modify(self, poll.POLLIN | poll.POLLOUT)

    def handle(self, mask, state):
        """
        Handle poll() event.  This will read available data and/or
        write pending data.  When all pending data has been written,
        the POLLOUT is removed from the pset.
    """
        self.log.debug("Handle called")
        if mask & poll.POLLIN:
            items = 0
            for item in self.reader.recv():
                items += 1
                self.log.debug("Received %r", item)
                if items > 1:
                    self.log.warning("Multiple netstrings received, only last takes effect")
                self.subscription_list = item
            self.log.debug("Received %d item%s in this batch", items, '' if items == 1 else 's')
            if self.reader.connection_lost:
                self.log.info("EOF on %r connection", self.client)
                self.close()
            if items > 0:
                self.compile()
                block_cnt = len(self.expression_list)
                self.log.info("Successful subscription on fd %d with %d filter block%s",
                                self.fileno(), block_cnt, '' if block_cnt == 1 else 's')
                self.filter(state)

        if mask & poll.POLLOUT:
            self.writer.send()
            if self.writer.queue() == 0:
                self.event_set._pset.modify(self, poll.POLLIN)


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
            parent restarts itself following an unexpected exception.
            It is safe to call at any time but note that existing
            connections are closed immediately without following the
            shutdown protocol.
        """
        self.log.info("Reset all events")
        for ev in list(self._events):
            try:
                ev.close()
            except:
                pass
        self._events = set()

    def idle(self):
        """
            Calls the idle() method on all recorded events.
            This should be called periodically when other
            operations don't require processing.
        """
        for ev in list(self._events):
            ev.idle()

    def filter(self, state):
        """
        Calls the filter() method on all recorded events.
    """
        for ev in self._events:
            ev.filter(state)
